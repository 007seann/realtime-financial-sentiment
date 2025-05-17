#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15

@author: Sean Sanggyu Choi
"""
import pandas as pd
import numpy as np
from scipy.optimize import minimize
from pykalman import KalmanFilter
from plugins.common.time_log_decorator import time_log
from pyspark.sql.functions import col, udf, array, monotonically_increasing_id, struct, collect_list, explode 
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, ArrayType, StructType, StructField, LongType



############################ Distributed Functions(Spark) ############################
@time_log
def train_model_spark(DD, dep, kappa, alpha_high, alpha_low = None, pprint = False, vol_q=None):
    """
    Trains the model based on a set of term counts from reports labeled with time series data.

    Parameters
    ----------
    DD : NxM+1 Dataframe
        Count of M terms for N reports and corresponding time series label.
    kappa : Float in [0,1]
        Quantile of the terms count distribution to be included.
    alpha_high : int
        Number of terms in positive and negative sentiment set.
    pprint : Boolean, optional
        Specifies whether to print progress (for analysis purposes). The default is False.

    Returns
    -------
    sent_words : list of length alpha_high + alpha_low
        Sentiment charged words.
    O_hat : alpha_high+alpha_lowx2 array
        Positive and negative distributions over sentiment charged words.

    """
    assert dep in ['_ret','_vol','_ret+1','_vol+1'], 'dep must be either _ret or _vol (+1)'
    if vol_q:
        assert vol_q < 1 and vol_q > 0, "Wrong quantile specified"
    
    if alpha_low == None:
        alpha_low = alpha_high
    
    
    # Add an index column to preserve row order
    DD = DD.withColumn("row_id", F.monotonically_increasing_id())

    # Define metadata columns and get vocabulary (i.e. term-count columns)
    meta_cols = ['_cik', '_ret', '_vol', '_ret+1', '_vol+1']
    vocab = [col for col in DD.columns if col not in meta_cols + ["row_id"]]

    # Create document-term DataFrame
    dd_doc = DD.select("row_id", *vocab)
    
    # --- Compute term occurrence counts and filter vocabulary ---
    # Count, for each term, in how many rows it is > 0
    agg_exprs = [F.sum(F.when(F.col(c) > 0, 1).otherwise(0)).alias(c) for c in vocab]
    term_counts_row = dd_doc.agg(*agg_exprs).first().asDict()
    
    # Compute quantile threshold (min_occur)
    counts = list(term_counts_row.values())
    min_occur = np.quantile(counts, kappa)
    if pprint:
        print("Term occurrence counts:")
        print(term_counts_row)
        print(f"min_occur (quantile {kappa}): {min_occur}")

    # Keep only terms with count >= min_occur
    keep_terms = [term for term, count in term_counts_row.items() if count >= min_occur]
    dd_doc = dd_doc.select("row_id", *keep_terms)
    if pprint:
        print(f"{len(keep_terms)} terms remaining for screening out of {len(vocab)} total.")
    
    # SCREENING FOR SENTIMENT WORDS
    # --- Define the dependent variable and high volatility indicator ---
    # Compute the time series column (ts) from DD
    # For _vol, compute the quantile or median as threshold
    if dep == '_vol':
        if vol_q is not None:
            mean_vol = DD.approxQuantile(dep, [vol_q], 0.01)[0]
        else:
            mean_vol = DD.approxQuantile(dep, [0.5], 0.01)[0]
    else:
        mean_vol = 0


    # Add a boolean column "high_vol": whether ts > mean_vol
    dd = DD.withColumn("high_vol", F.col(dep) > F.lit(mean_vol))
    
    # --- Compute co-occurrence frequencies ---
    # For each term in keep_terms, compute the number of rows with term>0 and with high_vol True
    agg_exprs_high = [F.sum(F.when((F.col(c) > 0) & (F.col("high_vol") == True), 1).otherwise(0)).alias(c)
                    for c in keep_terms]
    counts_high = dd.select(*agg_exprs_high).first().asDict()
    
    # Compute co-occurrence: for each term, fraction = (count with high_vol) / (overall count)
    cooc_dict = {term: (counts_high[term] / term_counts_row[term]) if term_counts_row[term] > 0 else 0 
                for term in keep_terms}
    
    
    # Select words: highest co-occurrence for high sentiment and lowest for low sentiment
    sorted_terms_desc = sorted(cooc_dict.items(), key=lambda x: -x[1])
    sorted_terms_asc  = sorted(cooc_dict.items(), key=lambda x: x[1])
    words_high = [term for term, val in sorted_terms_desc[:alpha_high]]
    words_low  = [term for term, val in sorted_terms_asc[:alpha_low]]
    sent_words = words_high + words_low
    if pprint:
        print('-----------------')
        print(f'Words associated with HIGH {dep}:')
        print(words_high)
        print('-----------------')
        print(f'Words associated with LOW {dep}:')
        print(words_low)
        print('-----------------')
    
    # LEARNING SENTIMENT TOPICS    
    # --- Construct document matrix for sentiment words and compute normalized frequencies ---
    # Select only the sentiment words columns
    D_title = dd_doc.select("row_id", *sent_words)
    
    # Compute row-wise sum over sentiment words (s_hat)
    s_hat_expr = F.lit(0)
    for word in sent_words:
        s_hat_expr = s_hat_expr + F.col(word)
    D_title = D_title.withColumn("s_hat", s_hat_expr)
    
    # (Optional) print fraction of rows with at least one sentiment word
    total_rows = D_title.count()
    nonzero_rows = D_title.filter(F.col("s_hat") > 0).count()
    if pprint:
        print(f"{round(nonzero_rows/total_rows*100,1)}% of reports have at least one sentiment-charged word")
    
    # For each sentiment word, compute normalized frequency per document:
    # norm_{word} = word_count / (s_hat if s_hat>0 else 1)
    for word in sent_words:
        D_title = D_title.withColumn("norm_" + word, 
                                    F.col(word) / F.when(F.col("s_hat") > 0, F.col("s_hat"))
                                    .otherwise(F.lit(1)))

    # --- Compute sentiment score (p_hat) ---
    # We need the p_hat for each row based on the dependent variable.
    # For alignment later, extract from dd using the same row_id.
    dd_p = dd.select("row_id", dep)
    if vol_q is not None:
        # Use min-max scaling
        min_ts = dd.agg(F.min(dep).alias("min")).first()["min"]
        max_ts = dd.agg(F.max(dep).alias("max")).first()["max"]
        dd_p = dd_p.withColumn("p_hat", (F.col(dep) - F.lit(min_ts)) / (F.lit(max_ts) - F.lit(min_ts)))
    else:
        # Use percent_rank (note: percent_rank() returns values in [0,1])
        w = Window.orderBy(F.col(dep))
        dd_p = dd_p.withColumn("p_hat", F.percent_rank().over(w))
    
    # --- Join normalized sentiment data with p_hat ---
    # Join on row_id and order by row_id to keep consistent ordering.
    joined_df = D_title.join(dd_p.select("row_id", "p_hat"), on="row_id")
    joined_df = joined_df.orderBy("row_id")
    
    # Select only the columns needed for regression: p_hat and the normalized sentiment frequencies.
    norm_cols = ["norm_" + word for word in sent_words]
    final_df = joined_df.select("p_hat", *norm_cols)
    
    # Collect to driver (assumes the number of rows is manageable)
    pdf = final_df.toPandas()
    p = pdf["p_hat"].values  # shape (N,)
    # Create a matrix for D_hat where each column corresponds to a sentiment word
    D_hat = pdf[norm_cols].values  # shape (N, num_sent_words)
    
    # --- Solve for sentiment topics via least squares ---
    # Create design matrix W: first column p_hat, second column (1-p_hat)
    W = np.vstack((p, 1-p)).T  # shape (N,2)
    # Solve W * beta = D_hat for beta for each sentiment word
    # beta will be a (2, num_sent_words) array; we then take its transpose.
    beta, residuals, rank, s = np.linalg.lstsq(W, D_hat, rcond=None)
    beta = beta.T  # shape (num_sent_words, 2)
    
    # Remove negative values and normalize each column so that they sum to 1
    beta = np.maximum(beta, 0)
    if beta[:,0].sum() > 0:
        beta[:,0] = beta[:,0] / beta[:,0].sum()
    if beta[:,1].sum() > 0:
        beta[:,1] = beta[:,1] / beta[:,1].sum()
    
    O_hat = beta  # final (num_sent_words, 2) array
    

    return sent_words, O_hat

def predict_sent_spark(model, arts, llambda):
    """
    Predicts a sentiment score for each 10-K filing (row) in a Spark DataFrame given a trained model.

    Parameters
    ----------
    model : list
        A two-element list containing:
        - S_hat: list of sentiment words (column names)
        - O_hat: numpy array of shape (num_sent_words, 2)
    arts : Spark DataFrame
        DataFrame with one row per article and columns representing term counts.
    llambda : positive float
        Penalty term that shrinks the estimated score towards 1/2 (neutral).

    Returns
    -------
    result_df : Spark DataFrame
        A DataFrame with a new column "p_hat" containing the estimated sentiment scores in [0,1].
    """
    S_hat = model[0]  # sentiment-charged words (list of column names)
    O_hat = model[1]  # numpy array of shape (len(S_hat), 2)

    # Define the UDF that computes the sentiment score for a single row.
    def compute_p(DD_i):
        """
        Given the counts for sentiment words in one document (as a list),
        compute the sentiment score.
        """
        DD_i = np.array(DD_i, dtype=float)
        s = DD_i.sum()
        if s == 0:
            return 0.5
        # Define the objective function to minimize.
        def p_fun(p_arr):
            p = p_arr[0]
            # Compute the sentiment component:
            # (weighted average of the log of positive/negative distributions)
            sen = np.dot(DD_i, np.log(p * O_hat[:,0] + (1-p) * O_hat[:,1])) / s
            # Penalty to shrink towards neutral (0.5)
            pen = llambda * np.log(p * (1-p))
            return -(sen + pen)
        res = minimize(p_fun, x0=[0.5], method='SLSQP', bounds=[(1e-5, 1-1e-5)])
        if res.success:
            return float(res.x[0])
        else:
            # If the optimization fails, return neutral sentiment.
            return 0.5

    # Register the UDF.
    compute_p_udf = udf(compute_p, DoubleType())

    # Ensure that arts has the sentiment word columns.
    # Create a new column "counts" which is an array of the counts from S_hat columns.
    arts_with_counts = arts.withColumn("counts", array(*[col(word) for word in S_hat]))

    # Apply the UDF to compute p_hat for each row.
    result_df = arts_with_counts.withColumn("p_hat", compute_p_udf(col("counts")))
    
    # Optionally, select only the p_hat column along with an identifier if needed.
    return result_df.select("Date","p_hat")

@time_log
def kalman_spark(mod_sent_df):
    """
    Apply the Kalman filter to the Spark DataFrame predictions for '_ret' (or '_vol').
    This function assumes that the entire DataFrame represents one time series,
    ordered by the 'Date' column.
    
    Parameters
    ----------
    mod_sent_df : Spark DataFrame
        DataFrame containing predictions with a column 'p_hat' and a 'Date' column.
        E.g.) 
        +----------+-------------------+
        |      Date|              p_hat|
        +----------+-------------------+
        |2016-03-17| 0.5532992437677514|
        |2009-11-19| 0.6873536008942858|
        |2015-05-20|0.26757786781953724|
        |2010-05-21| 0.7697687091784392|
        |2022-05-27| 0.6561448356912915|
        +----------+-------------------+
    
    Returns
    -------
    mod_sent_df_kalman : Spark DataFrame
        DataFrame with an additional column '_ret' containing Kalman-filtered values.
    """
    # Add a row index column to preserve ordering.
    # Note: monotonically_increasing_id() does not guarantee strict ordering,
    # so we order by Date afterward.
    mod_sent_df = mod_sent_df.withColumn("row_idx", monotonically_increasing_id())
    mod_sent_df = mod_sent_df.orderBy("Date")
    
    # Aggregate all rows into a single list while preserving the row index.
    agg_df = mod_sent_df.agg(
        collect_list(struct(col("row_idx"), col("p_hat"))).alias("p_data")
    )
    
    # Define a UDF that applies the Kalman filter to the full time series.
    def kalman_filter(data):
        # data is a list of dicts with keys 'row_idx' and 'p_hat'
        # Sort by row_idx to ensure correct ordering.
        data_sorted = sorted(data, key=lambda x: x["row_idx"])
        p_vals = [x["p_hat"] for x in data_sorted]
        if len(p_vals) == 0:
            return []
        arr = np.array(p_vals)
        # Initialize and fit the Kalman filter.
        kf = KalmanFilter(initial_state_mean=0, n_dim_obs=1)
        kf = kf.em(arr, n_iter=10)
        smoothed, _ = kf.filter(arr)
        # Build a list of dictionaries mapping row_idx to the smoothed value.
        return [{"row_idx": int(d["row_idx"]), "smoothed": float(smoothed[i, 0])}
                for i, d in enumerate(data_sorted)]
    
    # Define the output schema for the UDF.
    kalman_schema = ArrayType(
        StructType([
            StructField("row_idx", LongType(), False),
            StructField("smoothed", DoubleType(), False)
        ])
    )
    
    kalman_udf = F.udf(kalman_filter, kalman_schema)
    
    # Apply the UDF to the aggregated data.
    agg_df = agg_df.withColumn("kalman_result", kalman_udf(col("p_data")))
    
    # Explode the kalman_result so each row corresponds to one entry.
    exploded = agg_df.select(explode(col("kalman_result")).alias("k"))
    exploded = exploded.select(col("k.row_idx").alias("row_idx"),
                            col("k.smoothed").alias("_ret"))
    
    # Join the smoothed values back to the original DataFrame using row_idx.
    mod_sent_df_kalman = mod_sent_df.join(exploded, on="row_idx", how="left") \
                                    .orderBy("Date") \
                                    .drop("row_idx")
    
    return mod_sent_df_kalman


############################ CPU Intensive Functions(Pandas) ############################
@time_log
def train_model(DD, dep, kappa, alpha_high, alpha_low = None, pprint = False, vol_q=None):
    """
    Trains the model based on a set of term counts from reports labeled with time series data.

    Parameters
    ----------
    DD : NxM+1 Dataframe
        Count of M terms for N reports and corresponding time series label.
    kappa : Float in [0,1]
        Quantile of the terms count distribution to be included.
    alpha_high : int
        Number of terms in positive and negative sentiment set.
    pprint : Boolean, optional
        Specifies whether to print progress (for analysis purposes). The default is False.

    Returns
    -------
    sent_words : list of length alpha_high + alpha_low
        Sentiment charged words.
    O_hat : alpha_high+alpha_lowx2 array
        Positive and negative distributions over sentiment charged words.

    """
    assert dep in ['_ret','_vol','_ret+1','_vol+1'], 'dep must be either _ret or _vol (+1)'
    if vol_q:
        assert vol_q < 1 and vol_q > 0, "Wrong quantile specified"
    
    if alpha_low == None:
        alpha_low = alpha_high
    
    ts = DD[dep]
    N = DD.shape[0] 
    
    DD_doc = DD.drop(columns=['_cik', '_ret', '_vol', '_ret+1', '_vol+1'])
    DD_doc = DD_doc.astype(pd.SparseDtype('int', 0))
        
    term_occur = (DD_doc > 0).sum(axis=0)
    min_occur = term_occur.quantile(q=kappa)

    if pprint:
        print(f'Removing terms occuring in < {int(min_occur)} reports')
        print(f"Most common term '{term_occur.idxmax()}' occurs in {round(term_occur.max()/N*100, 1)} % of reports")
    keep_term = term_occur >= min_occur
    DD_doc = DD_doc[keep_term[keep_term].index]
    if pprint:
        print(f'{DD_doc.shape[1]} terms remaining for screening')
    
    # SCREENING FOR SENTIMENT WORDS
    
    # Determine mean volatility
    # LATER, USE SOME OTHER PARTITION, THIS IS QUITE ARBITRARY
    # Only roughly 1/3 of reports have higher than mean volatiltiy(really?) ===> Change to median!
    if dep == '_vol':
        if vol_q:
            mean_vol = ts.quantile(q=vol_q)
        else:
            mean_vol = ts.median()
    else:
        mean_vol = 0
    # mean_vol = ts.median()
    high_vol = ts > mean_vol
    
    # Computing co-occurence frequencies of terms with low/high volatility
    vocab_list = list(DD_doc.columns)
    M = len(vocab_list)
    cooc_high = np.zeros(M)
        
    # Computing co-occurrence frequencies of terms with low/high volatility via matrix multiplication
    arts_incl_term = (DD_doc > 0).astype('int8')
    cooc_high = (arts_incl_term.T @ high_vol.values.astype('int8')) /arts_incl_term.sum(axis=0)
    
    # Selecting words
    ind_high = np.argsort(-cooc_high)[:alpha_high]
    ind_low = np.argsort(cooc_high)[:alpha_low]
    words_high = np.array(vocab_list)[ind_high]
    words_low = np.array(vocab_list)[ind_low]
    sent_words = np.concatenate([words_high, words_low])
    
    if pprint:
        print('-----------------')
        print(f'Words associated with HIGH {dep}:')
        print(words_high)
        print('------------------')
        print(f'Words associated wiht LOW {dep}')
        print(words_low)
        print('------------------')
        
    # LEARNING SENTIMENT TOPICS
    D_title = DD_doc[sent_words]
    assert (D_title > 0).sum(axis=0).min() >= min_occur, 'Term with too few occurences remaining!'
    s_hat = D_title.sum(axis=1)
    if pprint:
        print(f'{round((s_hat>0).sum()/len(s_hat)*100, 1)}% of reports have at least one sent-charged word')
    D_hat = D_title.div(s_hat * (s_hat>0) + 1 * (s_hat == 0), axis=0)

    
    # Estimating sentiment scores p_hat
    if vol_q:
        p_hat = (ts - ts.min())/(ts.max() - ts.min())
    else:
        temp = ts.argsort()
        ranks = np.empty_like(temp)
        ranks[temp] = np.arange(N)
        p_hat = ranks / N + 1/N
    W_hat = np.vstack((p_hat, 1-p_hat))
    
    # Estimating 0 by regressing D_title on W_hat
    O_hat = np.linalg.lstsq(W_hat.T, D_hat, rcond=None)[0].T
    
    # Removing negative values and normalising
    O_hat = O_hat*(O_hat >= 0)
    O_hat = O_hat/O_hat.sum(axis=0)
    
    #sent_tone = O_hat[:,0] - O_hat[:,1] # T
    #sent_freq = O_hat[:,0] + O_hat[:,1] # F

    return sent_words, O_hat

@time_log
def predict_sent(model, arts, llambda):
    """
    Predicts a sentiment score for a doc given a trained model

    Parameters
    ----------
    model : list of 2
        S_hat, O_hat.
    art : Dataframe with N rows and named columns representing terms
        Counts of all words.
    llambda : positive float
        Penalty term to shrink the estimated score towards 1/2, i.e. neutral 
    Returns
    -------
    p_hat : Array of size N with floats [0,1]
        Estimated sentiment score of given article.

    """
    S_hat = model[0]
    O_hat = model[1]
    N = arts.shape[0]
    
    DD = arts[S_hat]
    s_hat = np.sum(DD, axis=1)
    def p_fun(p, DD_i):
        sen = 1/DD_i.sum() * DD_i @ np.log((p*O_hat[:,0] + (1-p)*O_hat[:,1]))
        pen = llambda * np.log(p*(1-p))
        return -(sen+pen)

    
    p_hat = np.zeros(N)
    for i in range(N):
        if s_hat[i] == 0:
            # If an article contains no sentiment charged words it is marked neutral
            p_hat[i] = 0.5
        else:
            # This can almost centainly be vectorised!
            res = minimize(p_fun, args=(np.array(DD.iloc[[i]])), x0=0.5, method='SLSQP', bounds=[(1e-5,1-1e-5)])
            assert res.success, 'p_hat not coveraged'
            p_hat[i] = res.x[0]
            
    return p_hat
            
@time_log
def loss(p_hat, yy):
    """
    Computes simple loss difference between estimated sentiment scores and normalized rank of time series

    Parameters
    ----------
    p_hat : [0,1]^N array
        Estimated sentiment scores for N reports
    y : R^N array
        Time series labels (e.g. returns, volatility) corresponding to N reports.

    Returns
    -------
    ll : float [0,0.75]
        Loss.

    """
    N = len(yy)
    temp = yy.argsort()
    ranks = np.empty_like(temp)
    ranks[temp] = np.arange(N)
    yy_ranks = ranks/N + 1/N
    
    return np.sum(np.abs(yy_ranks - p_hat))/N
@time_log
def loss_perc(p_hat, yy, marg = 0.01, pprint = True):
    """
    Percentage of 10-k fillings correctly esimated as high/low volatility.

    Parameters
    ----------
    p_hat : [0,1]^N array
        Estimated sentiment scores for N articles
    y : R^N array
        Time series labels (e.g. returns, volatility) corresponding to N articles.
    marg : [0,0.5)
        Margin to use when selecting scored articles, 
        e.g. marg = 0.1 ==> only consider fillings scored with p_hat < 0.4 or p_hat > 0.6. The default is 0.01
    pprint : Boolean, optional
        Specifies whether to print progress (for analysis purposes). The default is True.

    Returns
    -------
    ll : float [0,1]
        Loss.

    """
    N = len(yy)
    med_vol = np.mean(yy)
    high_est = p_hat > (0.5 + marg)
    low_est = p_hat < (0.5 - marg)
    N_scored = high_est.sum() + low_est.sum()
    #assert N_scored > 0, 'No reports scored outside margin'
    if pprint:
        print(f'{round(N_scored/N*100, 2)}% reports estimated as high/low (margin: {marg})')
    correct_high = (high_est & (yy>med_vol)).sum()
    correct_low = (low_est & (yy<med_vol)).sum()
    
    return (correct_high + correct_low)/N_scored, N_scored/N
@time_log
def kalman(p_series, n_iter=100, smooth=False):

    kf = KalmanFilter(transition_matrices = [[1]], observation_matrices = [[1]])
    kf = kf.em(p_series, n_iter=n_iter)
    if smooth:
        ssm = kf.smooth(p_series)[0]
    else:
        ssm = kf.filter(p_series)[0]
        
    return pd.Series(ssm[:, 0], index=p_series.index)

