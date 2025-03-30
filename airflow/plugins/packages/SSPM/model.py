#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15

@author: Sean Sanggyu Choi
"""
import pandas as pd
import numpy as np
from scipy.optimize import minimize
from scipy.sparse import csr_matrix
from pykalman import KalmanFilter
from plugins.common.time_log_decorator import time_log

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
    #DD = DD.drop(DD.columns[pd.concat([(term_occur < min_occur),pd.Series([False],index=[dep])])], axis=1)
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

