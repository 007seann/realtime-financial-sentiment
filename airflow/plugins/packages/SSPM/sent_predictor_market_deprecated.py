import os
import time
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from plugins.packages.PDCM.vol_reader_fun import price_reader
from plugins.packages.SSPM.model import train_model_spark, predict_sent_spark, kalman_spark
from pyspark.sql.functions import col, to_timestamp, date_format
from pyspark.sql import functions as F
class SentimentPredictor:
    def __init__(self, spark, config):
        # Configuration
        self.spark = spark
        self.constituents_path = config.get("constituents_path")
        self.fig_loc = config.get("fig_loc")
        self.input_path = config.get("input_path")
        self.window = config.get("window")

        # Create output directory if it doesn't exist
        if not os.path.exists(self.fig_loc):
            os.makedirs(self.fig_loc)

        # Initialize variables
        self.df_all = None
        self.mod_sent_ret = None
        self.mod_sent_vol = None

    def load_data(self):
        # Load main dataset
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        self.df_all = self.spark.read.parquet(self.input_path)
        self.df_all = self.df_all.withColumn("_ret", col("_ret") / 100).fillna(0.0)
        self.df_all = self.df_all.drop("level_0")
        self.df_all = self.df_all.orderBy(['Date', '_cik'])
        self.df_all = self.df_all.repartition(4)  # Adjust the number of partitions as needed
        self.df_all.persist() 
        print('Data loaded and preprocessed successfully.')

    def train_model(self):
        # Train model for '_ret' and '_vol'
        df_trn = self.df_all.filter((col("Date") <= self.window))
        kappa = self.adj_kappa(0.9)
        alpha_high = 100
        llambda = 0.1

        for dep in ["_ret", "_vol"]:
            mod = train_model_spark(df_trn, dep, kappa, alpha_high, pprint=False)
            
            meta_cols = ['_cik', '_ret', '_vol', '_ret+1', '_vol+1']
            train_arts = df_trn.drop(*meta_cols)
            train_arts = train_arts.withColumn("Date", col("Date").cast("string"))  # Convert Date to string for Spark compatibility
            
            preds = predict_sent_spark(mod, train_arts, llambda)

            if dep == "_ret":
                self.mod_sent_ret = preds.select("Date", "p_hat")
            else:
                self.mod_sent_vol = preds.select("Date", "p_hat")
                
        self.kalman_filter()
        print('Model training and prediction completed successfully.')
        
    # This is for Market Analysis     
    def kalman_filter(self):
        mod_avg_ret = self.mod_sent_ret.groupBy("Date").agg(F.mean("p_hat").alias("p_hat"))
        mod_avg_vol = self.mod_sent_vol.groupBy("Date").agg(F.mean("p_hat").alias("p_hat"))
        self.mod_sent_ret = kalman_spark(mod_avg_ret)
        self.mod_sent_vol = kalman_spark(mod_avg_vol)


    def adj_kappa(self, k, k_min=0.85):
        return 1 - (1 - k) / (1 - k_min)

    def save_results(self):
        self.mod_sent_ret.write.parquet(f"{self.fig_loc}/mod_sent_ret.parquet", mode="overwrite")
        self.mod_sent_vol.write.parquet(f"{self.fig_loc}/mod_sent_vol.parquet", mode="overwrite")
        print('Results saved successfully.')

    def run(self):
        # Run the entire workflow
        print("Starting sentiment analysis...")
        self.load_data()
        self.train_model()
        self.save_results()
