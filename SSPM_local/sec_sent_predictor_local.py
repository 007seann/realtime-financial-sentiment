import os
import time
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import sys
sys.path.append('/data/seanchoi/airflow')
from plugins.packages.SSPM.model import train_model, predict_sent, kalman
from airflow import DAG
from airflow.decorators import task
import datetime

class SentimentPredictor:
    def __init__(self, config):
        # Configuration
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
        dataset = pq.ParquetDataset(self.input_path)
        table = dataset.read()
        self.df_all = table.to_pandas()
        
        self.df_all = self.df_all.reset_index()
        self.df_all = self.df_all.sort_values(by=["Date", "_cik"]).reset_index(drop=True)
        self.df_all = self.df_all.drop(columns=["level_0"], errors="ignore")
        self.df_all = self.df_all.set_index("Date")
        self.df_all["_ret"] = self.df_all["_ret"].astype("float32") / 100
        self.df_all["_vol"] = self.df_all["_vol"].astype("float32")
        self.df_all = self.df_all.fillna(0.0)
        print('Data loaded and preprocessed successfully.')

    def train_model(self):
        # Train model for '_ret' and '_vol'
        df_trn = self.df_all.sort_index()[: f"{self.window}"]
        kappa = self.adj_kappa(0.9)
        alpha_high = 100
        llambda = 0.1

        for dep in ["_ret", "_vol"]:
            mod = train_model(df_trn, dep, kappa, alpha_high, pprint=False)
            preds = predict_sent(mod, df_trn.drop(columns=["_cik", "_vol", "_ret", "_vol+1", "_ret+1"]), llambda)

            if dep == "_ret":
                self.mod_sent_ret = pd.Series(preds, index=df_trn.index)
            else:
                self.mod_sent_vol = pd.Series(preds, index=df_trn.index)
    # This is for Market Analysis     
    def kalman_filter(self):
        mod_avg_ret = self.mod_sent_ret.groupby(self.mod_sent_ret.index).mean()
        mod_avg_vol = self.mod_sent_vol.groupby(self.mod_sent_vol.index).mean()
        mod_kal_ret = kalman(mod_avg_ret, smooth=True)
        mod_kal_vol = kalman(mod_avg_vol, smooth=True)

        return mod_kal_ret, mod_kal_vol

    def adj_kappa(self, k, k_min=0.85):
        return 1 - (1 - k) / (1 - k_min)

    def save_results(self):
        mod_sent_ret, mod_sent_vol = self.kalman_filter()
        mod_sent_ret.to_csv(f"{self.fig_loc}/mod_sent_ret.csv")
        mod_sent_vol.to_csv(f"{self.fig_loc}/mod_sent_vol.csv")

    def run(self):
        # Run the entire workflow
        print("Starting sentiment analysis...")
        self.load_data()
        self.train_model()
        self.save_results()

    
if __name__ == "__main__":
    # Read configuration from command-line arguments or environment variables
    base_path = "/data/seanchoi/airflow/"  # Correct base path
    config = {
        "constituents_path": os.path.join(base_path, sys.argv[1]),
        "fig_loc": os.path.join(base_path, sys.argv[2]),
        "input_path": os.path.join(base_path, sys.argv[3]),
        "window": sys.argv[4],
    }

    predictor = SentimentPredictor(config)
    predictor.run()