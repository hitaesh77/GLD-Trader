# data_handler.py
"""
Data handling and preprocessing module for GLD ETF Trading Bot
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pickle
import os
from config import *

class DataHandler:
    def __init__(self):
        self.scalers = {
            'x_scaler': None,
            'y_scaler': None
        }
    
    def load_data(self, filepath=None):
        """Load data from CSV file"""
        if filepath is None:
            filepath = COMBINED_DATA_FILE
        
        try:
            df = pd.read_csv(filepath, index_col="date", parse_dates=True)
            df = df.dropna()
            return df
        except Exception as e:
            raise Exception(f"Error loading data from {filepath}: {str(e)}")
    
    def split_data(self, df, target_col=TARGET_COLUMN, exog_cols=EXOG_COLUMNS):
        """Split data into ARIMA train, LSTM train, and test sets"""
        # First split: ARIMA training vs remaining
        arima_train, holdout = train_test_split(
            df, test_size=(1 - ARIMA_TRAIN_RATIO), shuffle=False
        )
        
        # Second split: LSTM training vs test
        lstm_train, test = train_test_split(
            holdout, test_size=TEST_RATIO, shuffle=False
        )
        
        # Prepare ARIMA data
        y_train_arima = arima_train[target_col]
        x_train_arima = arima_train[exog_cols]
        
        # LSTM uses the middle portion for training
        y_test_arima = lstm_train[target_col]  
        x_test_arima = lstm_train[exog_cols]
        
        # Final test set
        y_fin_test = test[target_col]
        x_fin_test = test[exog_cols]
        
        return {
            'arima_train': (y_train_arima, x_train_arima),
            'lstm_train': (y_test_arima, x_test_arima),
            'final_test': (y_fin_test, x_fin_test)
        }
    
    def prepare_lstm_data(self, residuals, dates, window_size=LSTM_WINDOW_SIZE):
        """Prepare residual data for LSTM training"""
        df_residuals = pd.DataFrame({
            'date': dates,
            'residuals': residuals
        })
        
        X, y = [], []
        for i in range(len(df_residuals) - window_size):
            X.append(df_residuals['residuals'].values[i:i + window_size])
            y.append(df_residuals['residuals'].values[i + window_size])
        
        return np.array(X), np.array(y), df_residuals
    
    def scale_lstm_data(self, X_train, y_train, X_test=None, y_test=None, fit_scalers=True):
        """Scale LSTM data using StandardScaler"""
        # Reshape for scaling
        n_train, window = X_train.shape
        X_train_flat = X_train.reshape(-1, 1)
        y_train_flat = y_train.reshape(-1, 1)
        
        if fit_scalers:
            # Fit scalers on training data
            self.scalers['x_scaler'] = StandardScaler()
            self.scalers['y_scaler'] = StandardScaler()
            
            self.scalers['x_scaler'].fit(X_train_flat)
            self.scalers['y_scaler'].fit(y_train_flat)
        
        # Transform training data
        X_train_scaled = self.scalers['x_scaler'].transform(X_train_flat)
        y_train_scaled = self.scalers['y_scaler'].transform(y_train_flat)
        
        # Reshape back
        X_train_scaled = X_train_scaled.reshape(n_train, window, 1)
        y_train_scaled = y_train_scaled.reshape(n_train, 1)
        
        result = {
            'X_train': X_train_scaled,
            'y_train': y_train_scaled
        }
        
        # Transform test data if provided
        if X_test is not None and y_test is not None:
            n_test = X_test.shape[0]
            X_test_flat = X_test.reshape(-1, 1)
            y_test_flat = y_test.reshape(-1, 1)
            
            X_test_scaled = self.scalers['x_scaler'].transform(X_test_flat)
            y_test_scaled = self.scalers['y_scaler'].transform(y_test_flat)
            
            X_test_scaled = X_test_scaled.reshape(n_test, window, 1)
            y_test_scaled = y_test_scaled.reshape(n_test, 1)
            
            result.update({
                'X_test': X_test_scaled,
                'y_test': y_test_scaled
            })
        
        return result
    
    def inverse_transform_predictions(self, predictions):
        """Inverse transform LSTM predictions"""
        if self.scalers['y_scaler'] is None:
            raise ValueError("Y scaler not fitted. Cannot inverse transform.")
        
        return self.scalers['y_scaler'].inverse_transform(
            predictions.reshape(-1, 1)
        ).flatten()
    
    def save_scalers(self, filepath=None):
        """Save scalers to file"""
        if filepath is None:
            filepath = os.path.join(MODEL_DIR, "scalers.pkl")
        
        with open(filepath, 'wb') as f:
            pickle.dump(self.scalers, f)
    
    def load_scalers(self, filepath=None):
        """Load scalers from file"""
        if filepath is None:
            filepath = os.path.join(MODEL_DIR, "scalers.pkl")
        
        try:
            with open(filepath, 'rb') as f:
                self.scalers = pickle.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Scalers file not found: {filepath}")
    
    def get_latest_data(self, n_days=LSTM_WINDOW_SIZE):
        """Get the latest n days of data for prediction"""
        df = self.load_data()
        return df.tail(n_days + 1)  # +1 to account for the prediction target
    
    def get_prediction_features(self, latest_data):
        """Extract features needed for next-day prediction"""
        # Get the latest exogenous variables for ARIMA
        arima_features = latest_data[EXOG_COLUMNS].iloc[-1:].values
        
        # Calculate residuals for the last LSTM_WINDOW_SIZE days
        # This would need the trained ARIMA model to compute residuals
        # For now, we'll return the structure
        
        return {
            'arima_features': arima_features,
            'latest_data': latest_data
        }