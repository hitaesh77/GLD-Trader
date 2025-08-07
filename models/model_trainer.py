import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pickle
import os
import logging
from datetime import datetime
from data_handler.data_handler import DataHandler
from config.config import *

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='logs/training.log', 
    filemode='a'
)
logger = logging.getLogger(__name__)

class LSTMDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

class LSTMModel(nn.Module):
    def __init__(self, input_size=1, hidden_size=LSTM_HIDDEN_SIZE, 
                 output_size=1, dropout=LSTM_DROPOUT):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True, dropout=dropout)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        lstm_out = lstm_out[:, -1, :]  
        return self.fc(lstm_out)

class ModelTrainer:
    def __init__(self):
        self.data_handler = DataHandler()
        self.arima_model = None
        self.lstm_model = None
        self.training_history = {}
        
    def train_arima_model(self, y_train, x_train, order=ARIMA_ORDER):
        logger.info("Training SARIMAX model...")
        
        try:
            arima_model = SARIMAX(y_train, exog=x_train, order=order)
            fitted_arima = arima_model.fit(disp=False, maxiter=ARIMA_MAX_ITER)
            
            self.arima_model = fitted_arima
            logger.info("SARIMAX model training completed successfully")
            
            return fitted_arima
            
        except Exception as e:
            logger.error(f"Error training SARIMAX model: {str(e)}")
            raise
    
    def calculate_residuals(self, fitted_arima, y_test, x_test):
        arima_predictions = fitted_arima.forecast(steps=len(y_test), exog=x_test)
        residuals = y_test.values - arima_predictions.values
        return residuals, arima_predictions
    
    def train_lstm_model(self, X_train, y_train, X_test=None, y_test=None):
        logger.info("Training LSTM model...")
        
        # Create datasets
        train_dataset = LSTMDataset(X_train, y_train)
        train_loader = DataLoader(train_dataset, batch_size=LSTM_BATCH_SIZE, shuffle=True)
        
        if X_test is not None and y_test is not None:
            test_dataset = LSTMDataset(X_test, y_test)
            test_loader = DataLoader(test_dataset, batch_size=LSTM_BATCH_SIZE, shuffle=False)
        else:
            test_loader = None
        
        # Initialize model
        model = LSTMModel()
        criterion = nn.MSELoss()
        optimizer = optim.SGD(model.parameters(), lr=LSTM_LEARNING_RATE, momentum=LSTM_MOMENTUM)
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=10, factor=0.5)
        
        # Training loop
        train_losses = []
        test_losses = []
        
        for epoch in range(1, LSTM_EPOCHS + 1):
            model.train()
            epoch_loss = 0
            
            for X_batch, y_batch in train_loader:
                optimizer.zero_grad()
                y_pred = model(X_batch)
                loss = criterion(y_pred, y_batch)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
            
            avg_train_loss = epoch_loss / len(train_loader)
            train_losses.append(avg_train_loss)
            
            # Evaluate on test set
            if test_loader is not None:
                model.eval()
                test_loss = 0
                with torch.no_grad():
                    for X_batch, y_batch in test_loader:
                        y_pred = model(X_batch)
                        loss = criterion(y_pred, y_batch)
                        test_loss += loss.item()
                
                avg_test_loss = test_loss / len(test_loader)
                test_losses.append(avg_test_loss)
                
                scheduler.step(avg_test_loss)
                
                if epoch % 100 == 0:
                    train_rmse = np.sqrt(avg_train_loss)
                    test_rmse = np.sqrt(avg_test_loss)
                    logger.info(f"Epoch {epoch:04d} — Train RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}")
        
        self.lstm_model = model
        self.training_history = {
            'train_losses': train_losses,
            'test_losses': test_losses if test_losses else None,
            'final_train_rmse': np.sqrt(train_losses[-1]),
            'final_test_rmse': np.sqrt(test_losses[-1]) if test_losses else None
        }
        
        logger.info("LSTM model training completed successfully")
        return model
    
    def full_training_pipeline(self):
        logger.info("Starting full training pipeline...")
        
        # Load and split data
        df = self.data_handler.load_data()
        data_splits = self.data_handler.split_data(df)
        
        # Train ARIMA model
        y_train_arima, x_train_arima = data_splits['arima_train']
        fitted_arima = self.train_arima_model(y_train_arima, x_train_arima)
        
        # Calculate residuals for LSTM training
        y_lstm_train, x_lstm_train = data_splits['lstm_train']
        lstm_residuals, _ = self.calculate_residuals(fitted_arima, y_lstm_train, x_lstm_train)
        
        # Prepare LSTM data
        X_lstm, y_lstm, _ = self.data_handler.prepare_lstm_data(lstm_residuals, y_lstm_train.index)
        
        # For test residuals (if needed for validation)
        y_fin_test, x_fin_test = data_splits['final_test']
        test_residuals, _ = self.calculate_residuals(fitted_arima, y_fin_test, x_fin_test)
        X_test_lstm, y_test_lstm, _ = self.data_handler.prepare_lstm_data(test_residuals, y_fin_test.index)
        
        # Scale data
        scaled_data = self.data_handler.scale_lstm_data(X_lstm, y_lstm, X_test_lstm, y_test_lstm)
        
        # Train LSTM model
        self.train_lstm_model(
            scaled_data['X_train'], scaled_data['y_train'],
            scaled_data['X_test'], scaled_data['y_test']
        )
        
        logger.info("Full training pipeline completed successfully")
        
        # Save models
        self.save_models()
        
        return {
            'arima_model': self.arima_model,
            'lstm_model': self.lstm_model,
            'training_history': self.training_history
        }
    
    def save_models(self):
        """Save trained models and scalers"""
        logger.info("Saving models...")

        os.makedirs("trained_models", exist_ok=True)
        
        # Save ARIMA model
        arima_path = os.path.join("trained_models", "arima_model.pkl")
        with open(arima_path, 'wb') as f:
            pickle.dump(self.arima_model, f)
        
        # Save LSTM model
        lstm_path = os.path.join("trained_models", "lstm_model.pth")
        torch.save(self.lstm_model.state_dict(), lstm_path)
        
        # Save model architecture info
        model_info = {
            'lstm_architecture': {
                'input_size': 1,
                'hidden_size': LSTM_HIDDEN_SIZE,
                'output_size': 1,
                'dropout': LSTM_DROPOUT
            },
            'arima_order': ARIMA_ORDER,
            'training_date': datetime.now().isoformat(),
            'training_history': self.training_history
        }
        
        info_path = os.path.join("trained_models", "model_info.pkl")
        with open(info_path, 'wb') as f:
            pickle.dump(model_info, f)
        
        # Save scalers
        self.data_handler.save_scalers()
        
        logger.info(f"Models saved to {"trained_models"}")
    
    def load_models(self):
        """Load trained models"""
        logger.info("Loading models...")
        
        try:
            # Load ARIMA model
            arima_path = os.path.join("trained_models", "arima_model.pkl")
            with open(arima_path, 'rb') as f:
                self.arima_model = pickle.load(f)
            
            # Load model info
            info_path = os.path.join("trained_models", "model_info.pkl")
            with open(info_path, 'rb') as f:
                model_info = pickle.load(f)
            
            # Initialize LSTM model with saved architecture
            arch = model_info['lstm_architecture']
            self.lstm_model = LSTMModel(
                input_size=arch['input_size'],
                hidden_size=arch['hidden_size'],
                output_size=arch['output_size'],
                dropout=arch['dropout']
            )
            
            # Load LSTM weights
            lstm_path = os.path.join("trained_models", "lstm_model.pth")
            self.lstm_model.load_state_dict(torch.load(lstm_path))
            self.lstm_model.eval()
            
            # Load scalers
            self.data_handler.load_scalers()
            
            self.training_history = model_info.get('training_history', {})
            
            logger.info("Models loaded successfully")
            
        except FileNotFoundError as e:
            logger.error(f"Model files not found: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading models: {str(e)}")
            raise

if __name__ == "__main__":
    trainer = ModelTrainer()
    results = trainer.full_training_pipeline()
    
    print("Training completed!")
    print(f"Final LSTM Train RMSE: {results['training_history']['final_train_rmse']:.4f}")
    if results['training_history']['final_test_rmse']:
        print(f"Final LSTM Test RMSE: {results['training_history']['final_test_rmse']:.4f}")