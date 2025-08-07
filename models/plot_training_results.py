import numpy as np
import matplotlib.pyplot as plt
from models.model_trainer import ModelTrainer
from config.config import LSTM_WINDOW_SIZE
import torch

# 1. Load models and data
trainer = ModelTrainer()
trainer.load_models()

# Load data and split
df = trainer.data_handler.load_data()
splits = trainer.data_handler.split_data(df)
y_train_arima, x_train_arima = splits['arima_train']
y_lstm_train, x_lstm_train = splits['lstm_train']
y_fin_test, x_fin_test = splits['final_test']

# 2. ARIMA predictions
arima_model = trainer.arima_model
arima_pred_lstm_train = arima_model.forecast(steps=len(y_lstm_train), exog=x_lstm_train)
arima_pred_fin = arima_model.forecast(steps=len(y_fin_test), exog=x_fin_test)

# 3. Residuals for LSTM
lstm_train_residuals = y_lstm_train.values - arima_pred_lstm_train.values
lstm_test_residuals = y_fin_test.values - arima_pred_fin.values

# 4. Prepare LSTM data (windowed)
X_lstm, y_lstm, _ = trainer.data_handler.prepare_lstm_data(lstm_train_residuals, y_lstm_train.index)
X_test_lstm, y_test_lstm, df_lstm_test = trainer.data_handler.prepare_lstm_data(lstm_test_residuals, y_fin_test.index)

# 5. Scale LSTM data (do not fit new scalers, use loaded)
scaled = trainer.data_handler.scale_lstm_data(X_lstm, y_lstm, X_test_lstm, y_test_lstm, fit_scalers=False)
X_lstm_scaled = scaled['X_train']
y_lstm_scaled = scaled['y_train']
X_test_lstm_scaled = scaled['X_test']
y_test_lstm_scaled = scaled['y_test']

# 6. LSTM predictions
lstm_model = trainer.lstm_model
lstm_model.eval()
with torch.no_grad():
    train_pred = lstm_model(torch.tensor(X_lstm_scaled, dtype=torch.float32)).cpu().numpy().flatten()
    test_pred = lstm_model(torch.tensor(X_test_lstm_scaled, dtype=torch.float32)).cpu().numpy().flatten()

# Inverse transform predictions
train_pred_inv = trainer.data_handler.inverse_transform_predictions(train_pred)
test_pred_inv = trainer.data_handler.inverse_transform_predictions(test_pred)

# 7. Plot 1: Actual vs ARIMA Prediction
plt.figure(figsize=(15, 8))
plt.plot(y_lstm_train.index, y_lstm_train.values, label='Actual GLD Price (Validation)', color='blue', linewidth=2)
plt.plot(y_lstm_train.index, arima_pred_lstm_train.values, label='ARIMA Prediction (Validation)', color='red', linewidth=2, linestyle='--')
plt.plot(y_fin_test.index, y_fin_test.values, label='Actual GLD Price (Test)', color='green', linewidth=2)
plt.plot(y_fin_test.index, arima_pred_fin.values, label='ARIMA Prediction (Test)', color='orange', linewidth=2, linestyle=':')
plt.title('GLD Price: Actual vs ARIMA Prediction')
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 8. Plot 2: Residuals - Actual vs LSTM Predicted
plt.figure(figsize=(15, 10))
plt.subplot(2, 1, 1)
plt.plot(y_lstm[LSTM_WINDOW_SIZE:], label='Actual Train Residuals', alpha=0.7)
plt.plot(train_pred_inv, label='Predicted Train Residuals', alpha=0.7)
plt.title('Training Set: Actual vs Predicted Residuals')
plt.legend()
plt.grid(True, alpha=0.3)

plt.subplot(2, 1, 2)
plt.plot(y_test_lstm[LSTM_WINDOW_SIZE:], label='Actual Test Residuals', alpha=0.7)
plt.plot(test_pred_inv, label='Predicted Test Residuals', alpha=0.7)
plt.title('Test Set: Actual vs Predicted Residuals')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# 9. Plot 3: Final Ensemble - Actual vs Model Prediction
final_predictions = arima_pred_fin.values[LSTM_WINDOW_SIZE:] + test_pred_inv
final_real = y_fin_test.values[LSTM_WINDOW_SIZE:]
final_dates = y_fin_test.index[LSTM_WINDOW_SIZE:]

plt.figure(figsize=(15, 8))
plt.plot(final_dates, final_predictions, label='ENSEMBLE MODEL PREDICTIONS', color='red', linewidth=2, linestyle='--')
plt.plot(final_dates, final_real, label='REAL GLD PRICE', color='blue', linewidth=2)
plt.title('GLD Price: Actual vs Model Prediction')
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Print RMSEs for comparison
from sklearn.metrics import mean_squared_error
print(f"Train RMSE: {np.sqrt(np.mean((y_lstm[LSTM_WINDOW_SIZE:] - train_pred_inv) ** 2)):.4f}")
print(f"Test RMSE: {np.sqrt(np.mean((y_test_lstm[LSTM_WINDOW_SIZE:] - test_pred_inv) ** 2)):.4f}")
print(f"Final Ensemble RMSE: {np.sqrt(np.mean((final_predictions - final_real) ** 2)):.4f}")