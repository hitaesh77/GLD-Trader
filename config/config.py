import os

# Model parameters
ARIMA_ORDER = (1, 1, 0)
ARIMA_MAX_ITER = 100
LSTM_WINDOW_SIZE = 14
LSTM_HIDDEN_SIZE = 16
LSTM_DROPOUT = 0.5
LSTM_EPOCHS = 1000
LSTM_BATCH_SIZE = 32
LSTM_LEARNING_RATE = 0.1
LSTM_MOMENTUM = 0.9

# Trading parameters
PRICE_THRESHOLD = 0.01 
STOP_LOSS_PCT = 0.05
TAKE_PROFIT_PCT = 0.03 

# Data split ratios
ARIMA_TRAIN_RATIO = 0.6
LSTM_TRAIN_RATIO = 0.6
TEST_RATIO = 0.4

# Feature columns
TARGET_COLUMN = 'Close'
EXOG_COLUMNS = [
    'EMA_30', 'EMA_60', 'EMA_200',
    'rsi_14',
    'MACD', 'MACD_Signal', 'MACD_Histogram',
    'BB_Middle', 'BB_Upper', 'BB_Lower',
    'Momentum_10',

    'DTWEXBGS',   
    'OIL_PRICE',          
    'FEDFUNDS',           
    'CPIAUCSL',  

    'Volume'
]

# Retraining schedule
RETRAIN_FREQUENCY = 'weekly'  # 'daily', 'weekly', 'monthly'
MIN_DAYS_BETWEEN_RETRAINS = 7