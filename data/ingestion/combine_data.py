import pandas as pd

def combine(df_gld, df_macro, df_news):
    df_gld = df_gld.copy()
    df_macro = df_macro.copy()
    df_news = df_news.copy()

    df = pd.concat([df_gld, df_macro, df_news], axis=1, join='inner', ignore_index=False)
    df.drop(columns=["Dividends","Stock Splits", "Capital Gains", "headlines", "currency", "headlines", "sentiments"], inplace=True, errors='ignore')
    return df

if __name__ == "__main__":
    df_gld = pd.read_csv("data/gld_daily.csv", index_col='date', parse_dates=True)
    df_macro = pd.read_csv("data/finbert_gold_news.csv", index_col='date', parse_dates=True)
    df_news = pd.read_csv("data/macro_data.csv", index_col='date', parse_dates=True)

    combined_df = combine(df_gld, df_macro, df_news)
    combined_df.to_csv("data/combined_data.csv")
