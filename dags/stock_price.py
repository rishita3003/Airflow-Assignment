from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import yfinance as yf
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'stock_price_prediction',
    default_args=default_args,
    description='Fetch and predict stock prices',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False
)

def fetch_data():
    tickers = ["AAPL", "GOOGL", "META", "MSFT", "AMZN"]
    data = {ticker: yf.Ticker(ticker).history(period="max") for ticker in tickers}
    for ticker, df in data.items():
        print(f"Saving data for {ticker}")
        df.to_csv(f'/home/rishita/airflow/tmp/{ticker}_data.csv')

def train_and_predict():
    tickers = ["AAPL", "GOOGL", "META", "MSFT", "AMZN"]
    errors = []

    for ticker in tickers:
        print("ticker: ", ticker)
        df = pd.read_csv(f'/home/rishita/airflow/tmp/{ticker}_data.csv')
        # print("Original Date Column:", df['Date'].head())
        
        # df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        # print("Converted Date Column:", df['Date'].head())

        for i in range(1, 6):  # Last 5 days
            train_df = df[:-i]  # Training on all but the last 'i' rows
            test_df = test_df = df.iloc[-i-1:-i] #Testing on the last ith row
            print(f"Training on {len(train_df)} samples, testing on {len(test_df)} samples")
            print(f"Training on {train_df}")
            print(f"Testing on {test_df}")
            model = LinearRegression()
            X = train_df[['Open', 'High', 'Low', 'Close', 'Volume']]
            y = train_df['High']
            model.fit(X, y)
            prediction = model.predict(test_df[['Open', 'High', 'Low', 'Close', 'Volume']])
            print(f"Prediction for {ticker} on {test_df['Date'].values[0]}: {prediction[0]}")
            actual = test_df['High'].values[0]
            relative_error = (prediction[0] - actual) / actual
            errors.append([ticker, test_df['Date'].values[0], relative_error])
    
    print("Errors: ", errors)
    error_df = pd.DataFrame(errors, columns=['Ticker', 'Date', 'Relative_Error'])
    pivot_df = error_df.pivot(index='Date', columns='Ticker', values='Relative_Error')
    pivot_df.to_csv('/home/rishita/airflow/tmp/final_errors.csv', index=False)
    print("Errors saved to /home/rishita/airflow/tmp/final_errors.csv")

fetch_data_operator = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

train_predict_operator = PythonOperator(
    task_id='train_and_predict',
    python_callable=train_and_predict,
    dag=dag,
)

fetch_data_operator >> train_predict_operator
