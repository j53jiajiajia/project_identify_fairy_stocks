import yfinance as yf
import sqlite3
import numpy as np

# This list contains 40 well-known US stocks and their symbols.
my_list = [
    "AAPL",  # Apple Inc.
    "MSFT",  # Microsoft Corporation
    "AMZN",  # Amazon.com Inc.
    "GOOGL", # Alphabet Inc. (Google)
    "TSLA",  # Tesla, Inc.
    "VFS",   # VINFAST AUTO LTD 妖股
    "JPM",   # JPMorgan Chase & Co.
    "COIN", # COINBASE GLOBAL INC 妖股
    "JNJ",   # Johnson & Johnson
    "V",     # Visa Inc.
    "PG",    # Procter & Gamble Company
    "HD",    # The Home Depot, Inc.
    "MA",    # Mastercard Incorporated
    "DIS",   # The Walt Disney Company
    "VZ",    # Verizon Communications Inc.
    "T",     # AT&T Inc.
    "BAC",   # Bank of America Corporation
    "CSCO",  # Cisco Systems, Inc.
    "TUP",   # TUPPERWARE BRANDS CORPORATION 妖股
    "PFE",   # Pfizer Inc.
    "MRK",   # Merck & Co., Inc.
    "GME",   # GAMESTOP CORPORATION 妖股
    "INTC",  # Intel Corporation
    "KO",    # The Coca-Cola Company
    "PEP",   # PepsiCo, Inc.
    "WMT",   # Walmart Inc.
    "XOM",   # Exxon Mobil Corporation
    "NKE",   # NIKE, Inc.
    "ABT",   # Abbott Laboratories
    "GOOG",  # Alphabet Inc. (Google)
    "CVX",   # Chevron Corporation
    "COST",  # Costco Wholesale Corporation
    "MCD",   # McDonald's Corporation
    "IBM",   # International Business Machines Corporation
    "TMO",   # Thermo Fisher Scientific Inc.
    "HON",   # Honeywell International Inc.
    "PM",    # Philip Morris International Inc.
    "LOW",   # Lowe's Companies, Inc.
    "XOM",   # Exxon Mobil Corporation
    "CAT",   # Caterpillar Inc.
    "CVNA"   # CARVANA CO 妖股
]


# Get stock data and build a database consisting of stock prices every 30 minutes.
us_tickers = my_list
db_name = 'us_stock_data.db'
conn = sqlite3.connect(db_name)

fairy_stocks_dict = {}
for ticker in us_tickers:
    # Fetch the most recent stock data
    stock_data = yf.download(ticker, period="60d", interval="30m")
    if not stock_data.empty:
        # Calculate log returns
        stock_data['Log_Return'] = 100 * np.log(stock_data['Close'] / stock_data['Close'].shift(1))
        # Keep only the latest 'max_rows' data points
        max_rows = 774
        stock_data = stock_data.tail(max_rows)
        table_name = ticker.replace("-", "_")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date TIMESTAMP PRIMARY KEY,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT,
            Log_Return FLOAT
        )
        """
        conn.execute(create_table_query)

        insert_query = f"""
        INSERT OR REPLACE INTO {table_name} (Date, Open, High, Low, Close, Volume, Log_Return)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        data_to_insert = [
            (
                date.strftime('%Y-%m-%d %H:%M:%S'),
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume'],
                row['Log_Return']
            )
            for date, row in stock_data.iterrows()
        ]
        conn.executemany(insert_query, data_to_insert)

        # Get the current number of rows in the table
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        current_rows = conn.execute(count_query).fetchone()[0]
        # Calculate how many rows to delete to maintain the desired row count
        rows_to_delete = max(current_rows - max_rows, 0)
        if rows_to_delete > 0:
            # Delete the oldest rows
            delete_query = f"DELETE FROM {table_name} WHERE Date IN (SELECT Date FROM {table_name} ORDER BY Date ASC LIMIT {rows_to_delete})"
            conn.execute(delete_query)
        conn.commit()

        fairy_stocks_reasons = []

        # Identify stock with Unusual Price Movements
        cleaned_data = stock_data[np.isfinite(stock_data['Log_Return'])]
        cleaned_data = cleaned_data.copy()
        mean_return = cleaned_data['Log_Return'].mean()
        std_return = cleaned_data['Log_Return'].std()
        cleaned_data['Log_Return_ZScore'] = (cleaned_data['Log_Return'] - mean_return) / std_return
        # Define a threshold for abnormal returns (e.g., z-score above a certain threshold)
        zscore_threshold = 15.0
        # Identify dates with unusual return rate
        unusual_return_dates = cleaned_data[abs(cleaned_data['Log_Return_ZScore']) > zscore_threshold]
        if not unusual_return_dates.empty:
            # print(f"unusual return rate for {ticker}:")
            # print(unusual_return_dates)
            fairy_stocks_reasons.append(
                f"Unusual return rate on {', '.join(unusual_return_dates.index.strftime('%Y-%m-%d %H:%M:%S'))}")

        # Identify stock with High Volatility
        stock_data = stock_data.copy()
        stock_data['max_price_change_rate'] = (stock_data['High'] - stock_data['Low'])/stock_data['Low']
        max_price_change_rate_threshold = 0.1
        high_Volatility_dates = stock_data[stock_data['max_price_change_rate'] > max_price_change_rate_threshold]
        if not high_Volatility_dates.empty:
            # print(f"High Volatility anomalies for {ticker}:")
            # print(high_Volatility_dates)
            fairy_stocks_reasons.append(
                f"High volatility on {', '.join(high_Volatility_dates.index.strftime('%Y-%m-%d %H:%M:%S'))}")

        # Identify stock with High Trading Volume
        stock_data = stock_data.copy()
        stock_data['Volume_ZScore'] = (stock_data['Volume'] - stock_data['Volume'].mean()) / stock_data['Volume'].std()
        zscore_threshold_volume = 15.0
        high_volume_dates = stock_data[stock_data['Volume_ZScore'] > zscore_threshold_volume]
        if not high_volume_dates.empty:
            # print(f"High trading volume anomalies for {ticker}:")
            # print(high_volume_dates)
            fairy_stocks_reasons.append(
                f"High trading volume on {', '.join(high_volume_dates.index.strftime('%Y-%m-%d %H:%M:%S'))}")

        if fairy_stocks_reasons:
            # Store the list of reasons as the value in the dictionary
            fairy_stocks_dict[ticker] = fairy_stocks_reasons

conn.close()

# Print the dictionary of fairy stocks and their reasons
for stock, reasons in fairy_stocks_dict.items():
    print(f"Fairy Stock: {stock}")
    print(f"Reasons: {'; '.join(reasons)}\n")


conn = sqlite3.connect('fairy_stocks.db')

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Define the SQL command to create the 'fairy_stocks_list' table
create_table_query = '''
CREATE TABLE IF NOT EXISTS fairy_stocks_list (
    fairy_stock_symbol TEXT PRIMARY KEY,
    reasons_to_be_fairy_stocks TEXT,
    tip TEXT
);
'''

# Execute the SQL command to create the table
cursor.execute(create_table_query)

# Insert data into the 'fairy_stocks_list' table
for stock, reasons in fairy_stocks_dict.items():
    tip = None
    abnormal_data_num = 1
    for i in reasons:
        abnormal_data_num += i.count(",")
    if abnormal_data_num <= 3:
        tip = f"The abnormal data is {abnormal_data_num} (less than or equal to 3 data), please check if any obvious reason or news that would justify such a change. If yes, it may not be a fairy stock."

    # Insert data into the table
    cursor.execute("INSERT OR REPLACE INTO fairy_stocks_list (fairy_stock_symbol, reasons_to_be_fairy_stocks, tip) VALUES (?, ?, ?)",
                   (stock, '; '.join(reasons), tip))

# Commit the changes and close the connection
conn.commit()
conn.close()
