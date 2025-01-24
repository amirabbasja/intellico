from typing import Any, Dict, Optional, List
from dotenv import load_dotenv 
import pandas as pd
from time import sleep
from decimal import Decimal
from requests.exceptions import RequestException
from psycopg2 import Error
from web3 import Web3
import re, time, psycopg2, requests, json, os, csv
from datetime import datetime
import mplfinance as mpf
import pandas as pd
from tqdm import tqdm
import numpy as np

######## Stand alone general utility functions ########
def coinGeckoCandles(poolAddress: str, network: str, timeframe: str, startDate: datetime = None, endDate: datetime = None, limit: int = 1000, plotDetails: dict = {"plot":False, "type":"mplfinance"}, verbose = False) -> pd.DataFrame:
    """
    Get OHLCVs from coingecko public api. Charts prices are returned in USD.
    
    Args:
        poolAddress (str): The address of the target pool
        network (str): Name of the network that pool belongs to.
        timeframe (str): The chart timeframe. available options:[1min, 5min, 15min, 1h,
            4h, 12h, 1d]
        startDate (datetime): Get candles before this date.
        endDate (datetime): Get candles until this date back in time.
        limit (int): Maximum number of candles to return in each batch. No more than 1000.
        plotDetails (dict): The details for plotting the candlestick chart. Redundant if 
            plot is False. Acceptable key-value pairs: 
            {"plot":True or False} -> Plot the candlestick data
            {"type":"mplfinance" or "type":"plotly"} -> What library to use for plotting
        verbose (bool): Pass true to see what app is doing.
    
    Returns:
        A pandas dataframe with columns ['Open', 'Close', 'High', 'Low', 'Volume'] and Date 
        as its index.
    """
    
    # Check parameters
    if 1000 < limit:
        raise Exception("Candle limit should be no more than 1000.")
    
    _tf = ""
    _aggregate = ""
    if timeframe.lower() in ["1min", "5min", "15min", "1h", "4h", "12h", "1d"]:
        if "min" in timeframe.lower():
            _tf = "minute"
            _aggregate = timeframe.replace("min","")
            if _aggregate not in ["1", "5", "15"]: raise Exception(f"Wrong number for timeframe {timeframe}")
        elif "h" in timeframe.lower():
            _tf = "hour"
            _aggregate = timeframe.replace("h","")
            if _aggregate not in ["1", "4", "12"]: raise Exception(f"Wrong number for timeframe {timeframe}")
        elif "d" in timeframe.lower():
            _tf = "day"
            _aggregate = timeframe.replace("d","")
            if _aggregate not in ["1"]: raise Exception(f"Wrong number for timeframe {timeframe}")
    else:
        raise Exception("Unacceptable timeframe passed. Acceptable values: [1min, 5min, 15min, 1h, 4h, 12h, 1d].")
    
    # Specifying network, desired pool, and time granularity
    parameters = f'{network}/pools/{poolAddress}'
    specificity = f'{1}'
    
    # Set the start and end of our search for candles
    _startTimestamp = str(int(startDate.timestamp())) if startDate else str(int(time.time()))
    _endTimestamp = None if not endDate else str(int(endDate.timestamp()))
    
    try:
        # Start accumulating candles
        candles = []
        _stop = False
        _batchHead = _startTimestamp
        _errorCounter = 0
        while not _stop:
            # Make the request
            url = f'https://api.geckoterminal.com/api/v2/networks/{parameters}/ohlcv/{_tf}' 
            params = {
                "limit":limit,
                "aggregate":_aggregate,
                "before_timestamp":_batchHead,
            }
            
            # Handle rate limit
            try:
                if verbose: print(f"Getting a new batch from {_batchHead} with {limit} candles.")
                response = requests.get(url,params)
                data = response.json()
                meta = data["meta"]
                
                # If we have a key named "meta", means that we are not rate limited
                _errorCounter = 0
                
                # Handling errors
                if "errors" in data:
                    for err in data['errors']:
                        print(f"Couldn't get klines. Error: {err['title']}\n")
                
                # Adding data to the candles list
                data = data['data']['attributes']['ohlcv_list']
                
                # SOme times there are no data, we handle that here
                if 0 < len(data):
                    if min(int(data[0][0]), int(data[-1][0])) <= int(_endTimestamp):
                        # Reached stopping point
                        for kline in data:
                            # Only add candles that their timestamp is bigger than _endTimestamp
                            if int(_endTimestamp) < int(kline[0]):
                                candles.append(kline)
                                
                        _stop = True
                        if verbose: print("End date reached. Stopping...")
                    else:
                        # Continue fetching candles with updating next batch's head
                        candles += data
                        _batchHead = str(min(int(data[0][0]), int(data[-1][0])))
                else:
                    _stop = True
                
            except Exception as ex:
                _errorCounter += 1
                if _errorCounter == 6: 
                    print("Waited for 3 minutes but still rate limited. breaking")
                    return None
                
                if "rate limit" in response.text.lower():
                    if verbose: print("Rate limited. Waiting...")
                    sleep(30)
                else:
                    return None

        df = pd.DataFrame(candles, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'], unit = 's')
        df.set_index('date', inplace = True)
        df = df.sort_index()
        
        # Only plot if the user wants to
        if plotDetails["plot"]:
            if plotDetails["type"].lower() == "mplfinance":
                ohlc = df

                sma = ohlc['close'].rolling(window=20).mean()
                apds = [
                    mpf.make_addplot(sma, color='orange', width=0.8) if 0 < sma.shape[0] else None
                    ]
                apds = list(filter(None, apds))
                
                # Custom style
                mc = mpf.make_marketcolors(
                    up='#26a69a',      # Green candle
                    down='#ef5350',    # Red candle
                    edge='inherit',    # Inherit the same color for edges
                    wick='inherit',    # Inherit the same color for wicks
                    volume='in',       # Volume bars use same colors as candles
                    ohlc='inherit'     # OHLC bars use same colors
                )

                # Create a custom style
                style = mpf.make_mpf_style(
                    marketcolors=mc,
                    gridstyle='',          # Remove grid
                    y_on_right=True,       # Price axis on the right
                    rc={'font.size': 10},  # Base font size
                    base_mpf_style='nightclouds'  # Base on the nightclouds style
                )

                # Plot with more customization
                mpf.plot(ohlc, 
                        type='candle',
                        title= f"{meta['base']['symbol']}/{'USD'}",
                        style=style,
                        volume=False,
                        figsize=(12, 8),      # Larger figure size
                        tight_layout=True,     # Tight layout
                        ylabel='Price',    
                        addplot=apds,    
                        datetime_format='%Y-%m-%d %H:%M',  # Date format
                        scale_padding={'left': 0.5, 'right': 0.5, 'top': 0.8, 'bottom': 0.8})  # Add some padding
            elif plotDetails["type"].lower() == "plotly":
                import plotly.graph_objects as go

                # df["date"] = df.index
                fig = go.Figure(data=[go.Candlestick(x=df.index,
                                open=df['open'],
                                high=df['high'],
                                low=df['low'],
                                close=df['close'],
                                increasing_line_color='#26A69A',    # green
                                decreasing_line_color='#EF5350',    # red
                                )])

                fig.update_layout(title='Candlestick Chart',
                                yaxis_title='Price',
                                xaxis_title='Date',
                                height=800,                        # height in pixels
                                paper_bgcolor='black',             # background color
                                plot_bgcolor='black',              # plot area color
                                font=dict(color='white'),          # text color
                                xaxis=dict(showgrid=False),        # remove x-axis grid
                                yaxis=dict(showgrid=False))        # remove y-axis grid

                fig.show()
            else:
                # Handle exceptions
                raise Exception(f"Error plotting the chart. Only mplfinance or plotly are acceptable for now. You entered: {plotDetails['type']}")
        
        return df
    except Exception as ex:
        print(f"Error while getting kline data: {str(ex)} | Response text: {response.text}")


######## Utility classes ########
class APIClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        """
        Initialize API client with base URL and optional API key
        
        Args:
            base_url (str): Base URL of the API
            api_key (str, optional): API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Set up default headers
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Add API key if provided
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'
            
        self.session.headers.update(self.headers)

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make GET request to API endpoint
        
        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters
            
        Returns:
            dict: Response data
        """
        return self._make_request('GET', endpoint, params=params)

    def post(self, endpoint: str, data: Dict) -> Dict:
        """
        Make POST request to API endpoint
        
        Args:
            endpoint (str): API endpoint
            data (dict): Data to send
            
        Returns:
            dict: Response data
        """
        return self._make_request('POST', endpoint, json=data)

    def put(self, endpoint: str, data: Dict) -> Dict:
        """
        Make PUT request to API endpoint
        """
        return self._make_request('PUT', endpoint, json=data)

    def delete(self, endpoint: str) -> Dict:
        """
        Make DELETE request to API endpoint
        """
        return self._make_request('DELETE', endpoint)

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """
        Make HTTP request with retry logic and error handling
        """
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/{endpoint.lstrip('/')}"
                response = self.session.request(method, url, **kwargs)
                
                # Raise error for bad status codes
                response.raise_for_status()
                
                return response.json()
                
            except RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    raise Exception(f"Request failed after {max_retries} attempts: {str(e)}")
                    
                # Handle rate limiting
                if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                    # Get retry-after header or use default delay
                    retry_after = int(e.response.headers.get('Retry-After', retry_delay))
                    time.sleep(retry_after)
                else:
                    # Exponential backoff
                    time.sleep(retry_delay * (2 ** attempt))
                    
            except ValueError as e:
                raise Exception(f"Invalid JSON response: {str(e)}")
                
            except Exception as e:
                raise Exception(f"Unexpected error: {str(e)}")

class dbUtils:
    """
    Utility class to handle database tasks (Works with postgreSQL).
    """
    def __init__(self, host, user, password, port=5432):
        """
        Initialize the class with database connection details.
        """
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self._conn = None
        self._cursor = None

    def insert_row(self, db_name, table_name, data, schema="public"):
        """
        Insert a row into the specified table.
        
        Args:
            db_name (str): Database name
            table_name (str): Table name 
            data (dict): Dictionary of column names and values to insert. Column names as keys and values as values
            schema (str): Schema name (default: "public")
        
        Returns:
            bool: True if insert was successful, False otherwise
            int: ID of inserted row (if available), None otherwise
        """
        try:
            self._connect_to_db(db_name)
            
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ["%s"] * len(values)
            
            query = f"""
                INSERT INTO {schema}.{table_name} 
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING id
            """
            
            self._cursor.execute(query, values)
            inserted_id = self._cursor.fetchone()[0]
            self._conn.commit()
            
            return True, inserted_id
            
        except Error as e:
            print(f"Error inserting row: {e}")
            return False, None

    def update_row(self, db_name, table_name, data, where_clause, where_values=None, schema="public"):
        """
        Update rows in the specified table that match the where clause.
        
        Args:
            db_name (str): Database name
            table_name (str): Table name
            data (dict): Dictionary of column names and values to update
            where_clause (str): WHERE condition (e.g., "id = %s" or "email = %s")
            where_values (tuple/list): Values for the where clause placeholders
            schema (str): Schema name (default: "public")
        
        Returns:
            bool: True if update was successful, False otherwise
            int: Number of rows updated
            
        Example:
            success, rows = checker.update_row( "my_database", "users", data, "last_login < %s AND status = %s", (last_30_days, "active")  # Update all active users who haven't logged in for 30 days)
            success, rows = checker.update_row( "my_database", "users", data,"id = %s", 1  # Update row with id = 1)
        """
        try:
            self._connect_to_db(db_name)
            
            # Prepare SET clause
            set_items = [f"{k} = %s" for k in data.keys()]
            set_values = list(data.values())
            
            # Add where values if they exist
            where_values = where_values if where_values else []
            if not isinstance(where_values, (list, tuple)):
                where_values = [where_values]
                
            # Combine all values
            all_values = set_values + list(where_values)
            
            query = f"""
                UPDATE {schema}.{table_name} 
                SET {', '.join(set_items)}
                WHERE {where_clause}
            """
            
            self._cursor.execute(query, all_values)
            rows_updated = self._cursor.rowcount
            self._conn.commit()
            
            return True, rows_updated
            
        except Error as e:
            print(f"Error updating row: {e}")
            return False, 0

    def get_rows(self, db_name, table_name, columns="*", where_clause=None, where_values=None, order_by=None, limit=None, offset=None, schema="public", dataframe = False):
        """
        Get rows from the specified table.
        
        Args:
            db_name (str): Database name
            table_name (str): Table name
            columns (str/list): Columns to select. Can be "*" or list of column names
            where_clause (str): Optional WHERE condition (e.g., "id = %s")
            where_values (tuple/list): Values for the where clause placeholders
            order_by (str): Optional ORDER BY clause (e.g., "id DESC")
            limit (int): Optional LIMIT clause
            offset (int): Optional OFFSET clause
            schema (str): Schema name (default: "public")
            dataframe (bool): Return results as a pandas dataframe (default: False)
        
        Returns:
            bool: True if query was successful, False otherwise
            list: List of dictionaries containing the rows, or empty list if no results
            
        Example:
            success, rows = checker.get_rows( "my_database", "users", order_by="created_at DESC", limit=10)
            success, rows = checker.get_rows("my_database", "users", where_clause="age > %s AND status = %s", where_values=[25, "active"]
            success, rows = checker.get_rows( "my_database", "users", order_by="id ASC", limit=10, offset=20  # Skip first 20 rows

        """
        try:
            self._connect_to_db(db_name)
            
            # Handle columns
            if isinstance(columns, list):
                columns = ", ".join(columns)
                
            # Build query
            query = f"SELECT {columns} FROM {schema}.{table_name}"
            
            # Add where clause if provided
            values = []
            if where_clause:
                query += f" WHERE {where_clause}"
                if where_values:
                    if not isinstance(where_values, (list, tuple)):
                        where_values = [where_values]
                    values.extend(where_values)
            
            # Add order by if provided
            if order_by:
                query += f" ORDER BY {order_by}"
                
            # Add limit if provided
            if limit is not None:
                query += f" LIMIT {limit}"
                
            # Add offset if provided
            if offset is not None:
                query += f" OFFSET {offset}"
                
            self._cursor.execute(query, values)
            
            # Get column names
            col_names = [desc[0] for desc in self._cursor.description]
            
            # Fetch all rows and convert to list of dictionaries
            rows = []
            for row in self._cursor.fetchall():
                rows.append(dict(zip(col_names, row)))
                
            if dataframe:
                return True, pd.DataFrame(rows, index=[rows[i]["id"] for i in range(0, len(rows))]).set_index("id", drop=True).sort_index()
            
            return True, rows
            
        except Error as e:
            print(f"Error fetching rows: {e}")
            return False, []

    def delete_row(self, db_name, table_name, where_clause, where_values=None, schema="public"):
        """
        Delete rows from the specified table that match the where clause.
        
        Args:
            db_name (str): Database name
            table_name (str): Table name
            where_clause (str): WHERE condition (e.g., "id = %s")
            where_values (tuple/list/single value): Values for the where clause
            schema (str): Schema name (default: "public")
        
        Returns:
            bool: True if deletion was successful, False otherwise
            int: Number of rows deleted
            
        Example:
            success, count = checker.delete_row("my_database", "users", "id = %s", 1  # Delete user with id = 1)
            success, count = checker.delete_row("my_database","users","last_login < %s AND status = %s",['2023-01-01', 'inactive']  # Delete inactive users who haven't logged in since 2023)
            success, count = checker.delete_row("my_database","users","email = %s","old@example.com")
        """
        try:
            self._connect_to_db(db_name)
            
            # Handle where values
            if where_values is not None and not isinstance(where_values, (list, tuple)):
                where_values = [where_values]
                
            query = f"DELETE FROM {schema}.{table_name} WHERE {where_clause}"
            
            self._cursor.execute(query, where_values)
            rows_deleted = self._cursor.rowcount
            self._conn.commit()
            
            return True, rows_deleted
            
        except Error as e:
            print(f"Error deleting row: {e}")
            return False, 0

    def database_exists(self, db_name):
        """
        Check if a table exists in the database
        
        Args:
            db_name (str): Name of the database
        
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            self._connect_to_postgres()
            self._cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            return self._cursor.fetchone() is not None
        except Error as e:
            print(f"Error checking database: {e}")
            return False

    def table_exists(self, db_name, table_name, schema="public"):
        """
        Check if a table exists in the database
        
        Args:
            db_name (str): Name of the database
            table_name (str): Name of the table to check
            schema (str, optional): Name of the schema
        
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            self._connect_to_db(db_name)
            self._cursor.execute("""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """, (schema, table_name))
            return self._cursor.fetchone() is not None
        except Error as e:
            print(f"Error checking table: {e}")
            return False

    def _connect_to_postgres(self):
        """
        Connect to postgres database if not already connected.
        """
        if not self._conn:
            self._conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                database="postgres"
            )
            self._conn.autocommit = True
            self._cursor = self._conn.cursor()

    def _connect_to_db(self, db_name):
        """
        Connect to specific database.
        
        Args:
            db_name (str): Name of the database to connect
            
        Returns:
            None
        """
        if self._conn:
            if self._conn.info.dbname != db_name:
                self._close()
        
        if not self._conn:
            self._conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                database=db_name
            )
            self._cursor = self._conn.cursor()

    def _close(self):
        """
        Close current connection if exists
        """
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._cursor = None
        self._conn = None

    def __del__(self):
        """
        Cleanup connections when object is destroyed
        """
        self._close()

class ConfigManager:
    def __init__(self, configFilePath:str, default_config={"appName":"screener", "latest_block_checked": -1}):
        """
        This class is used to load the configuration file and the environment variables.
        
        Args:
            configFilePath (str): Path to the config file (e.g. config.json)
            default_config (Dict, optional): Default configuration to use if file doesn't exist
        """
        
        self
        
        # Set object parameters
        self.configPath = configFilePath
        self.envPath = ".env"
        self.default_config = default_config 
        
        # Load the environment variables
        load_dotenv(self.envPath)
        self.alchemy_url = os.getenv('alchemy_URL')
        self.api_key = os.getenv('alchemy_API_key')
        self.database_name = os.getenv('database_name')
        self.APP_details = {'alchemy_url': self.alchemy_url, 'alchemy_key': self.api_key, "database_name" : self.database_name}
        
        if self.api_key is None or self.alchemy_url is None:
            raise ValueError(f"Environment variables not found. Are you sure there is a '.env' file in the app's working directory? Check 'env.example' file for correct configuration example.")
        
        
    def load_config(self):
        """
        Load configuration from file. Create default if doesn't exist.
        
        Returns:
            Dict: Api url and its key
            Dict: Configuration dictionary
        """
        try:
            if os.path.exists(self.configPath):
                with open(self.configPath, 'r') as file:
                    return self.APP_details, json.load(file)
            else:
                # File doesn't exist, create it with default config
                self.save_config(self.default_config)
                return self.APP_details, self.default_config
            
        except Exception as e:
            print(f"Error loading config: {str(e)}. Using default config.")
            return self.APP_details, self.default_config

    def save_config(self, config) -> bool:
        """
        Save configuration to file.
        
        Args:
            config (Dict): Configuration to save
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(self.configPath, 'w') as file:
                json.dump(config, file, indent=4)
            return True
        except Exception as e:
            print(f"Error saving config: {str(e)}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key (str): Configuration key to retrieve
            default (Any, optional): Default value if key doesn't exist
            
        Returns:
            Any: Configuration value
        """
        return self.config.get(key, default)

    def set(self, key: str, value: Any) -> bool:
        """
        Set a configuration value and save to file.
        
        Args:
            key (str): Configuration key to set
            value (Any): Value to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.config[key] = value
        return self.save_config(self.config)

    def reset_to_default(self) -> bool:
        """
        Reset configuration to default values.
        
        Returns:
            bool: True if successful, False otherwise
        """
        return self.save_config(self.default_config)

class SOLANA_Handler:
    def __init__(self, api_key: str = None):
        """
        Initialize the Solana block monitor with Alchemy API key.
        
        Args:
            api_key (str, optional): Alchemy API key. If not provided, will look for ALCHEMY_API_KEY env variable.
        """
        self.api_key = api_key or os.getenv('ALCHEMY_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided or set in ALCHEMY_API_KEY environment variable")
            
        self.base_url = f"https://solana-mainnet.g.alchemy.com/v2/{self.api_key}"
        self.headers = {
            "Content-Type": "application/json"
        }

    def _make_request(self, method: str, params: list = None):
        """
        Make a JSON-RPC request to Alchemy API.
        
        Args:
            method (str): The RPC method to call
            params (list, optional): Parameters for the RPC method
            
        Returns:
            Optional[Dict[str, Any]]: The response data or None if request failed
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or []
        }
        
        try:
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed (solana) for method {method}: {e}")
            return None

    def get_latest_block(self):
        """
        Get the latest Solana block height.
        
        Returns:
            Optional[int]: The latest block height or None if request failed
        """
        data = self._make_request("getBlockHeight")
        if data and "result" in data:
            return data["result"]
        return None

    def get_block_info(self, block_number: int, transaction_details: str = "full"):
        """
        Get detailed information about a specific block.
        
        Args:
            block_number (int): The block number to get information for
            transaction_details (str, optional): Level of transaction details to include.
                full: Include all transaction details (Very heavy).
                signatures: Include only transaction signatures (Much lighter).
                none: Exclude transaction details and return block summary.
            
        Returns:
            Optional[Dict[str, Any]]: Block information or None if request failed
        """
        
        # Check parameters
        if transaction_details not in ["full", "signatures", "none"]:
            raise ValueError("Invalid transaction_details. Must be 'full', 'signatures', or 'none'")
        
        data = self._make_request("getBlock", [block_number, {"encoding": "json", "transactionDetails": "signatures", "maxSupportedTransactionVersion": 0}])
        if data and "result" in data:
            return data["result"]
        return None

    def get_block_time(self, block_number: int):
        """
        Get the timestamp of a specific block.
        
        Args:
            block_number (int): The block number to get timestamp for
            
        Returns:
            Optional[int]: Block timestamp or None if request failed
        """
        data = self._make_request("getBlockTime", [block_number])
        if data and "result" in data:
            return data["result"]
        return None

class ETH_Handler:
    def __init__(self, handle:Web3):
        """
        This class is used to handle Ethereum blockchain interactions.
        
        Args:
            handle (Web3): Web3 object to interact with the blockchain. tested with alchemy API.
        """
        self.handle = handle
        self.ERC20_ABI = [ # Basic Token Information for Ethereum
            {"inputs": [], "name": "name", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "totalSupply", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            
            # Balance and allowance
            {"inputs": [{"type": "address"}], "name": "balanceOf", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [{"type": "address"}, {"type": "address"}], "name": "allowance", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            
            # Transfer events
            {"anonymous": False, "inputs": [{"indexed": True, "type": "address"}, {"indexed": True, "type": "address"}, {"indexed": False, "type": "uint256"}], "name": "Transfer", "type": "event"},
            {"anonymous": False, "inputs": [{"indexed": True, "type": "address"}, {"indexed": True, "type": "address"}, {"indexed": False, "type": "uint256"}], "name": "Approval", "type": "event"}
        ]
        
        self.ERC20_ABI_Minimal = [ # Basic Token Information for Ethereum (Minimal)
            {"constant": True,"inputs": [],"name": "symbol","outputs": [{"name": "", "type": "string"}],"type": "function"},
            {"constant": True,"inputs": [],"name": "decimals","outputs": [{"name": "", "type": "uint8"}],"type": "function"}
        ]

        self.UNIV2_ROUTER_ABI = [ # Uniswap V2 Pair ABI (minimal)
            {"constant": True,"inputs": [],"name": "token0","outputs": [{"name": "", "type": "address"}],"type": "function"},
            {"constant": True,"inputs": [],"name": "token1","outputs": [{"name": "", "type": "address"}],"type": "function"},
            {"constant": True,"inputs": [],"name": "getReserves","outputs": [{"name": "_reserve0", "type": "uint112"},{"name": "_reserve1", "type": "uint112"},{"name": "_blockTimestampLast", "type": "uint32"}],"type": "function"}
        ]
        
        with open("./resources/ABIs/UNI_V2_ROUTER02.abi", 'r') as abi_file:
            abi_content = abi_file.read()
            self.UNIV2_ROUTER_ABI = json.loads(abi_content)
        
        with open("./resources/ABIs/UNI_V2_PAIR.abi", 'r') as abi_file:
            abi_content = abi_file.read()
            self.UNIV2_PAIR_ABI = json.loads(abi_content)
            
        with open("./resources/ABIs/UNI_V3_POOL.abi", 'r') as abi_file:
            abi_content = abi_file.read()
            self.UNIV3_POOL_ABI = json.loads(abi_content)
        
        self.FACTORY_ABI = [ # Uniswap V2 Factory ABI (minimal for PairCreated event) 
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "token0", "type": "address"},
                    {"indexed": True, "name": "token1", "type": "address"},
                    {"indexed": False, "name": "pair", "type": "address"},
                    {"indexed": False, "name": "nil", "type": "uint256"}
                ],
                "name": "PairCreated",
                "type": "event"
            }
        ]


        # Contract address in charge of deploying pairs
        self.uniswap_Factory_V2 = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
        self.uniswap_Factory_V3 = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    
    def get_latest_block(self):
        """
        Get the latest block number from the blockchain.
        
        Returns:
            int: Latest block number
        """
        try:
            if self.handle.is_connected():
                # Get the latest block
                return self.handle.eth.get_block('latest')
                
            else:
                raise Exception("Failed to connect to the Ethereum node. Are you sure you have entered the API key correctly?")
        except Exception as e:
            print(f"Error getting latest block: {e}")
            return 0
    
    def get_pair_info(self, pair_address: str) -> Dict:
        """
        Get detailed information about a Uniswap V2 or V3 pairs.
        
        Args:
            web3obj: A web3 object (using web3 package).
            node_url: Ethereum node URL
        
        Returns:
            Dictionary containing pair information
        """
        try:
            # Connect to Ethereum network
            w3 = self.handle

            # Convert pair address to checksum format
            pair_address = Web3.to_checksum_address(pair_address)
            
            try:       
                # Try withUniswap V2 ABI
                pair_contract = w3.eth.contract(address=pair_address, abi=self.UNIV2_ROUTER_ABI)
            
                # Get token addresses
                token0_address = pair_contract.functions.token0().call()
                token1_address = pair_contract.functions.token1().call()
            except:
                try:
                    # Try withUniswap V3 ABI
                    pair_contract = w3.eth.contract(address=pair_address, abi=self.UNIV3_POOL_ABI)
                
                    # Get token addresses
                    token0_address = pair_contract.functions.token0().call()
                    token1_address = pair_contract.functions.token1().call()
                except:
                    raise Exception("Not a uniswap V2 or V3 pair. are you sure what you have entered is for a conteract address?")
            
            # Get pool's version
            version = None
            v2_contract = w3.eth.contract(address=pair_address, abi=self.UNIV2_PAIR_ABI)
            # Try V2
            try:
                v2_contract.functions.getReserves().call()
                version = "UNI_V2"
            except:
                pass  # Not a V2 pair, continue to check V3

            # Try V3
            v3_contract = w3.eth.contract(address=pair_address, abi=self.UNIV3_POOL_ABI)
            try:
                v3_contract.functions.slot0().call()
                version = "UNI_V3"
            except:
                pass  # Not a V3 pair either
            
            if version == None: version = "None"
            
            # Initialize token contracts
            token0_contract = w3.eth.contract(address=token0_address, abi=self.ERC20_ABI)
            token1_contract = w3.eth.contract(address=token1_address, abi=self.ERC20_ABI)
            
            # Get token information
            token0_symbol = token0_contract.functions.symbol().call()
            token1_symbol = token1_contract.functions.symbol().call()
            token0_decimals = token0_contract.functions.decimals().call()
            token1_decimals = token1_contract.functions.decimals().call()
            
            return {
                "pair_address": pair_address,
                "version": version,
                "token0": {
                    "address": token0_address,
                    "symbol": token0_symbol,
                    "decimals": token0_decimals
                },
                "token1": {
                    "address": token1_address,
                    "symbol": token1_symbol,
                    "decimals": token1_decimals
                }
            }
            
        except Exception as e:
            raise Exception(f"Error fetching pair information: {str(e)}")

    def get_token_price_from_pair(self, token_address: str,pair_address: str) -> Dict[str, Any]:
        """
        Get token price directly from a Uniswap V2 pair on Ethereum.
        
        Args:
            token_address: Address of the token to get price for
            pair_address: Address of the Uniswap V2 pair
            node_url: Ethereum node URL
        
        Returns:
            Dictionary containing price information
        """
        try:
            # Connect to Ethereum network
            w3 = self.handle
            if not w3.is_connected():
                raise Exception("Failed to connect to Ethereum network")

            # Convert addresses to checksum format
            token_address = Web3.to_checksum_address(token_address)
            pair_address = Web3.to_checksum_address(pair_address)
            
            # Initialize pair contract
            pair_contract = w3.eth.contract(address=pair_address, abi=self.UNIV2_ROUTER_ABI)
            
            # Get tokens in pair
            token0_address = pair_contract.functions.token0().call()
            token1_address = pair_contract.functions.token1().call()
            
            # Get reserves
            reserves = pair_contract.functions.getReserves().call()
            reserve0, reserve1 = reserves[0], reserves[1]
            
            # Initialize token contracts
            token0_contract = w3.eth.contract(address=token0_address, abi=self.ERC20_ABI)
            token1_contract = w3.eth.contract(address=token1_address, abi=self.ERC20_ABI)
            
            # Get decimals for both tokens
            decimals0 = token0_contract.functions.decimals().call()
            decimals1 = token1_contract.functions.decimals().call()
            
            # Get token symbols
            symbol0 = token0_contract.functions.symbol().call()
            symbol1 = token1_contract.functions.symbol().call()
            
            # Calculate price based on which token is which in the pair
            if token0_address.lower() == token_address.lower():
                # If our token is token0, price = reserve1/reserve0
                price = (reserve1 / 10**decimals1) / (reserve0 / 10**decimals0)
                base_token = token1_address
            else:
                # If our token is token1, price = reserve0/reserve1
                price = (reserve0 / 10**decimals0) / (reserve1 / 10**decimals1)
                base_token = token0_address
                
            return {
                "price": price,
                "base_token": base_token,
                "symbols": {
                    "symbol0": symbol0,
                    "symbol1": symbol1,
                },
                "reserves": {
                    "reserve0": reserve0,
                    "reserve1": reserve1
                },
                "decimals": {
                    "token0": decimals0,
                    "token1": decimals1
                },
                "formatted_price": f"{price:.8f}"
            }
            
        except Exception as e:
            raise Exception(f"Error fetching on-chain price: {str(e)}")

    def get_historical_prices(self, pair_address: str,token_address: str,from_block: int,to_block: int) -> List[Dict[str, Any]]:
        """
        Get historical token prices from Uniswap V2 swap events.
        
        Args:
            pair_address: Address of the Uniswap V2 pair
            token_address: Address of the token to get prices for
            from_block: Starting block number
            to_block: Ending block number
            node_url: Ethereum node URL
        
        Returns:
            List of dictionaries containing historical price data
        """
        try:
            # Connect to Ethereum network
            w3 = self.handle
            if not w3.is_connected():
                raise Exception("Failed to connect to Ethereum network")

            # Convert addresses to checksum format
            pair_address = Web3.to_checksum_address(pair_address)
            token_address = Web3.to_checksum_address(token_address)
            
            # Initialize pair contract
            pair_contract = w3.eth.contract(address=pair_address, abi=self.UNIV2_ROUTER_ABI)
            
            # Get token addresses in pair
            token0_address = pair_contract.functions.token0().call()
            token1_address = pair_contract.functions.token1().call()
            
            # Initialize token contracts
            token0_contract = w3.eth.contract(address=token0_address, abi=self.ERC20_ABI)
            token1_contract = w3.eth.contract(address=token1_address, abi=self.ERC20_ABI)
            
            # Get decimals and symbols
            decimals0 = token0_contract.functions.decimals().call()
            decimals1 = token1_contract.functions.decimals().call()
            symbol0 = token0_contract.functions.symbol().call()
            symbol1 = token1_contract.functions.symbol().call()
            
            # Determine which token is which in the pair
            is_token0 = token_address.lower() == token0_address.lower()
            base_decimals = decimals1 if is_token0 else decimals0
            token_decimals = decimals0 if is_token0 else decimals1
            
            # Get swap events
            swap_filter = pair_contract.events.Swap.create_filter(
                fromBlock=from_block,
                toBlock=to_block
            )
            events = swap_filter.get_all_entries()
            
            # Process events
            prices = []
            for event in events:
                # Get event data
                amount0In = event['args']['amount0In']
                amount1In = event['args']['amount1In']
                amount0Out = event['args']['amount0Out']
                amount1Out = event['args']['amount1Out']
                
                # Calculate amounts considering decimals
                if is_token0:
                    token_amount = (amount0In - amount0Out) / (10 ** token_decimals)
                    base_amount = (amount1Out - amount1In) / (10 ** base_decimals)
                else:
                    token_amount = (amount1In - amount1Out) / (10 ** token_decimals)
                    base_amount = (amount0Out - amount0In) / (10 ** base_decimals)
                
                # Calculate price only if there was actual movement
                if token_amount != 0 and base_amount != 0:
                    price = abs(base_amount / token_amount)
                    
                    # Get block timestamp
                    block = w3.eth.get_block(event['blockNumber'])
                    timestamp = datetime.fromtimestamp(block['timestamp'])
                    
                    prices.append({
                        'block_number': event['blockNumber'],
                        'timestamp': timestamp.isoformat(),
                        'price': price,
                        'token_amount': abs(token_amount),
                        'base_amount': abs(base_amount),
                        'transaction_hash': event['transactionHash'].hex()
                    })
            
            return prices
            
        except Exception as e:
            raise Exception(f"Error fetching historical prices: {str(e)}")
    
    def get_token_details(self, token_address):
        """
        Get token details from the blockchain.
        
        Args:
            token_address (str): Address of the token contract
        
        Returns:
            Dict: Token information (name, symbol, decimals, total_supply)
        """
        # Connect to Ethereum node
        web3 = self.handle
        
        # Convert to checksum address
        token_address = web3.to_checksum_address(token_address)
        
        # Validate address
        if not web3.is_address(token_address):
            raise Exception("Invalid address")
        
        if  len(web3.eth.get_code(token_address)) != 0:
            try:
                # Create contract instance
                contract = web3.eth.contract(address=token_address, abi = self.ERC20_ABI)
                
                # Get basic token information
                token_info = {
                    'address': token_address,
                    'name': contract.functions.name().call(),
                    'symbol': contract.functions.symbol().call(),
                    'decimals': contract.functions.decimals().call(),
                    'total_supply': contract.functions.totalSupply().call()
                }
                
                # Get contract code size (to verify it's a contract)
                token_info['is_contract'] = "True"
                
                return token_info
            except:
                token_info = {
                    'address': token_address,
                    'name': "-",
                    'symbol': "-",
                    'decimals': "-",
                    'total_supply': "-"
                }
                token_info['is_contract'] = "True"
                return token_info
        else:
            # Not a conteract
            return None

class SOL_Handler:
    def __init__(self, alchemyApiKey:str):
        """
        This class is used to handle Ethereum blockchain interactions.
        
        Args:
            alchemyApiKey (str): The alchemy api key
        """
        self.apiKey = alchemyApiKey
        self.baseURL = f"https://solana-mainnet.g.alchemy.com/v2/{alchemyApiKey}"

    def getSwapDetails_v1(self, sig:str, verbose = False):
        """
        Gets the transaction's details and returns it as a dict. Uses alchemy's api.
        
        Args:
            sig (str): The transaction's signature
            verbose (bool): Weather to print the tx details. Default is False.
        
        Returns:
            A dict with following keys: 
            [timestamp,slot,fee,success,token_changes(Dict),program_ids,accounts_involved(List)]
        """
        base_url = f"https://solana-mainnet.g.alchemy.com/v2/{self.apiKey}"
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "getTransaction",
            "params": [
                sig,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }
        
        try:
            response = requests.post(base_url, headers=headers, json=payload)
            response.raise_for_status()
            tx_data = response.json()
            
            if not tx_data.get("result"):
                print(f"Transaction not found: {sig}")
                return None
                
            # Extract relevant transaction data
            tx = tx_data["result"]
            meta = tx["meta"]
            
            # Get token balance changes with decimals
            pre_balances = {}
            post_balances = {}
            
            
            
            
            
            # Get user's main account
            user_account = tx["transaction"]["message"]["accountKeys"][0]
            account_indexes = {acc["pubkey"]: idx for idx, acc in enumerate(tx["transaction"]["message"]["accountKeys"])}

            # Track native SOL changes
            user_index = account_indexes.get(user_account["pubkey"])
            pre_sol = meta["preBalances"][user_index] / 1e9
            post_sol = meta["postBalances"][user_index] / 1e9
            sol_change = post_sol - pre_sol

            # Adjust for transaction fee
            if sol_change < 0:
                sol_change += meta["fee"] / 1e9

            # Track token changes
            swap_tokens = {
                "in": None,
                "out": None
            }

            # First check if native SOL was involved
            if abs(sol_change) > 0.000001:  # Filter out dust
                if sol_change > 0:
                    swap_tokens["in"] = {
                        "mint": "Native SOL",
                        "amount": abs(sol_change),
                        "decimals": 9
                    }
                else:
                    swap_tokens["out"] = {
                        "mint": "Native SOL",
                        "amount": abs(sol_change),
                        "decimals": 9
                    }
            
            
            
            
            
            
            
            
            
            for balance in meta["preTokenBalances"]:
                pre_balances[balance["mint"]] = {
                    "amount": float(balance["uiTokenAmount"]["uiAmount"] or 0),
                    "decimals": balance["uiTokenAmount"]["decimals"]
                }
                
            for balance in meta["postTokenBalances"]:
                post_balances[balance["mint"]] = {
                    "amount": float(balance["uiTokenAmount"]["uiAmount"] or 0),
                    "decimals": balance["uiTokenAmount"]["decimals"]
                }
            
            # Calculate token transfers
            token_changes = {}
            all_mints = set(pre_balances.keys()) | set(post_balances.keys())
            
            for mint in all_mints:
                pre_amount = pre_balances.get(mint, {"amount": 0})["amount"]
                post_amount = post_balances.get(mint, {"amount": 0})["amount"]
                decimals = pre_balances.get(mint, post_balances.get(mint))["decimals"]
                
                change = post_amount - pre_amount
                if change != 0:
                    token_changes[mint] = {
                        "amount": change,
                        "decimals": decimals
                    }
            
            # Get program IDs involved
            program_ids = set(ix.get("programId") for ix in 
                            tx["transaction"]["message"]["instructions"])
            
            # Compile swap details
            swap_details = {
                "timestamp": tx["blockTime"],
                "slot": tx["slot"],
                "fee": meta["fee"] / 1e9,  # Convert fee from lamports to SOL
                "success": not meta.get("err"),
                "token_changes": token_changes,
                "program_ids": list(program_ids),
                "accounts_involved": tx["transaction"]["message"]["accountKeys"]
            }
            
            if verbose:
                    print("\nSwap Transaction Details:")
                    print(f"Timestamp: {datetime.fromtimestamp(swap_details['timestamp'])}")
                    print(f"Slot: {swap_details['slot']}")
                    print(f"Fee: {swap_details['fee']:.9f} SOL")
                    print(f"Success: {'Yes' if swap_details['success'] else 'No'}")
                    
                    print("\nToken Changes:")
                    for mint, change in swap_details['token_changes'].items():
                        direction = "IN" if change['amount'] > 0 else "OUT"
                        print(f"  {direction}: {abs(change['amount'])} tokens "
                            f"(Mint: {mint}, Decimals: {change['decimals']})")
                    
                    print("\nPrograms Involved:")
                    for program_id in swap_details['program_ids']:
                        print(f"  {program_id}")
            
            return swap_details
            
        except Exception as e:
            print(f"Error fetching transaction: {str(e)}")
            return None

    def getSwapDetails_v2(self, sig:str, verbose = False):
        """
        Gets the transaction's details and returns it as a dict. Uses alchemy's api.
        Accounts for native tokens as well.
        
        Args:
            sig (str): The transaction's signature
            verbose (bool): Weather to print the tx details. Default is False.
        
        Returns:
            A dict with following keys: 
            [timestamp,slot,fee,success,token_changes(Dict),program_ids,accounts_involved(List)]
        """
        
        # Configuration
        # API request setup
        # try:
        url = f"https://solana-mainnet.g.alchemy.com/v2/{self.apiKey}"
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "getTransaction",
            "params": [
                sig,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }

        # Make request
        response = requests.post(url, headers=headers, json=payload)
            
        if not response.json().get("result"):
            print(f"Transaction not found: {sig}")
            print(response.text)
            return None
            
        tx_data = response.json()["result"]
        meta = tx_data["meta"]

        # Get user's main account
        user_account = tx_data["transaction"]["message"]["accountKeys"][0]
        account_indexes = {acc["pubkey"]: idx for idx, acc in enumerate(tx_data["transaction"]["message"]["accountKeys"])}

        # Track native SOL changes
        user_index = account_indexes.get(user_account["pubkey"])
        pre_sol = meta["preBalances"][user_index] / 1e9
        post_sol = meta["postBalances"][user_index] / 1e9
        sol_change = post_sol - pre_sol

        # Adjust for transaction fee
        if sol_change < 0:
            sol_change += meta["fee"] / 1e9

        # Track token changes
        swap_tokens = {
            "in": None,
            "out": None
        }

        # First check if native SOL was involved
        if abs(sol_change) > 0.000001:  # Filter out dust
            if sol_change > 0:
                swap_tokens["in"] = {
                    "mint": "Native SOL",
                    "amount": abs(sol_change),
                    "decimals": 9
                }
            else:
                swap_tokens["out"] = {
                    "mint": "Native SOL",
                    "amount": abs(sol_change),
                    "decimals": 9
                }

        # Get token changes
        all_token_balances = []

        # Collect all pre-balances with account ownership
        for balance in meta["preTokenBalances"]:
            all_token_balances.append({
                "mint": balance["mint"],
                "owner": balance.get("owner"),
                "time": "pre",
                "account_index": balance["accountIndex"],
                "amount": float(balance["uiTokenAmount"]["uiAmount"] or 0),
                "decimals": balance["uiTokenAmount"]["decimals"]
            })

        # Collect all post-balances with account ownership
        for balance in meta["postTokenBalances"]:
            all_token_balances.append({
                "mint": balance["mint"],
                "owner": balance.get("owner"),
                "time": "post",
                "account_index": balance["accountIndex"],
                "amount": float(balance["uiTokenAmount"]["uiAmount"] or 0),
                "decimals": balance["uiTokenAmount"]["decimals"]
            })

        # Calculate changes for each mint
        mint_changes = {}
        for balance in all_token_balances:
            mint = balance["mint"]
            if mint not in mint_changes:
                mint_changes[mint] = {
                    "pre": {},
                    "post": {},
                    "decimals": balance["decimals"]
                }
            
            time = balance["time"]
            owner = balance["owner"]
            if owner:
                mint_changes[mint][time][owner] = mint_changes[mint][time].get(owner, 0) + balance["amount"]

        # Analyze changes to find swap tokens
        for mint, data in mint_changes.items():
            pre_amount = data["pre"].get(user_account["pubkey"], 0)
            post_amount = data["post"].get(user_account["pubkey"], 0)
            change = post_amount - pre_amount
            
            if abs(change) > 0.000001:  # Filter out dust
                if change > 0 and not swap_tokens["in"]:
                    swap_tokens["in"] = {
                        "mint": mint,
                        "amount": abs(change),
                        "decimals": data["decimals"]
                    }
                elif change < 0 and not swap_tokens["out"]:
                    swap_tokens["out"] = {
                        "mint": mint,
                        "amount": abs(change),
                        "decimals": data["decimals"]
                    }

        # Add block timestamp
        swap_tokens["timestamp"] = tx_data['blockTime']
        
        if verbose:
            # Print results
            print(f"Timestamp: {datetime.fromtimestamp(tx_data['blockTime'])} ")
            print(f"Transaction Fee: {meta['fee'] / 1e9} SOL")
            print("\nSwap Details:")

            if swap_tokens["out"]:
                print("\nSwapped Out:")
                print(f"Token: {swap_tokens['out']['mint']}")
                print(f"Amount: {swap_tokens['out']['amount']}")
                print(f"Decimals: {swap_tokens['out']['decimals']}")

            if swap_tokens["in"]:
                print("\nReceived:")
                print(f"Token: {swap_tokens['in']['mint']}")
                print(f"Amount: {swap_tokens['in']['amount']}")
                print(f"Decimals: {swap_tokens['in']['decimals']}")
        
        return swap_tokens
        
        # except Exception as e:
        #     print(f"Error fetching transactions: {str(e)}")
        #     return None

    def getPoolTrades(self, poolAddress, limit=1000, searchStart = None, searchEnd = None, verbose = False):
        """
        Fetches recent trades for a pool. Uses alchemy's API.
        
        Args:
            poolAddress (str): The pool's address.
            limit (int): Maximum number of transactions to get in each batch.
            searchStart (str): The signature to start getting trades from. The
                search will continue back in time from this point on. Set None
                to start from the most recent trade.
            searchEnd (dict): When to end the search. It is a dictionary. Multiple 
                key value pairs are acceptable. Acceptable keys are as follows:
                "signature", "timestamp" (in seconds) and "slot".
            verbose (bool): True if you want the app to explain what its doing.
            
        Returns:
            A dist of trades. Each trade is a dictionary with keys blockTime, 
            slot and signature. 
        """
        _searchEndIsSet = False
        _searchPropertyKey = [] # What to search for stopping the loop (If any)
        # Process the arguments
        if searchEnd != None:
            if 3 < len(searchEnd):
                raise Exception("Maximum of 3 key-value pair is acceptable for searchEnd argument")
            
            for key in searchEnd.keys():
                if key not in ["signature", "timestamp", "slot"]:
                    raise Exception("The searchEnd parameter key should be one of the following: signature, timestamp or slot")
                else:
                    _searchEndIsSet = True
                    _searchPropertyKey.append(key)
        
        if 1000 < limit:
            raise Exception("Limit can not be more than 1000.")
        
        
        # Loop parameters
        trades = [] # Save the trades
        _stop = False # True when we want the loop to break
        errCount = 0 # Number of errors in requesting a batch
        nextBatchStart = searchStart # The signature to start the transaction search
        
        # Loop will break when:
        # 1. errCount is bigger than 10
        # 2. searchEnd parameter is found in batch
        # 3. If searchEnd is None, only one batch will be returned
        if verbose: print(f"Starting to fetch trades for pool {poolAddress}")
        while not _stop:
            if verbose: print("getting a new batch ...")
            
            batch = []
            errCount = 0
            
            try:
                # Get recent signatures for the pool
                headers = {
                    "accept": "application/json",
                    "content-type": "application/json"
                }
                
                payload = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "getSignaturesForAddress",
                    "params": [
                        poolAddress,
                        {
                            "limit": limit,
                            "before": nextBatchStart,
                            "until": None
                        }
                    ]
                }
                
                response = requests.post(
                    self.baseURL,
                    headers=headers,
                    json=payload
                )
                
                # Raise errors if no responses are returned
                if response.status_code != 200 or not response.json()["result"]:
                    raise Exception(f"API request failed: {response.status_code} - {response.text}")
                
                signatures = response.json()
                
                for sigInfo in signatures["result"]:
                    # Only save transactions without any errors
                    if sigInfo["err"] == None:
                        batch.append({
                            "blockTime": sigInfo["blockTime"],
                            "slot": sigInfo["slot"],
                            "signature": sigInfo["signature"]
                        })
                        
                        # Check stop statues
                        if "timestamp" in _searchPropertyKey:
                            if int(sigInfo["blockTime"]) < searchEnd["timestamp"]:
                                if verbose: print(f"Reached timestamp {searchEnd['timestamp']}. Breaking...")
                                _stop = True
                                break
                        
                        if "slot" in _searchPropertyKey:
                            if int(sigInfo["slot"]) < searchEnd["slot"]:
                                if verbose: print(f"Reached slot {searchEnd['slot']}. Breaking...")
                                _stop = True
                                break
                        
                        if "signature" in _searchPropertyKey:
                            if sigInfo["signature"] == searchEnd["signature"]:
                                if verbose: print(f"Reached signature {searchEnd['signature']}. Breaking...")
                                _stop = True
                                break
                        
                        # If not stopping, update the next batch's starting point
                        nextBatchStart = sigInfo["signature"]
                
                # If no end is specified, only get 1 batch
                if searchEnd == None:
                    _stop = True
                
                # Add this batch's trades to main list
                trades += batch
                
            except Exception as e:
                print(f"Error fetching trades: {str(e)}. Retrying for the {errCount} time ...")
                sleep(5)
                
                if errCount == 10:
                    print("Retried 10 times. Exiting.")
                    return trades
                
                errCount += 1

        return trades

class EVMTokenCandlestick:
    
    def __init__(self, path:str, web3:Web3, targetChain: str, verbose:bool = False):
        """
        The class for getting EVM chains pool trades and plotting their candlestick 
        charts. Coded for uniswap pools. Should add ABIs for other pool integrations. 
        
        Args:
            path: The path to search for *.csv files. Each file name should have the 
                following pattern: {startBlockNumber}-{endBlockNumber}.csv
                Each row should have 3 columns. Namely, block, timestamp and datetime.
            web3 (Web3): A web3 object to interact with blockchain.
            targetChain (str): The name of the chain that the pool is on
            verbose (bool): Weather the app should describe what its doing ot user.
        """
        
        # Get available block-timestamp pairs
        # Get all files and sort them
        # List to store the results
        availableBlocks = []
        
        # Get all files in the directory and add it to a dataframe
        try:
            files = os.listdir(path)
            
            # Process each file
            for filename in files:
                # Check if it's a CSV file
                if filename.endswith('.csv'):
                    # Use regex to find numbers separated by dash
                    matches = re.findall(r'(\d+)-(\d+)', filename)
                    
                    # If we found a match, convert strings to integers and add to our list
                    if matches:
                        # Take the first match (in case there are multiple)
                        x, y = matches[0]
                        availableBlocks.append({"start":int(x), "finish":int(y)})
        
        except FileNotFoundError:
            raise Exception(f"Directory not found: {path}")
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")
        
        if targetChain not in ["ETH"]:
            raise Exception("Only ETH chain is supported at this time")
        
        # Set params
        self.availableBlocks = pd.DataFrame(availableBlocks).sort_values("start")      
        
        with open("./resources/ABIs/UNI_V2_SWAP_EVENT.abi", 'r') as abi_file:
            abi_content = abi_file.read()
            self.UNI_V2_SWAP_EVENT_ABI = json.loads(abi_content)
        with open("./resources/ABIs/UNI_V3_SWAP_EVENT.abi", 'r') as abi_file:
            abi_content = abi_file.read()
            self.UNI_V3_SWAP_EVENT_ABI = json.loads(abi_content)
        
        self.web3 = web3
        self.verbose = verbose
        self.targetChain = targetChain

    def get_plot(self, df:pd.DataFrame, tf:str, info = None):
        """
        Plots the asset using matplotlib finance
        
        Args:
            df (pd.Dataframe): A dataframe containing price and time of the trade. Time
            of the trade should be in datetime and should be as the index of the 
            dataframe
            tf (str): The timeframe to aggregate the data for. (e.g. 1min, 1h, 1d, etc.) 
            info (dict): Info about the chart. Acceptable keys: {title, } 
        """
        
        ohlc = df["price"].resample(tf)

        ohlc = pd.DataFrame({
            'open': ohlc.first(),
            'high': np.maximum(ohlc.max(),ohlc.first().shift(-1)),
            'low': np.minimum(ohlc.min(),ohlc.first().shift(-1)),
            'close': ohlc.first().shift(-1)  # First price of next period
        }).dropna()

        # Plot an SMA20
        sma = ohlc['close'].rolling(window=20).mean()
        apds = [
            mpf.make_addplot(sma, color='orange', width=0.8) if 0 < sma.shape[0] else None
            ]
        apds = list(filter(None, apds))
        
        # Custom style
        mc = mpf.make_marketcolors(
            up='#26a69a',      # Green candle
            down='#ef5350',    # Red candle
            edge='inherit',    # Inherit the same color for edges
            wick='inherit',    # Inherit the same color for wicks
            volume='in',       # Volume bars use same colors as candles
            ohlc='inherit'     # OHLC bars use same colors
        )

        # Create a custom style
        style = mpf.make_mpf_style(
            marketcolors=mc,
            gridstyle='',          # Remove grid
            y_on_right=True,       # Price axis on the right
            rc={'font.size': 10},  # Base font size
            base_mpf_style='nightclouds'  # Base on the nightclouds style
        )

        # Plot with more customization
        mpf.plot(ohlc, 
                type='candle',
                title= info["title"] if info != None else 'Price Chart',
                style=style,
                volume=False,
                figsize=(12, 8),      # Larger figure size
                tight_layout=True,     # Tight layout
                ylabel='Price',    
                addplot=apds,    
                datetime_format='%Y-%m-%d %H:%M',  # Date format
                scale_padding={'left': 0.5, 'right': 0.5, 'top': 0.8, 'bottom': 0.8})  # Add some padding

    def process_transactions(self, transactions):
        """
        Process the raw transactions to a pandas dataframe. While block numbers are
        returned when downloading the trades from blockchain, these trades only contain 
        block number, and their timestamps are not denoted. To get each trade's 
        timestamp we should have a pair of each block's timestamp (which we take when
        making the object). After iterating through it, we take time stamp of each 
        required block number and add it to a new column. 
        
        Args:
            transactions (list): A list of trades, containing at leas two columns
        
        Returns:
            A dataframe containing price of each trade, and its time (in datetime and 
            as index)
        """
        df = pd.DataFrame(transactions)
        df['timestamp'] = None
        
        # Get timestamp for each trade using its block number
        for idx, row in self.availableBlocks.iterrows():
            __df = pd.read_csv(f"./resources/ETH_block_data/{row[0]}-{row[1]}.csv")
            __df = __df[__df["block"].isin(df.block_number)]
            
            if __df.shape[0] != 0:
                # Update df1's target_column based on matching ids
                df["timestamp"] = df["timestamp"].combine_first(df['block_number'].map(
                    __df.set_index('block')['timestamp']
                ))

        # If there are any missing block timestamps, find them
        __df = df[df["timestamp"].isna()]
        print(f"{__df.shape[0]} block timestamps were not found in the provided files. Downloading them manually. This might take some time...")
        for idx, row in tqdm(__df.iterrows(), total = __df.shape[0]):
            df.loc[idx,"timestamp"] = self.web3.eth.get_block(row["block_number"])["timestamp"]

        # Make timestamp in milliseconds. Cast price to float as well
        df["timestamp"] = (df["timestamp"] * 1000).astype(np.int64)
        df["price"] = df["price"].astype(float)
        
        # Take two columns (price and timestamp). Change timestamp to datetime and set it as index.
        df = df[["price","timestamp"]]
        df["datetime"] = pd.to_datetime(df["timestamp"],unit="ms")
        df = df.set_index("datetime", drop = True)
        
        return df

    def get_uniswap_pair_transactions(
        self, 
        pair_address: str, 
        from_block: int, 
        to_block: int, 
        step: int, 
        token0_decimals: int, 
        token1_decimals: int, 
        pool_version: int, 
        pref_token: int = None,
        min_tx_volume: float = None) -> List[dict]:
        
        """
        Fetches and processes swap events from a DEX pair contract.
        If the RPC node failed to send us the swap info (due to high size), it
        halves the block range (repeats 10 times) until a valid response is 
        received. 

        Args:
            pair_address (str): Address of the pair contract
            from_block (int): Starting block number
            to_block (int): Ending block number 
            step (int): The step size when searching iteratively, starting from "from_block"
                Set "None" to avoid dividing teh block range.
            token0_decimals (int): Decimal points ofr token 0.
            token2_decimals (int): Decimal points ofr token 2.
            pool_version (int): The version of uniswap pool. Required for choosing correct ABI.
                acceptable values: 2 or 3
            pref_token (int): Preferred token to calculate price and volume related to them. 
                Only 0, 1 and None are acceptable. If None passed, we have no preference and
                return the price of selling (Token 0 sold for token 1 or vice versa)
            min_tx_volume (float): Minimum transaction volume. The values below it will be 
                disregarded. Set None for disregarding this filter. The size will be calculated 
                in "pref_token". So for using this filter, you should pass this parameter as well.

        Returns:
            A list containing trades.
        
        Dependencies:
            - Web3.py
            - PAIR_ABI constant
        """
        # Check the requirements
        if pool_version not in [2, 3]:
            raise Exception("Pool version should be an integer. 2 or 3 are acceptable for now.")
        
        if pref_token not in [0, 1, None]:
            raise Exception("Preferred token can only be 0 or 1. Pass None if you have no preference")
        
        if pref_token == None and min_tx_volume != None:
            raise Exception("Minimum token volume will be calculated in pref_token. So you have to pass it as well")
        
        __dummyInt = self.web3.eth.block_number
        if __dummyInt < to_block:
            if self.verbose: print(f"Entered last block ({to_block}) is bigger than the most recent block on {self.targetChain} which is {__dummyInt}. Setting it end block to be {__dummyInt}")
            to_block = __dummyInt
        
        # Initialize web3 object
        w3 = self.web3
        
        # Create contract instance
        pair_address = Web3.to_checksum_address(pair_address)
        pair_contract = w3.eth.contract(
            address=pair_address, 
            abi = self.UNI_V2_SWAP_EVENT_ABI if pool_version == 2 else self.UNI_V3_SWAP_EVENT_ABI if pool_version == 3 else None)
        
        # If step is None, try to get the entire block range in one go
        if step == None:
            step = to_block - from_block
        
        # A list to save swap events
        swap_events = []
        
        # Loop params
        i = from_block
        counter = 0 # For every acceptable response increases by one. Goes back to zero when faced big size error in alchemy.
        __Stop = False # For stopping the loop
        
        # Get all swap events
        while i <= to_block and not __Stop:
            try:
                # Double the step if four previous steps were successful
                if counter == 3:
                    counter = 0
                    step = step * 2
                    if self.verbose: print(f"++ Doubled the step size to {step}")
                
                # Set the start and end of current block batch to download
                __from = i
                __to = i + step if i + step <= to_block else to_block
                
                # Break, if start and end of the block range is the same
                if __from == __to: break
                
                tmp = pair_contract.events.Swap.get_logs(
                    from_block = __from,
                    to_block = __to
                )
                swap_events += tmp
                counter += 1
                
                if self.verbose: print(f"Downloaded {len(tmp)} trades from trades block {__from} to {__to} ({to_block - __from} remaining). ")
                
                __Stop = True if i == to_block else False
                i = i + step if i + step <= to_block else to_block
                
            except Exception as e:
                # Try 10 times, each time half the step size to see what works
                for j in range(1,10):
                    try:
                        counter = 0
                        _step = int(step / (2 ** j))
                        if self.verbose: print(f"-- Halved each step to {_step}")
                        
                        __from = i
                        __to = i + _step if i + _step <= to_block else to_block
                        tmp = pair_contract.events.Swap.get_logs(
                            from_block = __from,
                            to_block = __to
                        )
                        swap_events += tmp
                        
                        if self.verbose: print(f"Downloaded {len(tmp)} trades from trades block {__from} to {__to} ({to_block - __from} remaining).")
                        
                        # If no errors found, update the step and go to previous searching scheme
                        step = _step
                        __Stop = True if i == to_block else False
                        i = i + step if i + step <= to_block else to_block
                        break
                    except Exception as e:
                        _errorText = str(e)
                        __Stop = True
                
                # Exit getting the transactions
                if j == 9:
                    raise Exception(f"Facing errors. Halved the steps until {_step} but still getting the following error:\n\n {_errorText}")
                
            time.sleep(.1)
            
        transactions = []
        
        if self.verbose: print(f"Download finished. Aggregating {len(swap_events)} trades ...")
        
        # Process each swap event
        for event in swap_events:
            if pool_version == 2:
                # Only for uniswap V2 transactions
                amount0_in = Decimal(event['args']['amount0In']) / Decimal(10 ** token0_decimals)
                amount1_in = Decimal(event['args']['amount1In']) / Decimal(10 ** token1_decimals)
                amount0_out = Decimal(event['args']['amount0Out']) / Decimal(10 ** token0_decimals)
                amount1_out = Decimal(event['args']['amount1Out']) / Decimal(10 ** token1_decimals)
                
                # Calculate effective amounts
                amount0_delta = amount0_in - amount0_out
                amount1_delta = amount1_in - amount1_out
                
                # Calculate price and volume
                if pref_token == None:
                    if amount0_delta > 0:  # Selling token0 for token1
                        price = abs(amount1_delta / amount0_delta) if amount0_delta != 0 else 0
                        volume = abs(amount0_delta)  # Assuming token1 is USD or stable
                    else:  # Selling token1 for token0
                        price = abs(amount0_delta / amount1_delta) if amount1_delta != 0 else 0
                        volume = abs(amount1_delta)  # Assuming token1 is USD or stable
                elif pref_token == 0:
                    price = abs(amount0_delta / amount1_delta) if amount1_delta != 0 else 0
                    volume = abs(amount0_delta)
                elif pref_token == 1:
                    price = abs(amount1_delta / amount0_delta) if amount0_delta != 0 else 0
                    volume = abs(amount1_delta)
                
                # Filter minimum transaction values
                if min_tx_volume != None:
                    if volume < min_tx_volume:
                        continue
                
                tx = {
                    'transaction_hash': event['transactionHash'].hex(),
                    'block_number': event['blockNumber'],
                    'sender': event['args']['sender'],
                    'recipient': event['args']['to'],
                    'price': price,
                    'volume': volume
                }
                
                transactions.append(tx)
            elif pool_version == 3:
                # Only for uniswap V3 transactions
                # # For uniswap V3, each transaction also contains the following info, however, we disregard them
                # # price_from_sqrt, amounts0, amount1, tick, liquidity which are calculated below:
                # sqrt_price_x96 = Decimal(event['args']['sqrtPriceX96'])
                # price_from_sqrt = (sqrt_price_x96 / Decimal(2**96)) ** 2
                
                # Get amounts (note: V3 uses signed integers)
                amount0 = Decimal(event['args']['amount0']) / Decimal(10 ** token0_decimals)
                amount1 = Decimal(event['args']['amount1']) / Decimal(10 ** token1_decimals)

                # Get price from sqrtPriceX96

                if pref_token == None:
                    # Determine which token was sold
                    if amount0 > 0:  # Positive means token was received by the pool (sold by user)
                        # Token0 was sold for Token1
                        sold_amount = amount0
                        bought_amount = -amount1  # Negative because it was received by user
                        # token_path = "0->1"
                        price = abs(amount1 / amount0) if amount0 != 0 else 0
                    else:
                        # Token1 was sold for Token0
                        sold_amount = amount1
                        bought_amount = -amount0  # Negative because it was received by user
                        # token_path = "1->0"
                        price = abs(amount0 / amount1) if amount1 != 0 else 0

                    # Calculate volume in terms of sold token
                    volume = float(abs(sold_amount))
                elif pref_token == 0:
                    price = abs(amount0 / amount1) if amount1 != 0 else 0
                    volume = abs(amount0)
                elif pref_token == 1:
                    price = abs(amount1 / amount0) if amount0 != 0 else 0
                    volume = abs(amount1)
                
                if price != 0:
                    tx = {
                        'transaction_hash': event['transactionHash'].hex(),
                        'block_number': event['blockNumber'],
                        'sender': event['args']['sender'],
                        'recipient': event['args']['recipient'],
                        "price": str(price),
                        "volume": volume,
                    }  
                else:
                    continue
                
                transactions.append(tx)                
            
        return transactions
