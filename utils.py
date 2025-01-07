from typing import Any, Dict, Optional, List
import os, csv
from dotenv import load_dotenv 
import json
import pandas as pd
import requests
from requests.exceptions import RequestException
from psycopg2 import Error
import psycopg2
from web3 import Web3
import time
from datetime import datetime

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

        self.PAIR_ABI = [ # Uniswap V2 Pair ABI (minimal)
            {"constant": True,"inputs": [],"name": "token0","outputs": [{"name": "", "type": "address"}],"type": "function"},
            {"constant": True,"inputs": [],"name": "token1","outputs": [{"name": "", "type": "address"}],"type": "function"},
            {"constant": True,"inputs": [],"name": "getReserves","outputs": [{"name": "_reserve0", "type": "uint112"},{"name": "_reserve1", "type": "uint112"},{"name": "_blockTimestampLast", "type": "uint32"}],"type": "function"}
        ]
        
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
        Get detailed information about a Uniswap V2 pair's tokens.
        
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
            
            # Initialize pair contract
            pair_contract = w3.eth.contract(address=pair_address, abi=self.PAIR_ABI)
            
            # Get token addresses
            token0_address = pair_contract.functions.token0().call()
            token1_address = pair_contract.functions.token1().call()
            
            # Get reserves
            reserves = pair_contract.functions.getReserves().call()
            
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
                "token0": {
                    "address": token0_address,
                    "symbol": token0_symbol,
                    "decimals": token0_decimals,
                    "reserve": reserves[0] / 10**token0_decimals
                },
                "token1": {
                    "address": token1_address,
                    "symbol": token1_symbol,
                    "decimals": token1_decimals,
                    "reserve": reserves[1] / 10**token1_decimals
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
            pair_contract = w3.eth.contract(address=pair_address, abi=self.PAIR_ABI)
            
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
            pair_contract = w3.eth.contract(address=pair_address, abi=self.PAIR_ABI)
            
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
