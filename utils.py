from typing import Any, Dict
import os
from dotenv import load_dotenv 
import json
import pandas as pd
from psycopg2 import Error
import psycopg2
from web3 import Web3
    
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

class ETH_Handler:
    def __init__(self, handle:Web3):
        """
        This class is used to handle Ethereum blockchain interactions.
        
        Args:
            handle (Web3): Web3 object to interact with the blockchain. tested with alchemy API.
        """
        self.handle = handle
        self.ERC20_ABI = [ # basic Token Information for ethereum
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
            