import os
from dotenv import load_dotenv 
import json
from utils import *
from web3 import Web3
import pandas as pd
from pprint import pprint
from utils import *
from tqdm import tqdm
from datetime import datetime
import time

# Get the config file
configObj = ConfigManager("config.json")
appInfo, configData = configObj.load_config()
nodeUrl = appInfo["alchemy_url"]+appInfo["alchemy_key"]

# Checking the database state
print("Checking database integrity ...")
db = dbUtils(user = "postgres", password = "1234", host  = "localhost", port =  "5432")
# See if database exists
if db.database_exists("screenerDB"):
    if not db.table_exists("screenerDB", "tokens"):
        print("Creating table 'tokens' in the database 'screenerDB'")
        # Cursor
        db._connect_to_db("screenerDB")
        con = db._conn
        cur = con.cursor()
        
        # Make a table for the new tokens
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                id SERIAL PRIMARY KEY,
                address VARCHAR(255),
                name VARCHAR(255),
                symbol VARCHAR(255),
                chain_name VARCHAR(255),
                decimals INT,
                inception_time BIGINT,
                inception_block BIGINT,
                total_supply VARCHAR(255)
            )
        """)
        
        con.commit()
        
        # close the connection
        con.close()
    else:
        pass
else:
    raise Exception(f"Database doesn't exist. First create the database with the name 'screenerDB'")

# Make the ETH blockchain handler
web3 = Web3(Web3.HTTPProvider(nodeUrl))
handler = ETH_Handler(web3)

latest_block = handler.get_latest_block()

print(f"{latest_block['number'] - configData['latest_block_checked']} blocks to check")

nonTokens = pd.DataFrame(columns=['address'])

try:
    # Go through past blocks to find conteract creation events
    for i in tqdm(range(configData['latest_block_checked'], latest_block['number']), total=latest_block['number'] - configData['latest_block_checked']):
        block = web3.eth.get_block(i, True)
        for tx in block.transactions:
            if tx["to"] == None:
                tx_receipt = web3.eth.get_transaction_receipt(tx['hash'])
                contract_address = tx_receipt['contractAddress']
                contract_code = web3.eth.get_code(contract_address)
                if contract_code != '0x':
                    _details = handler.get_token_details(contract_address)
                    if _details["name"] != "-":
                        _data = {
                            'address': _details["address"],
                            'name': _details["name"],
                            'symbol': _details["symbol"],
                            'chain_name': "Ethereum",
                            'decimals': _details["decimals"],
                            'inception_time': datetime.now().timestamp(),
                            'inception_block': i,
                            'total_supply': str(_details["total_supply"])
                        }
                        state, _ = db.insert_row(appInfo["database_name"], "tokens", _data)
                        
                        # Raise an error if couldn't add to the database 
                        if not state:
                            raise Exception(f"Error in inserting token {contract_address} in the database")
                    else:
                        # DELETE
                        nonTokens = pd.concat([nonTokens, pd.DataFrame([contract_address], columns=['address'])])
                        pass
        configData['latest_block_checked'] = i
        configObj.save_config(appInfo, configData)
        nonTokens.to_csv("nonTokens.csv") # DELETE

        time.sleep(0.02)
    
    # Update the latest block checked
    configData['latest_block_checked'] = i
    configObj.save_config(appInfo, configData)
    print("Database updated successfully")
    
    # DELETE
    nonTokens.to_csv("nonTokens.csv")
    
except Exception as e:
    configData['latest_block_checked'] =  i
    configObj.save_config(appInfo, configData)
    print(f"Error in block {i}")
    print(f"Error: {e}")