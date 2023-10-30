import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#Loading environmental variables
load_dotenv()

def connecting():

    try: 
        connect = psycopg2.connect(
            host = os.environ.get('host'),
            database=os.environ.get('db'),
            port=os.environ.get('port'),
            user=os.environ.get('user'),
            password=os.environ.get('password'))
        

        #Checking connection by checking postgres version
        cur = connect.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()

        if version is not None:
            logging.info("Connected to Postgresql Database")
    
    except Exception as e:
        logging.error(f"{e}")

    return connect

def loading(data:pd.DataFrame):

    
    create_table = "CREATE TABLE IF NOT EXISTS houses \
            (id SERIAL PRIMARY KEY, listing_url TEXT, name TEXT, house_rules TEXT, summary TEXT, last_review TIMESTAMP, bedrooms INT)"
    
    insert_data = "INSERT INTO houses(listing_url, name, house_rules, summary, last_review, bedrooms) values(%s, %s, %s, %s, %s, %s)"
    
    conn = connecting()

    cursor = conn.cursor()
    

    #Creating table
    try: 
        cursor.execute(create_table)
        logging.info("Created table")
    except Exception as e:
        logging.error(f"{e}")
    

    #Inserting data by converting dataframe into list of tuples
    try:
       cursor.executemany(insert_data, data.to_records(index=False)) 
       logging.info("Inserted rows")
    except Exception as e:
        print(data.to_records(index=False)[0])
        logging.error(f"{e}")

    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info("Connection on Postgresql closed")
    
        
    


    