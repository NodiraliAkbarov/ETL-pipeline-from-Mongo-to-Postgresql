from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#loading env variables
load_dotenv()
uri = os.getenv("uri")

#Creating new connection

def extracting():
    #Creating new connection
    client = MongoClient(uri)


    #Send a ping to confirm succesfull connection
    try : 
        client = MongoClient(uri)
        client.admin.command("ping")
        logging.info("Successfully connected to MongoDB")
    except Exception as e:
        logging.error(f"{e}")


    try : 
        db = client["sample_airbnb"]
        row_count = db["listingsAndReviews"].count_documents({})
        logging.info(f"Received {row_count} documents ")
        data = list(db["listingsAndReviews"].find({}, projection={"_id":0,"name":1,"summary":1,"listing_url":1,"house_rules":1,"bedrooms":1,"last_review":1}))
    except Exception as e:
        logging.error(f"{e}")
    
    client.close()
    logging.info("Connection on MongoDB closed")
    return data



    


    



