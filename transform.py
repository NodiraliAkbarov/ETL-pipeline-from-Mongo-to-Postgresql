import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def transforming(data:list):
    df = pd.DataFrame(data=data)

    #Filtering bedrooms more than 3
    df = df[df["bedrooms"]>1]


    #Filtering rows that contains city word
    df = df[df['name'].str.contains("city")]
    

    #Removing nulls in last_review
    df.dropna(subset=['last_review'], inplace=True)

    
    #Changing datatype from datatime to object 
    df['last_review'] = df['last_review'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    logging.info("Successfully transformed data")
   
    return df
