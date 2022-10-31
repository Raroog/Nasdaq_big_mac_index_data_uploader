#!/usr/bin/env python


# pip install pandas

import pandas as pd
import config


def get_big_mac_data(countries_csv, start_date, end_date, api_key):

    """
    Takes bigmac index data from nasdaq API and
    returns pandas dataframe with data from countries listed in countries csv 

    Parameters:

    countries_csv(csv): csv file with list of all countries having bigmac index in database
    start_date(str): start_date (yyyy-mm-dd) of data date range
    end_date(str): end_date (yyyy-mm-dd) of data date range
    api_key(str): key to Nasdaq API
    """

    prefix_url = "https://data.nasdaq.com/api/v3/datasets/ECONOMIST/BIGMAC_"
    df_list = []

    for code, name in zip(countries["code"], countries["name"]):
        code = code[-3:]
        name = name.split("-",1)[1].strip()
        df_country = pd.DataFrame.from_dict({"country" : [name]})
        df_data = pd.read_csv(
            f"{prefix_url}{code}.csv?start_date={start_date}&end_date={end_date}&api_key={api_key}")
        df = pd.concat([df_country, df_data], axis = 1)    
        df_list.append(df)
    final_df = pd.concat(df_list, ignore_index=True)
    
    return final_df


if __name__ == '__main__':
    api_key = config.API_KEY["api_key"]
    key = config.AWS_KEYS["key"]
    secret = config.AWS_KEYS["secret"]
    countries = pd.read_csv(
    f"https://data.nasdaq.com/api/v3/databases/ECONOMIST/metadata?api_key={api_key}",
    compression = "zip")

    start_date = "2021-07-31"
    end_date = "2021-07-31"

    final_df = get_big_mac_data(countries, start_date, end_date, api_key)

    final_df.to_csv("s3://onwelo-bucket-bp/onvelo/big_mac_index_new.csv",
            storage_options={'key': key,
                            'secret': secret})
