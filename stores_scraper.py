import configparser
import logging
import time
import warnings

import googlemaps
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    filename="log_store_scraper.log",
    filemode="a",
    format="%(asctime)s :: %(levelname)s :: %(message)s",
)
log = logging.getLogger("__name__")

config = configparser.ConfigParser()
config.read("ConfigFile.properties")

api_key = config.get("google", "api_key")

ACCOUNT=config.get("snowflake", "ACCOUNT")
host=config.get("snowflake", "host")
USER=config.get("snowflake", "USER")
warehouse=config.get("snowflake", "warehouse")
DATABASE=config.get("snowflake", "DATABASE")
ROLE = config.get("snowflake", "ROLE")
SCHEMA=config.get("snowflake", "SCHEMA")

conn = snowflake.connector.connect(
  user='ALAIN.MORE@RAPPI.COM',
  account='hg51401',
  authenticator='externalbrowser',
  database='FIVETRAN',
  role='GLOBAL_ECOMMERCE_WRITE_ROLE',
  schema='GLOBAL_ECOMMERCE'
)

gmaps = googlemaps.Client(key=api_key)

code_list=['CO']
store_to_scrape='Koaj'


def execute_snowflake_query(query, with_cursor=False):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        res = cursor.fetchall()
        if with_cursor:
            return (res, cursor)
        else:
            return res
    finally:
        cursor.close()


def pandas_df_from_snowflake_query(query):
    result, cursor = execute_snowflake_query(query, with_cursor=True)
    if len(result)==0:
        return 0
    else:
        headers = list(map(lambda t: t[0], cursor.description))
        df = pd.DataFrame(result)
        df.columns = headers
        return df


def get_stores(store, lat, lng):
    search_result = gmaps.places_nearby(
        keyword=store, 
        location=str(lat) + "," + str(lng), 
        radius=5000
    )
    all_store_results = search_result["results"]
    if "next_page_token" in search_result:
        time.sleep(2)
        other_results_1 = gmaps.places(
            page_token=search_result["next_page_token"]
        )
        all_store_results = all_store_results + other_results_1["results"]

        if "next_page_token" in other_results_1:
            time.sleep(2)
            other_results_2 = gmaps.places(
                page_token=other_results_1["next_page_token"]
            )
            all_store_results = all_store_results + other_results_2["results"]
    
    return all_store_results


def get_centroid_coords():
    conn.cursor().execute("USE SCHEMA FIVETRAN.GLOBAL_ECOMMERCE")
    conn.cursor().execute("USE ROLE GLOBAL_ECOMMERCE_WRITE_ROLE")
    conn.cursor().execute("USE WAREHOUSE ECOMMERCE")
    df_mzcentroids_all = pandas_df_from_snowflake_query(
            """   
                select * from GLOBAL_VALUE_PROP_DS.active_microzones
            """
    )
    # print('ALL mzcentroides: ',df_mzcentroids_all.shape[0])
    
    for code in code_list:
        log.info(">>>>>>>>>>>> Code: " + code)
        df_microzones=df_mzcentroids_all[df_mzcentroids_all['CODE']=='CO']
        log.info(df_microzones.shape[0])
        log.info(df_microzones.head())
        for index, row in df_microzones.iterrows(): 
            mz_id = row["MICROZONE_ID"]
            log.info("")
            log.info(">>>>> INDEX " + str(index) + ", MZ: " + str(mz_id) + ", " + str(row["MICROZONE_NAME"]))
            lat = row["LATITUDE"]
            lng = row["LONGITUDE"]
            log.info("lat: " + str(lat))
            log.info("lng: " + str(lng))
            search_result = get_stores(store_to_scrape, lat, lng)
            df_search_result = pd.json_normalize(search_result)
            try:
                df_search_result = df_search_result.drop(
                    columns=[
                        "geometry.viewport.northeast.lat",
                        "geometry.viewport.northeast.lng",
                        "geometry.viewport.southwest.lat",
                        "geometry.viewport.southwest.lng",
                    ]
                )
            except:
                log.info("Error de drop")
            log.info("-- df_search_result --")
            log.info(len(df_search_result))
            # log.info(df_search_result.head())
            df_search_result.columns = df_search_result.columns.map(lambda x: x.split(".")[-1])
            df_search_result = df_search_result.drop_duplicates(subset="place_id", keep="first")
            stores_to_insert = []
            for index, row in df_search_result.iterrows():
                place_id = row["place_id"]
                store_name = row["name"]
                store_lat = row["lat"]
                store_lng = row["lng"]
                log.info("STORE " + str(index) + ": " + str(place_id) + 
                    ", " + str(store_name) + 
                    ", " + str(store_lat) + 
                    ", " + str(store_lng))

                if store_to_scrape.upper() in store_name.upper():
                    stores_to_insert.append([
                        code,store_to_scrape,mz_id,place_id,
                        store_name,store_lat,store_lng,
                        pd.to_datetime('today').strftime('%Y-%m-%d')
                        ])

            df_stores_to_insert = pd.DataFrame(
                stores_to_insert, 
                columns=['CODE','BRAND','MICROZONE_ID','PLACE_ID','STORE_NAME','LATITUDE','LONGITUDE','UPDATED_AT'])
            log.info("Se insertaran " + str(df_stores_to_insert.shape[0]))
            if df_stores_to_insert.shape[0] > 0:
                success, nchunks, nrows, _ = write_pandas(conn, df_stores_to_insert, str(code)+'_MZ_STORES_AVAILABILITY')
                print(f'{nrows} added to table')
                log.info("Insert OK. Added rows: " + str(nrows))
            else:
                print("0 added to table")
                log.info("0 rows inserted")
                    

def main():
    print(">>> Process start.")
    try:
        get_centroid_coords()
    finally:
        conn.close()

if __name__ == "__main__":
    main()