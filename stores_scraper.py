"""
stores_scraper.py
Author: Alain Moré
Date: 13-09-2021

Scrapes Google places API for the given stores and matches them
to existing Rappi stores, to validate if they are, or not, in Rappi
"""

import configparser
import logging
import time
import warnings
from datetime import datetime

import googlemaps
import pandas as pd
import snowflake.connector
from geopy import distance
from snowflake.connector.pandas_tools import write_pandas

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

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user="ALAIN.MORE@RAPPI.COM",
    account="hg51401",
    authenticator="externalbrowser",
    database="FIVETRAN",
    role="GLOBAL_ECOMMERCE_WRITE_ROLE",
    schema="GLOBAL_ECOMMERCE",
)

# Google Places API Client
gmaps = googlemaps.Client(key=api_key)

# Store parameters to scrape, currently hardcoded por 1 store.
# Need to change in the execute method to probably use a loop reading from a table or CSV file
code_list = ["CO"]
brand_to_scrape_id = 32917
brand_to_scrape = "Koaj"


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
    if len(result) == 0:
        return 0
    else:
        headers = list(map(lambda t: t[0], cursor.description))
        df = pd.DataFrame(result)
        df.columns = headers
        return df


def get_stores(store, lat, lng):
    """
    Scrape all stores matching the 'store' parameter,
    prefering options located in a 5000 meters radius from
    the point whose coordinates are lat,lng
    """
    search_result = gmaps.places_nearby(
        keyword=store, location=str(lat) + "," + str(lng), radius=5000
    )
    all_store_results = search_result["results"]
    # Might need to do 1 or 2 additional searches if we get more than 20 results.
    # Maximum: 60 results total, in 3 pages of 20.
    if "next_page_token" in search_result:
        time.sleep(2)
        other_results_1 = gmaps.places(page_token=search_result["next_page_token"])
        all_store_results = all_store_results + other_results_1["results"]

        if "next_page_token" in other_results_1:
            time.sleep(2)
            other_results_2 = gmaps.places(
                page_token=other_results_1["next_page_token"]
            )
            all_store_results = all_store_results + other_results_2["results"]

    return all_store_results


def get_store_details(place_id):
    """
    Function to get phone and address of a scraped store,
    since the default search doesn't give these details.
    """
    address = ""
    phone = ""
    log.info(">GET PLACE DETAILS: " + place_id)
    my_fields = ["formatted_address", "formatted_phone_number"]
    place_details = gmaps.place(place_id=place_id, fields=my_fields)
    result = place_details["result"]
    if "formatted_address" in result:
        address = result["formatted_address"]
    if "formatted_phone_number" in result:
        phone = result["formatted_phone_number"]
    return address, phone


def get_distance(row):
    """
    Get the distance between a scraped store and
    a store that already exists in Rappi
    """
    scraped_store_coords = (row["G_LATITUDE"], row["G_LONGITUDE"])
    rappi_store_coords = (row["LAT"], row["LNG"])
    return distance.distance(scraped_store_coords, rappi_store_coords).km


def is_store_in_rappi(row):
    """
    If the distance is equal or less than 200m,
    we consider it's the same store as the one in Rappi
    """
    if row["DISTANCE"] <= 0.2:
        return True
    else:
        return False


def execute_process():
    """
    Function that executes and orchestrates the whole process
    """
    conn.cursor().execute("USE SCHEMA FIVETRAN.GLOBAL_ECOMMERCE")
    conn.cursor().execute("USE ROLE GLOBAL_ECOMMERCE_WRITE_ROLE")
    conn.cursor().execute("USE WAREHOUSE ECOMMERCE")

    # As explained before, this loop needs to be adapted to whatever
    # input method might be used to scrape (CSV, table or other)
    for code in code_list:
        log.info(">>>>>>>>>>>> Code: " + code)
        # Get all existing stores in Rappi
        df_rappi_stores = pandas_df_from_snowflake_query(
            """   
            SELECT
                COUNTRY AS CODE
                ,STORE_ID AS RP_STORE_ID
                ,IS_ENABLED AS RP_IS_ENABLED
                ,LAT
                ,LNG
                ,BRAND_ID AS RP_BRAND_ID
                ,MICROZONE_ID AS RP_MICROZONE_ID
            FROM
            GLOBAL_ECOMMERCE.ECOMMERCE_STORES
            WHERE
                COUNTRY = '"""
            + code
            + """'
                AND RP_BRAND_ID = """
            + str(brand_to_scrape_id)
            + """
            """
        )
        log.info(">>RAPPI stores: " + code + ", " + str(brand_to_scrape_id))
        log.info(df_rappi_stores.shape[0])
        log.info(df_rappi_stores.head())

        # Get all active microzones info for the current country
        df_microzones = pandas_df_from_snowflake_query(
            """   
            SELECT * 
            FROM GLOBAL_VALUE_PROP_DS.active_microzones
            WHERE 
                CODE = '"""
            + code
            + """'
                AND IS_ACTIVE 
                AND MICROZONE_ID < 234
                """
        )
        log.info(df_microzones.shape[0])
        log.info(df_microzones.head())

        # For each microzone from he previous step, scrape all stores for the current brand,
        # in an approximate 5km radius from the mz centroid
        stores_to_insert = []
        for index, row in df_microzones.iterrows():
            mz_id = row["MICROZONE_ID"]
            log.info("")
            log.info(
                ">>>>> INDEX "
                + str(index)
                + ", MZ: "
                + str(mz_id)
                + ", "
                + str(row["MICROZONE_NAME"])
            )
            lat = row["LATITUDE"]
            lng = row["LONGITUDE"]
            log.info("lat: " + str(lat))
            log.info("lng: " + str(lng))
            search_result = get_stores(brand_to_scrape, lat, lng)
            df_search_result = pd.json_normalize(search_result)

            # We drop some info returned by the Places API that we don't need
            # and may confuse the script
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
                log.info("No columns to drop")
            log.info("-- df_search_result --")
            log.info(len(df_search_result))

            # We split the composed columns so we only get normal, non-composed column names
            df_search_result.columns = df_search_result.columns.map(
                lambda x: x.split(".")[-1]
            )

            # For each scraped store:
            for index, row in df_search_result.iterrows():
                place_id = row["place_id"]
                store_name = row["name"]
                store_lat = row["lat"]
                store_lng = row["lng"]
                store_types = row["types"]
                store_status = row["business_status"]
                store_address, store_phone = get_store_details(place_id)

                log.info(
                    "STORE "
                    + str(index)
                    + ": "
                    + str(place_id)
                    + ", "
                    + str(store_name)
                    + ", "
                    + str(store_lat)
                    + ", "
                    + str(store_lng)
                    + ", "
                    + str(store_status)
                    + ", "
                    + str(store_types)
                    + ", "
                    + str(store_address)
                    + ", "
                    + str(store_phone)
                )

                # We validate the scraped result has the store name in its NAME field,
                # if not we ignore it
                if brand_to_scrape.upper() in store_name.upper():
                    stores_to_insert.append(
                        [
                            code,
                            brand_to_scrape_id,
                            brand_to_scrape,
                            mz_id,
                            place_id,
                            store_name,
                            store_status,
                            store_types,
                            store_address,
                            store_phone,
                            store_lat,
                            store_lng,
                            datetime.now(),
                        ]
                    )
            print(
                ">> results "
                + str(index)
                + ": "
                + str(brand_to_scrape_id)
                + ", "
                + brand_to_scrape
            )

        df_scraper = pd.DataFrame(
            stores_to_insert,
            columns=[
                "CODE",
                "BRAND_ID",
                "BRAND_NAME",
                "SEARCH_MICROZONE_ID",
                "G_PLACE_ID",
                "G_STORE_NAME",
                "G_BUSINESS_STATUS",
                "G_TYPES",
                "G_ADDRESS",
                "G_PHONE",
                "G_LATITUDE",
                "G_LONGITUDE",
                "UPDATED_AT",
            ],
        )

        # We remove duplicate scraped stores
        df_scraper = df_scraper.drop_duplicates(subset="G_PLACE_ID", keep="first")
        log.info(">>Tiendas unicas scrapeadas: " + str(len(df_scraper.index)))
        log.info(
            ">>Tiendas unicas para el brand en Rappi: "
            + str(len(df_rappi_stores.index))
        )

        # We join, for each of our scraped stores, all existing rappi stores for the brand
        # Then, for each combination, we get its distance and validate if it might be in Rappi or not
        df_joined = pd.merge(df_scraper, df_rappi_stores, on="CODE", how="left")
        df_joined["DISTANCE"] = df_joined.apply(lambda row: get_distance(row), axis=1)
        df_joined["IN_RAPPI"] = df_joined.apply(
            lambda row: is_store_in_rappi(row), axis=1
        )
        # We drop duplicate not needed columns
        df_joined.drop(
            ["RP_BRAND_ID", "LAT", "LNG", "RP_MICROZONE_ID"], axis=1, inplace=True
        )
        print(">> df_joined")
        print(df_joined.head())
        print(list(df_joined.columns.values))
        # df_joined.to_csv('join.csv',encoding='utf-8')
        print("-----------------------------------------------------------")

        # We group all rows by unique scraped store (PlACE_ID), and only keep the ones with
        # the smallest distance. If the smallest distance is 200m or less, its safe to say we have it in Rappi
        df_stores_to_insert = df_joined.loc[
            df_joined.groupby("G_PLACE_ID").DISTANCE.idxmin()
        ].reset_index(drop=True)
        df_stores_to_insert = df_stores_to_insert[
            [
                "CODE",
                "BRAND_ID",
                "BRAND_NAME",
                "SEARCH_MICROZONE_ID",
                "RP_STORE_ID",
                "RP_IS_ENABLED",
                "G_PLACE_ID",
                "G_STORE_NAME",
                "G_BUSINESS_STATUS",
                "G_TYPES",
                "G_ADDRESS",
                "G_PHONE",
                "G_LATITUDE",
                "G_LONGITUDE",
                "UPDATED_AT",
                "DISTANCE",
                "IN_RAPPI"
            ]
        ]
        print(">> df_stores_to_insert")
        print(df_stores_to_insert.head())
        print(list(df_stores_to_insert.columns.values))
        df_stores_to_insert.to_csv('store_inserted.csv',encoding='utf-8')

        # We insert the results in snowflake, in the country's correct table
        log.info("Se insertaran " + str(df_stores_to_insert.shape[0]))
        if df_stores_to_insert.shape[0] > 0:
            success, nchunks, nrows, _ = write_pandas(
                conn, df_stores_to_insert, str(code) + "_MZ_STORES_AVAILABILITY"
            )
            print(f"{nrows} added to table")
            log.info("Insert OK. Added rows: " + str(nrows))
        else:
            print("0 added to table")
            log.info("0 rows inserted")


def main():
    print(">>> Process start.")
    try:
        execute_process()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
