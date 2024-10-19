"""
Transforms and Loads drinks data into the local Databricks database
"""

import csv
import os
from dotenv import load_dotenv
from databricks import sql


# load the csv file and insert into a new databricks database
def load(dataset="data/drinks.csv"):
    """Transforms and Loads data into the local Databricks database"""
    payload = csv.reader(open(dataset, newline=""), delimiter=",")
    next(payload)
    
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    ) as connection:
        with connection.cursor() as cursor:
            # country, beer_servings, spirit_servings, 
            # wine_servings, total_litres_of_pure_alcohol
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS jl1229_drinksdb 
                           (country STRING, beer_servings INT, spirit_servings INT, 
                           wine_servings INT, total_litres_of_pure_alcohol FLOAT);
                """
            )

            cursor.execute("SELECT * FROM jl1229_drinksdb")
            result = cursor.fetchall()
            if not result:
                print("Loading data into the database")
                insert_sql = "INSERT INTO jl1229_drinksdb VALUES "
                for i in payload:
                    insert_sql += "\n" + str(tuple(i)) + ","
                insert_sql = insert_sql[:-1] + ";"
                
                cursor.execute(insert_sql)

            cursor.close()
            connection.close()
    
    return "Drinks data loaded or already loaded"


if __name__ == "__main__":
    load()
