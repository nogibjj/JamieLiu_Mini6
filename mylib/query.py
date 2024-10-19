"""
Query the drinks data from the Databricks database
"""

from dotenv import load_dotenv
from databricks import sql
import os

complex_query = """
WITH country_average AS (
  SELECT country,
    total_litres_of_pure_alcohol, 
    (beer_servings + spirit_servings + wine_servings) / 3.0 \
      AS avg_of_beer_spirit_wine_servings
  FROM jl1229_drinksdb
),

group_summary AS (
  SELECT total_litres_of_pure_alcohol,
    COUNT(country) AS country_count
  FROM jl1229_drinksdb
  GROUP BY total_litres_of_pure_alcohol
)

SELECT jl1229_drinksdb.country, jl1229_drinksdb.beer_servings, \
  jl1229_drinksdb.spirit_servings, jl1229_drinksdb.wine_servings, \
    jl1229_drinksdb.total_litres_of_pure_alcohol, 
       group_summary.country_count, country_average.avg_of_beer_spirit_wine_servings
FROM jl1229_drinksdb
JOIN country_average 
  ON jl1229_drinksdb.country = country_average.country
JOIN group_summary 
  ON jl1229_drinksdb.total_litres_of_pure_alcohol = \
    group_summary.total_litres_of_pure_alcohol
ORDER BY jl1229_drinksdb.total_litres_of_pure_alcohol DESC, \
  country_average.avg_of_beer_spirit_wine_servings DESC;
"""

def query():
    """Query the database"""
    load_dotenv()
    with sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_KEY"),
    ) as connection:
        with connection.cursor() as cursor:

            cursor.execute(complex_query)
            result = cursor.fetchall()

            for row in result:
                print(row)

            cursor.close()
            connection.close()
    return "Query successful"


if __name__ == "__main__":
    query()
