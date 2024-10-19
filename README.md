# JamieLiu_Mini6

## Overview

This project involves querying a dataset about alcohol consumption using Databricks. The dataset contains information about different countries and their alcohol consumption in terms of beer, spirit, and wine servings, as well as the total litres of pure alcohol consumed.

The core of the project is a Python script that connects to a Databricks database and runs a complex SQL query to generate insights from the dataset. The query utilizes aggregations, joins, and sorting to provide a detailed overview of alcohol consumption patterns.

## Dataset

The dataset (`jl1229_drinksdb`) contains the following columns:

- `country`: Name of the country
- `beer_servings`: Average number of beer servings per year
- `spirit_servings`: Average number of spirit servings per year
- `wine_servings`: Average number of wine servings per year
- `total_litres_of_pure_alcohol`: Total litres of pure alcohol consumed per year

## The Complex Query

The complex query in the project performs the following operations:

1. **Country Average Calculation (`country_average` CTE)**:

   - For each country, the query calculates the average of beer, spirit, and wine servings. This average is represented as `avg_of_beer_spirit_wine_servings`.
   - This average is computed using the formula:

     ```
     (beer_servings + spirit_servings + wine_servings) / 3.0
     ```

2. **Group Summary Calculation (`group_summary` CTE)**:
   - The query groups the data by `total_litres_of_pure_alcohol` and calculates the number of countries in each group (`country_count`).
3. **Main Query**:
   - The main query joins the original dataset with the results of the two CTEs (`country_average` and `group_summary`).
   - It selects the following fields for each country:
     - `country`, `beer_servings`, `spirit_servings`, `wine_servings`, `total_litres_of_pure_alcohol`
     - The count of countries in the same `total_litres_of_pure_alcohol` group (`country_count`)
     - The average of beer, spirit, and wine servings for that country (`avg_of_beer_spirit_wine_servings`)
   - The results are sorted in descending order by:
     - `total_litres_of_pure_alcohol`: Higher alcohol consumption groups are displayed first.
     - `avg_of_beer_spirit_wine_servings`: Within each group, countries are displayed in descending order of their average servings.

## Usage

### Prerequisites

- Python 3.x
- Databricks SQL Connector for Python
- A `.env` file with the following variables:
  - `SERVER_HOSTNAME`: Databricks server hostname
  - `HTTP_PATH`: Databricks HTTP path
  - `DATABRICKS_KEY`: Access token for authentication

### Running the Script

1. **Clone the repository:**
   ```bash
   git clone git@github.com:nogibjj/JamieLiu_Mini6.git
   cd JamieLiu_Mini6
   ```
2. **Install project dependencies:**
   ```bash
   make install
   ```
3. **Format the code:**

   ```bash
   make format
   ```

4. **Run linting checks:**

   ```bash
   make lint
   ```

5. **Run tests:**

   ```bash
   make test
   ```

6. **Run all steps (Install, Format, Lint, Test):**
   ```bash
   make all
   ```
