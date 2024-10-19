"""
Extract a dataset

drinks dataset
"""
import requests


def extract(url= "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/alcohol-consumption/drinks.csv", 
            file_path="data/drinks.csv"):
    """"Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path

if __name__ == "__main__":
    extract()
    