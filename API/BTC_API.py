import requests
import os
from datetime import datetime, timezone

def get_price(event,context):
    price=[]
    api_key = os.getenv("API_KEY")
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=" + api_key
    response = requests.get(url)
    if response.status_code == 200:
        data=response.json()
        for date_str in data["Time Series (Daily)"]:

            values = data["Time Series (Daily)"][date_str]
            timestamp = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

            
            open_price = float(values["1. open"])
            high_price = float(values["2. high"])
            low_price  = float(values["3. low"])
            close_price = float(values["4. close"])
            volume_price = int(values["5. volume"])



            price.append(
                    {"timestamp":timestamp,"open_price":open_price, "high_price":high_price, "low_price":low_price
                    , "close_price":close_price, "volume_price":volume_price}

            )
    else:
        print(f"Error: Unable to fetch data from API. Status code: {response.status_code}")
    
    return price
