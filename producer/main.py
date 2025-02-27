import time
import requests
import os
import random
import json 
from faker import Faker

fake = Faker()

names = [fake.name() for _ in range(20)]
name_weights = [i for i in range(20)]

pages = ["/home", "/about", "/contact", "/products", "/cart", "/checkout"]
page_weights = [10, 2, 3, 2, 2, 1]

browsers = ["Chrome", "Firefox", "Safari", "Edge"]
browser_weights = [1, 3, 2, 3]

def main():
    INGEST_API_URL = os.environ["INGEST_API_URL"]


    for i in range(1000):
        event = {
                "metadata": {
                    "name" : "page_load",
                    "version" : 'v1',
                    "timestamp": fake.date_time_between(start_date="-7d", end_date="now").isoformat()
                },
                "payload": {
                    "page": random.choices(pages, weights=page_weights)[0],
                    "user_name": random.choices(names, weights=name_weights)[0],
                    "browser": random.choices(browsers, weights=browser_weights)[0],
                },
            }
        
        # Randomly inject some null values into the "browser" column
        if random.randint(0,100) < 10:
            event['payload']['browser'] = None
        
        print(event)

        r = requests.post(
            url=f"{INGEST_API_URL}/event", 
            data=json.dumps(event)
        )

        time.sleep(0.1)



if __name__ == '__main__':
    main()