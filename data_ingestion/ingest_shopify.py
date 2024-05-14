import creds
import requests
from datetime import datetime
import json

def create_session():

    session = requests.Session()
    session.headers.update({
        "X-Shopify-Access-Token": creds.token
    })

    return session

def main(start_date, end_date):

    store = create_session()
    orders = store.get(creds.url+"/admin/api/"+creds.api_version+"/orders.json?status=any&created_at_min="+start_date+"&created_at_max="+end_date)
    order_json = orders.json()

    # return semua order
    for i, order in enumerate(order_json['orders']):
        
        with open("../data/"+datetime.now().strftime("%d%m%Y_")+str(i)+".json", "w") as json_file:
            json.dump(order, json_file)

if __name__=="__main__":
    start_date = '2024-04-14 00:00:00.000'
    end_date = '2024-05-01 23:59:59.000'
    main(start_date=start_date, end_date=end_date)