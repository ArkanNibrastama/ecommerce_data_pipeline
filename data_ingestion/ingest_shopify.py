import creds
import requests

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
    # print(order_json)
    # print('\n'+'='*5)
    res = {}

    # return semua order
    for i, order in enumerate(order_json['orders']):
        res[i] = {
            'order_id' : order['id'],
            'is_confirmed' : order['confirmed'],
            'payment_gateway' : order['payment_gateway_names'],
            'total_price' : order['total_price'],
            'order_date' : order['created_at']
            # seharusnya kita tau informasi ttg:
            #  - usernya : id,nama,tempat, dll
                #customer
            #  - productya : product apa aja yang dibeli sama user, berapa jumlahnya, berapa harganya (per item)
                #line_items
        }

    print(res)

if __name__=="__main__":
    start_date = '2024-04-14 00:00:00.000'
    end_date = '2024-04-20 23:59:59.000'
    main(start_date=start_date, end_date=end_date)