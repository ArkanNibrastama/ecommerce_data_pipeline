import shopify, cred

SHOP_URL = 'tesecommercearkan.myshopify.com'
API_VERSION = '2024-01'
cred = cred.token()

session = shopify.Session(SHOP_URL, API_VERSION, cred)
shopify.ShopifyResource.activate_session(session=session)

products = shopify.Product.find(limit=10)
for product in products:
    print(product.title)


# what's next:
# - explore shopify data ingetion (what data can be ingested? and how?)
# - connect shopify with GA4