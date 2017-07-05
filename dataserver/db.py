import datetime
from pymongo import MongoClient
db = MongoClient().dividend_investing


def get_companies():
    companies = db.companies.find(projection={'_id': 0})
    return list(companies)


def get_company_data(ticker):
    company = db.companies.find(filter={'ticker': ticker}, projection={'_id': 0})
    return list(company)[0]


def get_historical_data(ticker, data_type='price', date_range='5'):
    today = datetime.datetime.today()
    try:
        count_years = int(date_range)
    except Exception:
        count_years = 5

    start_date = datetime.datetime(today.year - count_years, today.month, today.day)

    if data_type == 'price':
        history = db.history.aggregate([
            {'$match': {'ticker': ticker}},
            {'$project': {
                '_id': 1,
                'price': '$daily',
                'ticker': 1
            }},
            {'$unwind': '$price'},
            {'$match': {'price.date': {'$gte': start_date}}},
            {'$group': {
                '_id': {
                    'id': '$_id',
                    'ticker': '$ticker'
                },
                'price': {'$push': '$price'},
            }},
            {'$project': {
                '_id': 0,
                'ticker': '$_id.ticker',
                'price': 1
            }}
        ])
    elif data_type == 'dividend':
        history = db.history.aggregate([
            {'$match': {'ticker': ticker}},
            {'$project': {
                '_id': 1,
                'dividend': 1,
                'ticker': 1
            }},
            {'$unwind': '$dividend'},
            {'$match': {'dividend.date': {'$gte': start_date}}},
            {'$group': {
                '_id': {
                    'id': '$_id',
                    'ticker': '$ticker'
                },
                'dividend': {'$push': '$dividend'}
            }},
            {'$project': {
                '_id': 0,
                'ticker': '$_id.ticker',
                'dividend': 1
            }}
        ])
    else:
        raise Exception('Invalid data type parameter')

    return list(history)[0]
