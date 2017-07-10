#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from pymongo import MongoClient
import numpy as np
from datetime import datetime
import pandas as pd
from lib.yahoo_downloader import Downloader
import math
import logging
import sys


class DividendData:
    '''Class for saving, retrieving and updating company data in aristocrats database'''
    def __init__(self):
        logging.basicConfig(filename='datahandler.log', filemode='w', level=logging.INFO, format='%(asctime)s;%(levelname)s;%(message)s')
        logging.debug('Setting up data wrapper')
        self.db = MongoClient().dividend_investing
        self.downloader = Downloader()
        self.DRIP_URL = 'http://www.dripinvesting.org/tools/U.S.DividendChampions.xls'
        try:
            self.companies = self._get_companies()
        except Exception as e:
            logging.critical(e)
            sys.exit(1)
        logging.info('Ready to crunch data!')

    def _define_category(self, years):
        if years > 24:
            return 'champion'
        elif years > 9:
            return 'contender'
        else:
            return 'challenger'

    def download_drip_sheet(self):
        '''Get data sheet from DRIP investing'''
        logging.debug('Trying to download DRIP datasheet...')
        try:
            companies = pd.read_excel(self.DRIP_URL, sheetname='All CCC')
        except Exception as e:
            logging.critical('Cannot download DRIP datasheet: {0}'.format(e))
        else:
            logging.info('DRIP data downloaded')
            companies.columns = companies.iloc[4]
            companies.reindex(companies)
            useful_columns = ['Name', 'Symbol', 'Industry', 'Yrs', '1-yr', '3-yr', '5-yr', '10-yr', 'EPS']
            df = companies[useful_columns][5:-2]
            name_map = {
                '1-yr': 'divg1y',
                '3-yr': 'divg3y',
                '5-yr': 'divg5y',
                '10-yr': 'divg10y',
                'Yrs': 'divRaiseYrs',
                'Symbol': 'ticker',
                'Industry': 'industry',
                'Name': 'name'
            }
            df.rename(columns=name_map, inplace=True)
            df['downloaded'] = datetime.today()
            df.replace('n/a', value=np.nan, inplace=True)
            df['category'] = df['divRaiseYrs'].apply(self._define_category)
            company_list = df.where(pd.notnull(df), None).to_dict(orient='records')
            logging.debug('DRIP list is processed')
            return company_list

    def _download_company_history(self, ticker, interval=20):
        '''Get price and dividend history of a company from Yahoo and format/process it'''
        logging.debug('Getting {1} year history for {0}'.format(ticker, interval))
        try:
            data = self.downloader.get_history(ticker=ticker, years=interval)
        except Exception as e:
            logging.error('Failed to download history for {0}: {1}'.format(ticker, e))
            return []
        else:
            data['date'] = data.index
            data['ticker'] = ticker

            data.drop(['Open', 'High', 'Low', 'Close', 'Volume', 'Stock Splits'], axis=1, inplace=True)
            data.rename(columns={
                'Adj Close': 'adjClose',
                'Dividends': 'dividend'
            }, inplace=True)

            # Yahoo data series sometimes contain 'null' values
            data['adjClose'] = pd.to_numeric(data['adjClose'], errors='coerce')
            data.dropna(axis=0, how='any', subset=['adjClose'], inplace=True)

            data['lastDivAnnual'] = data['dividend'].replace(0, value=np.nan).fillna(method='ffill') * 4
            try:
                data['divYield'] = data['lastDivAnnual'] / data['adjClose'] * 100
            except Exception as e:
                logging.error('When trying to calculate div yield for {0}: {1}'.format(ticker, e))
                data['divYield'] = np.nan

            daily = data.drop(['dividend'], axis=1).where(pd.notnull(data), None)
            daily['type'] = 'price'

            dividend = data[data['dividend'] != 0].drop(['adjClose', 'lastDivAnnual', 'divYield'], axis=1)
            dividend['type'] = 'dividend'
            logging.info('Retrieved history for {0}'.format(ticker))
            return daily.to_dict(orient='records') + dividend.to_dict(orient='records')

    def _get_companies(self):
        '''Download company list from database companies collection'''
        logging.debug('Getting company list from database')
        try:
            cursor = self.db.companies.find()
        except Exception as e:
            logging.error('Cannot retrieve company list: {0}'.format(e))
            return None
        else:
            return list(cursor)

    def _check_download_date(self):
        '''Get the last time the company list was updated'''
        logging.debug('Checking last download date in company list')
        if self.companies and len(self.companies) > 0:
            max_date = np.max([company['downloaded'] for company in self.companies])
            return max_date
        else:
            logging.debug('No max date, company list is empty')
            return None

    def get_tickers(self, category='all'):
        '''Get list of aristocrat tickers with optional filter for category'''
        if category != 'all':
            tickers = [company['ticker'] for company in self.companies if company['category'] == category]
        else:
            tickers = [company['ticker'] for company in self.companies]
        logging.debug('Returning ticker list for {0} category'.format(category))
        return tickers

    def update_basic_company_data(self):
        '''Insert or update basic company data, returning list of tickers'''
        logging.debug('Updating basic company data')
        last_downloaded = self._check_download_date()
        if last_downloaded and datetime.today().month == last_downloaded.month:
            logging.info('Company list seems up-to-date, skipping download')
        else:
            logging.info('Company list is outdated, download needed')
            self.companies = self.download_drip_sheet()
            logging.debug('Starting to upload company profiles')
            for company in self.companies:
                try:
                    self.db.companies.update_one({'ticker': company['ticker']}, {'$set': company}, upsert=True)
                except Exception as e:
                    logging.error('{0} when uploading {1} to company collection'.format(e, company['ticker']))
                else:
                    logging.info('{0} is uploaded to company collection'.format(company['ticker']))
            logging.debug('Company list update finished')
        return self.get_tickers()

    def update_company_history(self, ticker):
        '''Save new entries in price and dividend history'''
        logging.debug('Updating history for {0}'.format(ticker))
        try:
            # Get the last existing date from database
            agg = self.db.history.aggregate([
                {'$match': {'ticker': ticker}},
                {'$project': {'_id': 0, 'price': 1}},
                {'$unwind': '$price'},
                {'$sort': {'price.date': -1}},
                {'$limit': 1},
                {'$project': {'price.date': 1}}
            ])
            max_date = list(agg)[0]['price']['date']
            delta = datetime.today() - max_date
            interval = min([math.ceil(delta.days / 365) + 1, 20])
        except IndexError:
            # Company is not in database or no price data yet
            max_date = datetime(1900, 1, 1)
            interval = 20

        data = self._download_company_history(ticker, interval)

        price_data = [item for item in data if item['type'] == 'price' and item['date'] > max_date]
        dividend_data = [item for item in data if item['type'] == 'dividend' and item['date'] > max_date]

        if len(price_data) == 0 and len(dividend_data) == 0:
            logging.info('No new data for {0}, no updates added to database'.format(ticker))
            return True
        else:
            logging.info('{0} price data, {1} dividend data for {2}'.format(len(price_data), len(dividend_data), ticker))

        # Insert document and placeholder fields if company is not in collection
        try:
            self.db.history.update_one(
                {'ticker': ticker},
                {'$setOnInsert': {
                    'ticker': ticker
                },
                '$push': {
                    'price': {'$each': price_data},
                    'dividend': {'$each': dividend_data}
                },
                '$set': {'lastUpdated': datetime.today()}
                },
                upsert=True)
        except Exception as e:
            logging.error('Cannot update {0} in history collection: {1}'.format(ticker, e))
            return False
        else:
            logging.info('{0} history updated with {1} values'.format(ticker, len(price_data) + len(dividend_data)))
            return True

    def _get_latest_data(self, ticker, type='price'):
        '''Get the last entry in history of a given ticker'''
        try:
            agg = self.db.history.aggregate([
                {'$match': {'ticker': ticker}},
                {'$project': {'_id': 0, 'price': 1}},
                {'$unwind': '$price'},
                {'$sort': {'price.date': -1}},
                {'$limit': 1}
            ])
            last_item = list(agg)[0]['price']
        except Exception as e:
            logging.error('Cannot retrieve latest price data for {0}: {1}'.format(ticker, e))
            return None
        else:
            logging.debug('Last price data retrieved for {0}'.format(ticker))
            return last_item

    def _calculate_payout_ratio(self, ticker, last_div):
        '''Calculate the dividend-to-EPS payout ratio, if possible'''
        EPS = self.db.companies.find_one({'ticker': ticker}, projection={'_id': 0, 'EPS': 1})['EPS']
        if EPS and EPS > 0:
            payout = last_div / EPS * 100
        else:
            payout = None
        logging.debug('Payout calculated for {0}'.format(ticker))
        return payout

    def get_yield_distribution(self, ticker, interval=10):
        '''Calculate min, max, stdev, mean div yield for the given interval'''
        try:
            history = self.db.history.find_one({'ticker': ticker}, projection={'_id': 0, 'price': 1})['price']
        except Exception as e:
            logging.error('Cannot retrieve price history for {0} for yield distribution: {1}'.format(ticker, e))
            return None
        else:
            if len(history) > 0:
                max_date = np.max([item['date'] for item in history])
                start_date = max_date.replace(year=max_date.year - interval)
                filtered = [item['divYield'] for item in history if item['date'] > start_date and item['divYield']]

                yield_dist = {
                    'interval': interval,
                    'max': np.max(filtered),
                    'min': np.min(filtered),
                    'mean': np.mean(filtered),
                    'std': np.std(filtered)
                }
                return yield_dist
            else:
                logging.info('No history downloaded for {0}, cannot calculate yield distribution'.format(ticker))
                return None

    def update_company_profile(self, ticker):
        '''Set additional fields and data on existing aristocrat'''
        logging.debug('Updating {0} profile'.format(ticker))
        try:
            data_used = self.db.companies.find_one({'ticker': ticker}, projection={'_id': 0, 'lastDataUsed': 1})
            latest_data = self._get_latest_data(ticker)

            if 'lastDataUsed' in data_used and data_used['lastDataUsed'] == latest_data['date']:
                logging.info('No new data for {0}, skipping profile update'.format(ticker))
                return True

            payout = self._calculate_payout_ratio(ticker, latest_data['lastDivAnnual'])
            yield_dist = self.get_yield_distribution(ticker)
            self.db.companies.update_one({'ticker': ticker}, {'$set': {
                    'annualDividend': latest_data['lastDivAnnual'],
                    'payout': payout,
                    'yieldDist': yield_dist,
                    'lastDataUsed': latest_data['date'],
                    'divYield': latest_data['divYield'],
                    'lastUpdated': datetime.today()
            }})
        except Exception as e:
            logging.error('Cannot update {0} profile: {1}'.format(ticker, e))
            return None
        else:
            logging.info('{0} profile updated'.format(ticker))
            return True
