#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from pymongo import MongoClient
import numpy as np
from datetime import datetime
import pandas as pd
from lib.yahoo_downloader import Downloader
import os


class DividendData:
    '''Class for saving, retrieving and updating company data in aristocrats database'''
    def __init__(self):
        os.remove('log.txt')
        self._log('Setting up data wrapper')
        self.db = MongoClient().dividend_investing
        self.downloader = Downloader()
        self.DRIP_URL = 'http://www.dripinvesting.org/tools/U.S.DividendChampions.xls'
        self.companies = self._get_companies()
        self._log('Data wrapper setup done')

    def _log(self, message, exception=False):
        '''Log events to text file'''
        if exception:
            message = 'Error: ' + message
        with open('log.txt', 'a') as file:
            file.write('{0}\t{1}\r'.format(datetime.now(), message))

    def print_log(self):
        '''Print log to stdout'''
        try:
            with open('log.txt', 'r') as file:
                for line in file:
                    print(line)
        except OSError as e:
            self._log('{0}: {1}'.format(e.filename, e.strerror), exception=True)
            self.print_log()

    def _define_category(self, years):
        if years > 24:
            return 'champion'
        elif years > 9:
            return 'contender'
        else:
            return 'challenger'

    def download_drip_sheet(self):
        '''Get data sheet from DRIP investing'''
        self._log('Trying to download DRIP datasheet...')
        try:
            companies = pd.read_excel(self.DRIP_URL, sheetname='All CCC')
        except Exception as e:
            self._log('Cannot download DRIP datasheet: {0}'.format(e), exception=True)
        else:
            self._log('DRIP data downloaded')
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
            df.drop(['divRaiseYrs'], axis=1, inplace=True)
            company_list = df.where(pd.notnull(df), None).to_dict(orient='records')
            self._log('DRIP list is processed')
            return company_list

    def download_company_history(self, ticker, interval=20):
        '''Get price and dividend history of a company from Yahoo'''
        self._log('Getting {1} year history for {0}'.format(ticker, interval))
        try:
            data = self.downloader.get_history(ticker=ticker, years=interval)
        except Exception as e:
            self._log('Failed to download history for {0}: {1}'.format(ticker, e), exception=True)
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
                self._log('When trying to calculate div yield for {0}: {1}'.format(ticker, e), exception=True)
                data['divYield'] = np.nan

            daily = data.drop(['dividend'], axis=1).where(pd.notnull(data), None)
            daily['type'] = 'price'


            dividend = data[data['dividend'] != 0].drop(['adjClose', 'lastDivAnnual', 'divYield'], axis=1)
            dividend['type'] = 'dividend'
            self._log('Retrieved history for {0}'.format(ticker))
            return daily.to_dict(orient='records') + dividend.to_dict(orient='records')

    def _get_companies(self):
        '''Download company list from database companies collection'''
        self._log('Getting company list from database')
        try:
            cursor = self.db.companies.find()
        except Exception as e:
            self._log('Cannot retrieve company list: {0}'.format(e))
            return None
        else:
            return list(cursor)

    def _check_download_date(self):
        '''Get the last time the company list was updated'''
        self._log('Checking last download date in company list')
        if self.companies and len(self.companies) > 0:
            max_date = np.max([company['downloaded'] for company in self.companies])
            return max_date
        else:
            self._log('No max date, company list is empty')
            return None

    def get_tickers(self, category='all'):
        '''Get list of aristocrat tickers with optional filter for category'''
        if category != 'all':
            tickers = [company['ticker'] for company in self.companies if company['category'] == category]
        else:
            tickers = [company['ticker'] for company in self.companies]
        self._log('Returning ticker list for {0} category'.format(category))
        return tickers

    def update_basic_company_data(self):
        '''Insert or update basic company data, returning list of tickers'''
        self._log('Updating basic company data')
        last_downloaded = self._check_download_date()
        if last_downloaded and datetime.today().month == last_downloaded.month:
            self._log('Company list seems up-to-date, skipping download')
        else:
            self._log('Company list is outdated, download needed')
            self.companies = self.download_drip_sheet()
            self._log('Starting to upload company profiles')
            for company in self.companies:
                try:
                    self.db.companies.update_one({'ticker': company['ticker']}, {'$set': company}, upsert=True)
                except Exception as e:
                    self._log('{0} when uploading {1} to company collection'.format(e, company['ticker']), exception=True)
                else:
                    self._log('{0} is uploaded to company collection'.format(company['ticker']))
            self._log('Company list update finished')
        return self.get_tickers()

    def update_company_history(self, ticker, data):
        '''Save new entries in price and dividend history'''
        self._log('Updating history for {0}'.format(ticker))
        price_data = [item for item in data if item['type'] == 'price']
        dividend_data = [item for item in data if item['type'] == 'dividend']

        # Insert document and placeholder fields if company is not in collection
        result = self.db.history.update_one(
            {'ticker': ticker},
            {'$setOnInsert': {
                'ticker': ticker,
                'price': price_data,
                'dividend': dividend_data,
                'lastUpdated': datetime.today()}
            },
            upsert=True)

        if result.upserted_id:
            self._log('Inserted {0} to history collection with {1} new data'.format(ticker, len(data)))
            return True
        else:
            # Get the existing entry and max dates if no insert happened
            entry = self.db.history.find_one({'ticker': ticker}, projection={'_id': 0})
            price_max_date = np.max([item['date'] for item in entry['price']])
            div_max_date = np.max([item['date'] for item in entry['dividend']])

            price_to_upload = [item for item in price_data if item['date'] > price_max_date]
            dividend_to_upload = [item for item in dividend_data if item['date'] > div_max_date]

            if len(price_to_upload) == 0 and len(dividend_to_upload) == 0:
                self._log('No new data for {0}, no updates added to database'.format(ticker))
                return True
            else:
                self._log('{0} price data, {1} dividend data for {2}'.format(len(price_to_upload), len(dividend_to_upload), ticker))
                try:
                    self.db.history.update_one(
                        {'ticker': ticker},
                        {'$push': {
                            'price': {'$each': price_to_upload},
                            'dividend': {'$each': dividend_to_upload}
                        },
                        '$set': {'lastUpdated': datetime.today()}
                    })
                except Exception as e:
                    self._log('Cannot update {0} in history collection: {1}'.format(ticker, e), exception=True)
                    return False
                else:
                    self._log('{0} history updated with {1} values'.format(ticker, len(price_to_upload) + len(dividend_to_upload)))
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
            self._log('Cannot retrieve latest price data for {0}: {1}'.format(ticker, e), exception=True)
            return None
        else:
            self._log('Last price data retrieved for {0}'.format(ticker))
            return last_item

    def _calculate_payout_ratio(self, ticker, last_div):
        '''Calculate the dividend-to-EPS payout ratio, if possible'''
        EPS = self.db.companies.find_one({'ticker': ticker}, projection={'_id': 0, 'EPS': 1})['EPS']
        if EPS and EPS > 0:
            payout = last_div / EPS * 100
        else:
            payout = None
        self._log('Payout calculated for {0}'.format(ticker))
        return payout

    def get_yield_distribution(self, ticker, interval=10):
        '''Calculate min, max, stdev, mean div yield for the given interval'''
        try:
            history = self.db.history.find_one({'ticker': ticker}, projection={'_id': 0, 'price': 1})['price']
        except Exception as e:
            self._log('Cannot retrieve price history for {0} for yield distribution: {1}'.format(ticker, e), exception=True)
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
                self._log('No history downloaded for {0}, cannot calculate yield distribution'.format(ticker))
                return None

    def update_company_profile(self, ticker):
        '''Set additional fields and data on existing aristocrat'''
        self._log('Updating {0} profile'.format(ticker))
        try:
            latest_data = self._get_latest_data(ticker)
            payout = self._calculate_payout_ratio(ticker, latest_data['lastDivAnnual'])
            yield_dist = self.get_yield_distribution(ticker)
            self.db.companies.update_one({'ticker': ticker}, {'$set': {
                    'annualDividend': latest_data['lastDivAnnual'],
                    'payout': payout,
                    'yieldDist': yield_dist,
                    'lastUpdated': datetime.today(),
                    'divYield': latest_data['divYield']
            }})
        except Exception as e:
            self._log('Cannot update {0} profile: {1}'.format(ticker, e), exception=True)
            return None
        else:
            self._log('{0} profile updated'.format(ticker))
            return True
