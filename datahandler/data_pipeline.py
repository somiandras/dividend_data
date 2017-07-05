#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from lib.data_wrapper import DividendData


if __name__ == '__main__':
    data = DividendData()
    tickers = data.update_basic_company_data()
    for ticker in tickers:
        data.update_company_history(ticker)
        data.update_company_profile(ticker)
