import pandas as pd
import datetime as dt
import pickle
from user_data import *
from handle_data import handler
import pytz
import os
import warnings
warnings.filterwarnings("ignore")

import time

capital = float(handler().account_details()['NAV'])
daily_risk = capital * MAX_PERCENTAGE_ACCOUNT_AT_RISK
ind = ind()

assets = ['EUR_USD', 'EUR_JPY', 'EUR_GBP', 'EUR_AUD', 'EUR_CAD', 'USD_JPY', 'GBP_USD', 'AUD_USD', 'USD_CAD', 
        'GBP_JPY', 'AUD_JPY', 'CAD_JPY', 'GBP_AUD', 'GBP_CAD',
        'SPX500_USD', 'NAS100_USD', 'UK100_GBP', 'DE30_EUR', 'FR40_EUR', 'JP225_USD', 'HK33_HKD', 
        'AU200_AUD', 'IN50_USD', 'XAU_USD', 'WTICO_USD', 'XCU_USD', 'US2000_USD']

trade_short = [] 
trade_long = [] 

class build_plan:

    def __init__(self):
            pass

    def _get_new_data(self):

            if 'OANDA' not in os.listdir('./../DATA/'):
                    last = dt.datetime.now(tz=pytz.timezone("Europe/Moscow")) - dt.timedelta(1900)
                    db = pd.DataFrame()
            else:
                    db = pd.read_pickle('./../DATA/OANDA') 
                    db.columns = db.columns.str.lower()

                    db = db.reset_index().set_index('date')
                    last = sorted(db.index.unique())[-1]


            data = pd.DataFrame()

            if last.date() == (dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date() - dt.timedelta(1)): 
                    pass
            else:
                    for i in assets: 
                            df = handler().candle_data(i, 1440, 1, from_dt= last, to_dt= (dt.datetime.now(tz=pytz.timezone("Europe/Moscow"))) - dt.timedelta(1), use='plan') 

                            data = pd.concat([data, df], sort=True).drop_duplicates()

                    df = data.reset_index().rename({'index': 'date'}, axis=1).set_index(['date', 'asset'])

                    if len(db) > 1:
                            db = db.reset_index().set_index(['date', 'asset'])
                            df = pd.concat([db, df], sort=True).drop_duplicates()

                    df = df.reset_index().set_index('date')
                    
                    self._remove_duplicated(df)



    def _remove_duplicated(self, df):

            df = df.reset_index()
            db = pd.DataFrame()

            for i in df.asset.unique():
                    data = df[df.asset == i]      
                    pd.DataFrame.drop_duplicates(data, subset='date', inplace=True)  
                    db = pd.concat([db, data], sort=True)

            db = db.set_index('date')

            pd.DataFrame.sort_index(db, inplace=True)
            db.index = pd.to_datetime(db.index, format='%Y-%m-%d')

            last = [i for i in db.index.unique() if len(db[db.index == i]) == db.asset.nunique()][-1]
            db = db[db.index <= last]

            db.to_pickle('./../DATA/OANDA') 

            db.index = db.index + dt.timedelta(1)
            db = db[db.index.dayofweek < 5]

    def run_daily():

        plan = {
            'EUR_USD_0': {
                'atr' : 0.00750,
                'break_lunch' : [1100, 1500],
                'trading_hours' : [900, 1800],
                'profit' : [5, 30, 100, 'day'],
                'stop' : [1.5, 30, 100, 'day'],
                'duration' : pd.to_datetime(30, unit='m').time(),
                'try_qty' : 3,
                'direction' : 'sell',
                'strat' : {'strat2': 3},
                'strat_cond' : 'and',
                'strat_name' : 'trade_short',
                'size' : 100
                }, 

            'IBUS500': {
                'atr' : 18.00000,
                'break_lunch' : [1100, 1500],
                'trading_hours' : [900, 1800],
                'profit' : [5, 30, 100, 'day'],
                'stop' : [1.5, 30, 100, 'day'],
                'duration' : pd.to_datetime(60, unit='m').time(),
                'try_qty' : 3,
                'direction' : 'sell',
                'strat' : {'strat1': 5},
                'strat_cond' : 'and',
                'strat_name' : 'trade_short',
                'size' : 100
                }
            }

        pd.to_pickle(plan, f'./DATA/plan/plan_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')


        return plan


