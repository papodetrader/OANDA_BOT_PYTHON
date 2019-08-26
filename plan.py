import pandas as pd
import datetime as dt
from plan_indicat import ind
import pickle
from user_data import *
from handle_data import handler
import pytz

import oandapyV20.endpoints.instruments as instruments
from oandapyV20 import API
from dateutil.parser import parse

api = API(access_token=ACCESS_TOKEN, environment=ENVIRONMENT)

risk = MAX_PERCENTAGE_ACCOUNT_AT_RISK
capital = float(handler().account_details()['NAV'])
daily_risk = capital * risk
ind = ind()

assets = ['EUR_USD', 'EUR_AUD', 'GBP_JPY', 'GBP_CAD', 'USD_CAD', 'USD_JPY', 'AUD_JPY', 
        'SPX500_USD', 'NAS100_USD', 'UK100_GBP', 'DE30_EUR', 'FR40_EUR', 'JP225_USD', 'HK33_HKD', 
        'AU200_AUD', 'IN50_USD', 'XAU_USD', 'WTICO_USD', 'XCU_USD', 'US2000_USD']

trade_short = [] 
trade_long = [] 

class build_plan:

        def __init__(self):
                pass

        def candle_data(self, symbol, granularity, de, para):
                """ Candle data for symbols """

                params = {"from": de,
                "granularity": granularity, 
                "to": para,
                "alignmentTimezone": "Europe/Moscow", 
                }

                r = instruments.InstrumentsCandles(instrument=symbol, params=params)
                data = api.request(r)
                clean = [{'time':i['time'],"open":i['mid']['o'],
                        "close":i['mid']['c'],'high':i['mid']['h'],
                        'low':i['mid']['l']} for i in data['candles']]
        
                df = pd.DataFrame(clean)
                
                df.set_index('time',inplace=True)
                df.index = [ parse(i).strftime('%Y-%m-%d') for i in df.index ]
                df.index = pd.to_datetime(df.index)
                
                df = df.convert_objects(convert_numeric=True)
                df['asset'] = symbol

                return df


        def _get_new_data(self):
                db = pd.read_pickle('./../DATA/OANDA') 
                db.columns = db.columns.str.lower()

                db = db.reset_index().set_index('date')
                last = sorted(db.index.unique())[-1].date()

                data = pd.DataFrame()

                if last == (dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date() - dt.timedelta(1)): #UP-
                        pass
                else:
                        for i in assets: 
                                df = self.candle_data(i, 'D', last, (dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()) - dt.timedelta(1)) #UP-

                                data = pd.concat([data, df], sort=True).drop_duplicates()

                        data = data.reset_index().rename({'index': 'date'}, axis=1).set_index(['date', 'asset'])

                        db = db.reset_index().set_index(['date', 'asset'])

                        df = pd.concat([db, data], sort=True).drop_duplicates()

                        df = df.reset_index().set_index('date')
                        pd.DataFrame.sort_index(df, inplace=True)
                        df.to_pickle('./../DATA/OANDA') #UP-

                        ind.indicador(df)


        def run_daily(self):
                self._get_new_data()

                db = pd.read_pickle('./../DATA/OHLC_IND') #UP-
                db = db.dropna()
                db = db[db.index == db.index[-30]]

                ltm = db[db.asset.isin(trade_short)]
                ltm['strat'] = 'trade_short'

                ltp = db[db.asset.isin(trade_long)]
                ltp['strat'] = 'trade_long'

                trade = pd.concat([ltm, ltp])
                trade.index = trade.index + dt.timedelta(1)

                plano = {}
                total_try = []

                for i in range(len(trade)):
                        atr = trade.iloc[i].ATR
                        trading_hours = [900, 1800]
                        break_lunch = [1200, 1400]
                        profit = [5, 30, 100, 'day']
                        stop = [1, 30, 100, 'day']
                        duration = pd.to_datetime(120, unit='m').time()
                        try_qty = 3
                        strat_cond = 'and'

                        if trade.iloc[i].strat == 'trade_short':
                                direction = 'sell'
                                strat = {'strat1': 30, 'strat1': 5}
                                strat_name = 'trade_short'
                                try_qty = 3
                                strat_cond = 'and'

                        elif trade.iloc[i].strat == 'trade_long':
                                direction = 'buy'
                                strat = {'strat1': 30, 'strat1': 5}
                                strat_name = 'trade_long'
                                try_qty = 3
                                strat_cond = 'and'


                        else:
                                pass

                        total_try.append(try_qty)


                        plano.update({trade.iloc[i].asset+'_'+str(i):
                                        {'asset': trade.iloc[i].asset, 'direction': direction, 'size': 0, 'profit': profit, 'stop': stop, 'start': trading_hours[0], 'end': trading_hours[1], 'duration': duration, 'try_qty': try_qty,
                                        'strat': strat, 'strat_cond': strat_cond, 'strat_name': strat_name, 'atr': atr, 'break_start': break_lunch[0], 'break_end': break_lunch[1]}
                                        })

                if len(plano.keys()) > 0:
                        size = int(daily_risk / len(plano.keys())) #sum(total_try) #UPGRADED
                else:
                        size = 0

                for i in plano:
                        plano.get(i).update({
                                'size': size
                        })

                with open(f'./DATA/plan/plan_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}', 'wb') as file:
                        pickle.dump(plano, file, protocol=pickle.HIGHEST_PROTOCOL)


                