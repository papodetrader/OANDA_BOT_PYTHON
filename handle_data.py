from dateutil.parser import parse
import pandas as pd
from oandapyV20 import API
import oandapyV20.endpoints.trades as trades
import oandapyV20.endpoints.instruments as instruments
from user_data import *
import oandapyV20
from oandapyV20.contrib.requests import MarketOrderRequest, LimitOrderRequest
from oandapyV20.contrib.requests import TakeProfitDetails, StopLossDetails
import oandapyV20.endpoints.orders as orders
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.positions as positions
import oandapyV20.endpoints.forexlabs as labs
import datetime as dt
import pytz

api = API(access_token=ACCESS_TOKEN, environment=ENVIRONMENT)
r = trades.TradesList(ACCOUNT_ID)

class handler:
    
    def __init__(self):
        pass
    


    def getGranularity(self, size_in_minutes):
            if size_in_minutes == 1:
                return "M1"
            elif size_in_minutes == 2:
                return "M2"
            elif size_in_minutes == 3:
                return "M3"
            elif size_in_minutes == 4:
                return "M4"
            elif size_in_minutes == 5:
                return "M5"
            elif size_in_minutes == 10:
                return "M10"
            elif size_in_minutes == 15:
                return "M15"
            elif size_in_minutes == 30:
                return "M30"
            elif size_in_minutes == 60:
                return "H1"
            elif size_in_minutes == 120:
                return "H2"
            elif size_in_minutes == 180:
                return "H3"
            elif size_in_minutes == 240:
                return "H4"
            elif size_in_minutes == 480:
                return "H8"
            elif size_in_minutes == 1440:
                return "D"
            elif size_in_minutes == 10080:
                return "W"
            elif size_in_minutes == 43800:
                return "M"
            # default: two hour candles
            return "H2"



    def candle_data(self, symbol, size_in_minutes, count, from_dt='', to_dt= dt.datetime.now() - dt.timedelta(hours=3), use=''):
        """ Candle data for symbols """
        
        to_dt = str(to_dt.date()) + 'T' + str(to_dt.time()) + 'Z'
    
        params = {
            "granularity": self.getGranularity(size_in_minutes),
            "to": to_dt,
            # 'alignmentTimezone': pytz.timezone('Europe/Moscow')
        }

        if from_dt != '':
            from_dt = str(from_dt.date()) + 'T' + str(from_dt.time()) + 'Z'
            params.update({'from': from_dt})
        elif from_dt == '':
            params.update({'count': count})


        r = instruments.InstrumentsCandles(instrument=symbol, params=params)

        data = api.request(r)

        clean = [{'time':i['time'],"open":i['mid']['o'],
                "close":i['mid']['c'],'high':i['mid']['h'],
                'low':i['mid']['l']} for i in data['candles']]

        df = pd.DataFrame(clean)
        df.set_index('time',inplace=True)

        if use == 'plan':
            df.index = [ parse(i).strftime('%Y-%m-%d') for i in df.index ] 
        else:
            df.index = [ parse(i).strftime('%Y-%m-%d %H:%M:%S') for i in df.index ]

        df.index = pd.to_datetime(df.index)

        df.index = pd.to_datetime(df.index)
        df[df.columns] = df[df.columns].apply(pd.to_numeric)

        df['asset'] = symbol

        return df


    
    def order(self, symbol, size, target, stop, type='mkt'):
        
        mktOrderLong = MarketOrderRequest(symbol,
                      units= size,
                      takeProfitOnFill= TakeProfitDetails(price=target).data,
                      stopLossOnFill=StopLossDetails(price=stop).data)

        lmtOrderLong = LimitOrderRequest(symbol,
                      units= size,
                      price= round(target * 1.05, self.account_instruments(symbol)),
                      takeProfitOnFill= TakeProfitDetails(price=target).data,
                      stopLossOnFill=StopLossDetails(price=stop).data)

        if type == 'lmt':
            r = orders.OrderCreate(ACCOUNT_ID, lmtOrderLong.data)
        else:
            r = orders.OrderCreate(ACCOUNT_ID, mktOrderLong.data)

        data = api.request(r)
        
        return data


    
    def read_positions(self):
        orders = {}
        count = 0

        for i in self.positions():
            position = self.history(i)['trade']
            orders.update({f'{position["instrument"]}_{pd.to_datetime(position.get("openTime").split("T")[0]).date()}_{count}': {

                'asset': position['instrument'],
                'date': pd.to_datetime(position.get('openTime').split('T')[0]).date(),
                'entry_time': pd.to_datetime((str(int(position.get('openTime').split('T')[1][0:2]) + 3)) +':'+ position.get('openTime').split('T')[1][3:5]).time(),
                'tradeID': position['id'],
                'entry_price': position['price'],
                'qty': position['currentUnits'],
                'target': position['takeProfitOrder']['price'],
                'stop': position['stopLossOrder']['price'],
                'margin': position['initialMarginRequired'],
                'unrealizedPL': float(position['unrealizedPL']),
            }})

            count =+ count + 1

        return orders



    def history(self, tradeid):
        r = trades.TradeDetails(ACCOUNT_ID, tradeid)
        rv = api.request(r)

        return rv



    def account_details(self):
        
        r = accounts.AccountDetails(ACCOUNT_ID)
        rv = api.request(r)
        details = rv.get('account')
        
        return details



    def account_instruments(self, asset, option='digits'):
        r = accounts.AccountInstruments(ACCOUNT_ID, params={'instruments': asset})
        
        if option == 'digits':
            try:
                rv = api.request(r)['instruments'][0]['displayPrecision']
            except:
                rv = min(len(str(self.candle_data(asset, 1, 1).iloc[0].close).split('.')[1]), 5)
            
        # rv = api.request(r)['instruments'] #to_find solutions
    
        return rv


    
    def positions(self):
        
        r = positions.PositionList(ACCOUNT_ID)
        position = []

        position.append([i['short'].get('tradeIDs') for i in api.request(r)['positions'] if i['short'].get('tradeIDs') is not None])
        position.append([i['long'].get('tradeIDs') for i in api.request(r)['positions'] if i['long'].get('tradeIDs') is not None])
        position = [item for sublist in position for item in sublist]
        position = [item for sublist in position for item in sublist]

        return position


    
    def close_position(self, symbol, side, qty):
        
        if side == "buy":
            data = {'longUnits': qty}
            
        elif side == "sell":
            data = {'shortUnits': qty}
            
        r = positions.PositionClose(accountID=ACCOUNT_ID,
                             instrument=symbol,
                             data=data)
        data = api.request(r)
        
        return data


    
    def close_all_positions(self):
        
        if len(self.positions()) > 0:
            for id in self.positions():
                if int(self.history(id)['trade']['currentUnits']) > 0:
                    direction = 'buy'
                else:
                    direction = 'sell'
                
                self.close_position(self.history(id)['trade']['instrument'], direction, str(abs(int(self.history(id)['trade']['currentUnits']))))
                
        print("\n closed all positions \n")



    def calc_spread(self, asset, number=70000):

        params = {
            "instrument": asset,
            "period":  number,
        }

        r = labs.Spreads(params)

        data = [i[1] for i in api.request(r).get('avg')]

        return sum(data) / len(data)



    def std_curr(self, asset):

        if asset.split('_')[1] == 'USD':
            return 1
        elif asset.split('_')[1] in ['AUD', 'GBP', 'EUR']:
            asset = asset.split('_')[1] + '_' + 'USD'
            return self.candle_data(asset, 1, 2).iloc[0].close
        else:
            asset = 'USD' + '_' + asset.split('_')[1]
            return self.candle_data(asset, 1, 2).iloc[0].close

