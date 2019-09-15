import ta 
import pandas as pd
import numpy as np
from handle_data import handler

class indicators:
    
    def __init__(self):
        self.handle = handler()

    def ATR(self, df, period = 14, multiplier = 1):
        """ ATR bands input ATR(time = 14, multiplier = 1)."""
        
        df.columns = map(str.lower, df.columns) 
        df.sort_index(inplace = True)
        df = df.reset_index() 
        z = ta.average_true_range(df['high'], df['low'], df['close'], period)
        z = z.iloc[-1]

        return float(z)*multiplier
  
    def MA(self, df, period = 14):
        
        df.columns = map(str.lower, df.columns) 
        df.sort_index(inplace = True)
        z = df.close.rolling(period).mean()
        z = z.iloc[-1]

        return round(float(z), 5)

    def candlestick(self, data):
        
        data.columns = map(str.lower, data.columns) 

        df = {}
        for i in range(len(data.index)):
            if (((data.open.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'HD')}) 
            elif (data.high.iloc[i] < data.high.iloc[i-1]) and (data.low.iloc[i] > data.low.iloc[i-1]) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.20) and (data.close.iloc[i] > data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Hp')})
            elif (data.high.iloc[i] > data.high.iloc[i-1]) and (data.low.iloc[i] < data.low.iloc[i-1]) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (data.close.iloc[i] > data.open.iloc[i]): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'Ep')})
            elif (((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.75):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Mp')})
            elif (1-((data.high.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'GS')}) 
            elif (data.high.iloc[i] < data.high.iloc[i-1]) and (data.low.iloc[i] > data.low.iloc[i-1]) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.20) and (data.close.iloc[i] < data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Hm')})
            elif (data.high.iloc[i] > data.high.iloc[i-1]) and (data.low.iloc[i] < data.low.iloc[i-1]) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (data.close.iloc[i] < data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Em')})
            elif (((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < -0.67) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'Mm')})
        
            else:
                df.update ({i: (data.index[i], data.asset[i], 'X')})    
        

        df = pd.DataFrame(df)
        
        return df.T.iloc[-2][2]


#################################################################################################


    def rsi(self, df, period): 

        df.columns = map(str.lower, df.columns) 
        df.sort_index(inplace = True)
        df = df.reset_index() 

        rsi_k = ta.stoch(df['high'], df['low'], df['close'], period)
        rsi_k = rsi_k.iloc[-1]
        rsi_d = ta.stoch_signal(df['high'], df['low'], df['close'], period, 3)
        rsi_d = rsi_d.iloc[-1]

        return int(rsi_d), int(rsi_k)


#################################################################################################


    def volatility_vs_spread(self, curr):

        target_df = self.handle.candle_data(curr, 30, 101)
        trinta = self.ATR(target_df, 100, 1)

        digits = self.handle.account_instruments(curr)

        spread = self.handle.calc_spread(curr)

        spread_adj = spread / int(str(1)+(digits-1)*str(0))

        df = self.handle.candle_data(curr, 1440, 101)
        day = self.ATR(df, 100, 1)


        return round(day / spread_adj, 2), round(trinta / spread_adj, 2), round(day / trinta, 2), round(day, digits), round(trinta, digits), round(spread_adj, digits)


#################################################################################################


    def channel(self, df, period=50):

        df.columns = map(str.lower, df.columns)
        df.sort_index(inplace = True)
        df = df.reset_index()

        if ta.donchian_channel_hband_indicator(df.close, period)[0] == 1.0:
            return 'long'

        elif ta.donchian_channel_lband_indicator(df.close, period)[0] == 1.0:
            return 'short'

        
        return 'none'


#################################################################################################


    def candlestick2(self, data):

        data.columns = map(str.lower, data.columns) 

        df = {}
        for i in range(len(data.index)):
            if (((data.open.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'HD')})
            elif (data.high.iloc[i] < data.high.iloc[i-1]) and (data.low.iloc[i] > data.low.iloc[i-1]) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.20) and (data.close.iloc[i] > data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Hp')})
            elif (data.high.iloc[i] > data.high.iloc[i-1]) and (data.low.iloc[i] < data.low.iloc[i-1]) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (data.close.iloc[i] > data.open.iloc[i]): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'Ep')})
            elif (((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.67) and (((data.close.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.75): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'Mp')})
            elif (1-((data.high.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'GS')}) 
            elif (data.high.iloc[i] < data.high.iloc[i-1]) and (data.low.iloc[i] > data.low.iloc[i-1]) and (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.20) and (data.close.iloc[i] < data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Hm')})
            elif (data.high.iloc[i] > data.high.iloc[i-1]) and (data.low.iloc[i] < data.low.iloc[i-1]) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.33) and (data.close.iloc[i] < data.open.iloc[i]):
                df.update ({i: (data.index[i].date(), data.asset[i], 'Em')})
            elif (((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < -0.67) and (1-((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.25): 
                df.update ({i: (data.index[i].date(), data.asset[i], 'Mm')})
            elif (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.15) and (((data.open.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.30) and (((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.30):                    
                df.update ({i: (data.index[i], data.asset[i], 'Dm')})           
            elif (abs((data.close.iloc[i] - data.open.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) < 0.15) and (((data.open.iloc[i] - data.low.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.30) and (((data.high.iloc[i] - data.close.iloc[i]) / (data.high.iloc[i] - data.low.iloc[i])) > 0.30):
                df.update ({i: (data.index[i], data.asset[i], 'Dp')})
            else:
                df.update ({i: (data.index[i], data.asset[i], 'X')}) 
                

        
        df = pd.DataFrame(df)
        
        return df.T.iloc[-2][2]

#################################################################################################

    def correlation(self, df, symbol, symbol_corr=list()):
        
        list_corr = {}

        df.index.names = ['date']

        for i in symbol_corr:
            df1 = set(df[df.asset == symbol].index.unique())
            df2 = set(df[df.asset == i].index.unique())

            date_corr = df1.intersection(df2)

            df = df[df.index.isin(date_corr)]

            a = df[df.asset == symbol].reset_index()
            b = df[df.asset == i].reset_index()

            pd.DataFrame.drop_duplicates(a, 'date', inplace=True)
            pd.DataFrame.drop_duplicates(b, 'date', inplace=True)

            a = list(a.close)
            b = list(b.close)

            corr = round(np.corrcoef(a, b)[0, 1], 2)

            list_corr.update({(symbol, i): {'corr': corr}})

        df = pd.DataFrame(list_corr.values(), list_corr.keys())

        return df

#################################################################################################

    def apply_force(self, symbol='EUR_USD', time=1440, period=2):
        
        fx_list = ['EUR_USD', 'EUR_JPY', 'EUR_GBP', 'EUR_AUD', 'EUR_CAD',
                    'USD_JPY', 'GBP_USD', 'AUD_USD', 'USD_CAD', 
                    'GBP_JPY', 'AUD_JPY', 'CAD_JPY',
                    'GBP_AUD', 'GBP_CAD']

        if symbol in fx_list:
            func_list = [i for i in fx_list if symbol in i]
        else:
            func_list = ['FR40_EUR', 'DE30_EUR', 'UK100_GBP', 'HK33_HKD', 'JP225_USD', 'SPX500_USD', 'NAS100_USD', 'AU200_AUD', 'US2000_USD']

        lt = []

        for i in func_list:

            df = self.handle.candle_data(i, time, period)
            i = i.split('_')[0] + i.split('_')[1] 

            if df.iloc[-1].close > df.iloc[-2].close and df.iloc[-1].close > df.iloc[-1].open:

                if symbol not in fx_list:
                    lt.append(1)
                elif i[3:] == symbol[3:]:
                    lt.append(-1)
                elif i[:3] == symbol[:3]:
                    lt.append(1)

            else:
                if symbol not in fx_list:
                    lt.append(-1)
                if i[3:] == symbol[3:]:
                    lt.append(1)
                elif i[:3] == symbol[:3]:
                    lt.append(-1)


        total = len([i for i in lt if i > 0]) / len(lt)

        return round(total, 2)

#################################################################################################

    def trend(self, df, period= 20):
    
        df['CH_HIGH'] = ta.donchian_channel_hband(df.close, period) - ta.donchian_channel_hband(df.close, period).shift(1)
        df['CH_LOW'] = ta.donchian_channel_lband(df.close, period) - ta.donchian_channel_lband(df.close, period).shift(1)

        high = df[df['CH_HIGH'] > 0].index[-1]
        low = df[df['CH_LOW'] < 0].index[-1]

        if high > low:
            return 'long'
        else:
            return 'short'

        return 'none'


