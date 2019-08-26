import ta 
import pandas as pd
from handle_data import handler

class indicators:
    
    def __init__(self):
        self.handle = handler

    def ATR(self, df, period = 14, multiplier = 1):
        """ ATR bands input ATR(time = 14, multiplier = 1)."""
        
        df.columns = map(str.lower, df.columns) #UPGRADE
        df.sort_index(inplace = True)
        df = df.reset_index() #UPGRADE
        z = ta.average_true_range(df['high'], df['low'], df['close'], period)
        z = z.iloc[-1]

        return float(z)*multiplier
  
    def MA(self, df, period = 14):
        
        df.columns = map(str.lower, df.columns) #UPGRADE
        df.sort_index(inplace = True)
        z = df.close.rolling(period).mean()
        z = z.iloc[-1]

        return float(z)

#################################################################################################

    def rsi(self, df, period): #UPGRADE

        df.columns = map(str.lower, df.columns) #UPGRADE
        df.sort_index(inplace = True)
        df = df.reset_index() #UPGRADE

        rsi_k = ta.stoch(df['high'], df['low'], df['close'], period)
        rsi_k = rsi_k.iloc[-1]
        rsi_d = ta.stoch_signal(df['high'], df['low'], df['close'], period, 3)
        rsi_d = rsi_d.iloc[-1]

        return rsi_d, rsi_k

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


