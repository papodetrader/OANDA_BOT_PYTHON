import pandas as pd
from user_data import *
from plan import daily_risk
from handle_data import handler
import time
from indicat import indicators
from strategy import strategy
import pytz
import datetime as dt
import pickle
from chart import chart
from calendario import cal_list


import logging
logging.basicConfig( filename= (f"./execution.log"),
                     filemode='w',
                     level=logging.ERROR,
                     format= '%(asctime)s - %(levelname)s - %(message)s',
                     datefmt= "%Y-%m-%d %H:%M:%S"
                   )


class trading_execution():

    def __init__ (self, plan, size_lt, trades, orders, x):
        self.orders = orders
        self.trades = trades
        self.size_lt = size_lt
        self.plan = plan
        self.x = x
        self.intraday = self.first_data()
        self.asset_info = self.info()

        self.handle = handler()
        self.strat = strategy(self.plan)
        self.ind = indicators()



    def current_time(self):
        x = dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).hour * 100 + dt.datetime.utcnow().minute
        return x


    def info(self):
    
        plan = pd.DataFrame(self.plan.values(), self.plan.keys())
        db = {}

        for i in plan.asset.unique():
            db.update({i: {'digits': handler().account_instruments(i)}})

        print(db)
        return db



    def time_to_minutes(self, time_std):
        hour_to_minute = time_std.hour * 60
        
        return hour_to_minute + time_std.minute



    def change_start(self, i):
        waiting = min([self.plan.get(i)['strat'].get(ii) for ii in self.plan.get(i)['strat'].keys()])

        self.plan.get(i).update({
            'start': int(
                str(pd.to_datetime((self.time_to_minutes(dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()) + waiting), unit='m').time().hour) +
                str(pd.to_datetime((self.time_to_minutes(dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()) + waiting), unit='m').time().minute)
                )
            })



    def check_duration(self, i):
        
        order_time = self.time_to_minutes(self.orders.get(i)['entry_time']) 
        duration = self.time_to_minutes(self.plan.get(i)['duration']) 

        if (order_time + duration) < self.time_to_minutes(dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()):
            return True
        else:
            return False


    def first_data(self):

        ''' the first call-load of intraday database to save on future calls to broker for data 
            it is important remember that the qty of data requested most fit the needed for all indicators
        '''

        y = pd.DataFrame()

        for i, ii in self.x:
            temp = handler().candle_data(i, ii, 205).iloc[:-1]
            temp['tf'] = ii
            y = pd.concat([y, temp], sort=True)

        return y

    
    def database(self, asset):

        timenow = dt.datetime.now(tz=pytz.timezone("Europe/Moscow")) - dt.timedelta(hours=3)
        df = pd.DataFrame()
        y = pd.DataFrame() #

        for i, ii in self.x:
            if i == asset:

                last = self.intraday[(self.intraday.asset == i) & (self.intraday.tf == ii)].index.unique()[-1]

                if (self.time_to_minutes(timenow) - self.time_to_minutes(last)) >= (2* ii):
                    df = self.handle.candle_data(i, ii, 205, last, timenow).iloc[:-1]
                    df['tf'] = ii

                    df = df.reset_index().rename({'index': 'date'}, axis=1).set_index(['date', 'asset', 'tf'])

                    self.intraday = self.intraday.reset_index().rename({'index': 'date'}, axis=1).set_index(['date', 'asset', 'tf'])
                    self.intraday = pd.concat([self.intraday, df], sort=True).sort_index().drop_duplicates(keep='last')
                    self.intraday = self.intraday.reset_index().set_index('date')   

                x = self.intraday[(self.intraday.asset == i) & (self.intraday.tf == ii)].iloc[-205:] #
                y = pd.concat([y, x]) #
                

        self.intraday = self.intraday[~(self.intraday.asset == y.asset.unique()[0])] #
        self.intraday = pd.concat([self.intraday, y]) #
        

        return self.intraday



    def add_log(self, i):

        try:
      
            history = self.handle.history(self.orders.get(i)['tradeID'])['trade']

            close_time = pd.to_datetime(str((int(history['closeTime'].split('T')[1][0:2]) + 3)) +':'+ history['closeTime'].split('T')[1][3:5]).time()
            close_date = pd.to_datetime(history['closeTime'].split('T')[0])

            self.trades.update({self.orders.get(i)['tradeID']:{
                'entry_date': self.orders.get(i)['date'],
                'margin': self.orders.get(i)['margin'],
                'entry_price': self.orders.get(i)['entry_price'],
                'qty': self.orders.get(i)['qty'],
                'stop': self.orders.get(i)['stop'],
                'target': self.orders.get(i)['target'],
                'entry_time': self.orders.get(i)['entry_time'],
                'intraday_strat': self.orders.get(i)['intraday_strat'],
                'events': self.orders.get(i)['events'],
                'others': self.orders.get(i)['others'],

                'plan_key': i,
                'asset': self.plan.get(i)['asset'],
                'strat': self.plan.get(i)['strat'],
                'exit_profit': self.plan.get(i)['profit'],
                'exit_stop': self.plan.get(i)['stop'],
                'direction': self.plan.get(i)['direction'],
                'strat_cond': self.plan.get(i)['strat_cond'],
                'strat_name': self.plan.get(i)['strat_name'],

                'close_price': history.get('averageClosePrice'),
                'status': history.get('state'),
                'realizedPL': round(float(history.get('realizedPL')),2),
                'close_time': close_time,
                'close_date': close_date,
                'closingID': history.get('closingTransactionIDs')
                }})

            pd.to_pickle(self.trades, f'./DATA/trades/trades_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')


            print('\n', pd.DataFrame(self.trades.values(), self.trades.keys())[['plan_key', 'asset', 'entry_date', 'entry_price',
                                                                        'close_price', 'entry_time', 'close_time', 'qty', 
                                                                        'realizedPL']])

        except Exception as e:
            print(str(e) + ' on execution.add_log()')
            logging.ERROR(str(e) + ' on execution.add_log()')
            print(self.orders.get(i))
            self.orders.pop(i)
            pass


        
    def close_all(self):

        lt = []

        if len(self.orders.keys()) > 0:

            for i in self.orders.keys():

                if self.orders.get(i)['tradeID'] in self.handle.positions():

                    trade = self.handle.close_position(self.plan[i]['asset'], self.plan[i]['direction'], str(abs(int(self.orders.get(i)['qty']))))

                    if 'longOrderFillTransaction' or 'shortOrderFillTransaction' in trade.keys():
                        lt.append(i)    


        if len(lt) > 0:
            for i in lt:
                self.add_log(i)
                self.size_lt.pop(i)
                self.orders.pop(i)
            pd.to_pickle(self.orders, f'.orders')


        
        print('\n', pd.DataFrame(self.trades.values(), self.trades.keys())[['plan_key', 'asset', 'entry_date', 'entry_price',
                                                                    'close_price', 'entry_time', 'close_time', 'qty', 
                                                                    'realizedPL']])

        print(f'\n Order Dictionary -> {pd.DataFrame(self.orders.values(), self.orders.keys())}')

        print(f'\n Open Positions -> {self.handle.positions()}')

        now = pd.to_timedelta(str(dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()))
        next_day = dt.timedelta(hours=23, minutes=59, seconds=59)
        total_wait = next_day - now
        total_wait = (int(str(total_wait).split(' ')[2].split(':')[0]) * 3600) + (int(str(total_wait).split(' ')[2].split(':')[1]) * 60) + 60

        print(f'\n !!! Daily Risk - Reward achieved or End of Day and will resume in {int(total_wait / 60)} minutes !!!')    

        time.sleep(total_wait)



    def day_mgt(self):

        try:
            
            if self.orders == {}:
                orders_pl = 0
            else:
                orders_pl = sum(pd.DataFrame(self.orders.values(), self.orders.keys())['unrealizedPL'])

                lt = []

                for i in self.orders.keys():
                    
                    if self.orders.get(i)['tradeID'] not in self.handle.positions():

                        lt.append(i)

                    elif self.check_duration(i):
                        self.orders.get(i).update({ 
                            'qty': self.handle.history(self.orders.get(i)['tradeID'])['trade']['currentUnits']
                        })
                        self.size_lt.update({i: self.handle.history(self.orders.get(i)['tradeID'])['trade']['currentUnits']})

                        trade = self.handle.close_position(self.plan[i]['asset'], self.plan[i]['direction'], str(abs(int(self.orders.get(i)['qty']))))
                        
                        if 'longOrderFillTransaction' or 'shortOrderFillTransaction' in trade.keys():
                            lt.append(i)

                    else:
                        try:
                            self.orders.get(i).update({
                                        'unrealizedPL': round(float(self.handle.history(self.orders.get(i)['tradeID'])['trade'].get('unrealizedPL')),2),
                                    })
                        except Exception as e:
                            print(str(e) + ' on order.update(unrealized) at func day_mgt()')
                            logging.error(str(e) + ' on order.update(unrealized) at func day_mgt()')

                        pd.to_pickle(self.orders, f'./orders') #several save on loop, delay time but gain on safety


                if len(lt) > 0:
                    for i in lt:
                        self.change_start(i)
                        self.add_log(i)
                        self.size_lt.pop(i)
                        self.orders.pop(i)
                    pd.to_pickle(self.orders, f'./orders')


            if len(self.orders.keys()) > 0:
                print('\n', pd.DataFrame(self.orders.values(), self.orders.keys())[['tradeID', 'date', 'entry_time', 'qty', 
                                                                                'entry_price', 'stop', 'target', 'unrealizedPL']])


            if self.trades == {}:
                closed_pl = 0
            else:
                closed_pl = sum(pd.DataFrame(self.trades.values(), self.trades.keys())['realizedPL'])
                
                x = [i for i in self.trades.keys() if self.trades[i].get('entry_date') != dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).date()]
                if x == []:
                    pass
                else:
                    for i in x:
                        self.trades.pop(i)
                    pd.to_pickle(self.trades, f'./DATA/trades/trades_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')


            if len(self.trades.keys()) > 0:
                print('\n', pd.DataFrame(self.trades.values(), self.trades.keys())[['plan_key', 'asset', 'entry_date', 'entry_price',
                                                                                    'close_price', 'entry_time', 'close_time', 'qty', 
                                                                                    'realizedPL']])


            if (orders_pl + closed_pl) < (-1 * daily_risk):
                print(f'START CLOSE_ALL. Open Orders = {orders_pl}, Closed Orders = {closed_pl}, Daily Risk = {daily_risk}')

                self.close_all()

            elif (orders_pl + closed_pl) > (3 * daily_risk):
                print(f'START CLOSE_ALL. Open Orders = {orders_pl}, Closed Orders = {closed_pl}, Daily Risk = {daily_risk}')

                self.close_all()

        except Exception as e:
            print(str(e) + ' error on execution.day_mgt()')
            logging.error(str(e) + ' error on execution.day_mgt()')
            pass

        

    def exit_calc(self, curr, id, type='day'):

        if type == 'day':
            target = (self.plan[id]['profit'][0] / 10) * self.plan[id]['atr']
            stop_price = (self.plan[id]['stop'][0] / 10) * self.plan[id]['atr']

        else:
            # target_df = self.handle.candle_data(curr, self.plan[id]['profit'][1], self.plan[id]['profit'][2] + 1 )
            target_df = self.intraday[(self.intraday.asset == curr) & (self.intraday.tf == self.plan[id]['profit'][1])]
            target = self.ind.ATR(target_df, self.plan[id]['profit'][2], self.plan[id]['profit'][0])

            # stop_df = self.handle.candle_data(curr, self.plan[id]['stop'][1], self.plan[id]['stop'][2] + 1 )
            stop_df = self.intraday[(self.intraday.asset == curr) & (self.intraday.tf == self.plan[id]['profit'][1])]
            stop_price = self.ind.ATR(stop_df, self.plan[id]['stop'][2], self.plan[id]['stop'][0])

        return target, stop_price



    def condition(self, id, curr):

        try:
                
            if ((self.plan[id]['try_qty'] >= 1) and 
                ((self.current_time() > self.plan[id]['start'] and self.current_time() < self.plan[id]['end']) 
                and (self.current_time() < self.plan[id]['break_start'] or self.current_time() > self.plan[id]['break_end'])) and 
                (id not in self.orders.keys())): 

                df = self.database(self.plan[id]['asset'])
                df = df[df.asset == self.plan[id]['asset']]

                strat = self.strat.master(id, df, self.plan[id]['strat_cond'])


                if strat[0] == 'True':

                    target, stop_price = self.exit_calc(curr, id, self.plan[id]['profit'][3])
                    current_price =  self.handle.candle_data(curr, 1, 1).close.values[0]
                    digits = self.asset_info.get(curr)['digits']
                    size = int(self.handle.std_curr(curr) * (self.plan[id]['size'] / stop_price))
                    direction = self.plan[id]['direction']

                    others = {}


                    if direction == 'buy':

                        target = round(target + current_price, digits)
                        stop = round(current_price - stop_price, digits)

                    elif direction == 'sell':

                        target = round(current_price - target, digits)
                        stop = round(current_price + stop_price, digits)


                    if len(self.size_lt) > 0:
                        lt = [self.size_lt.get(i)[0] for i in self.size_lt.keys()]
                        if curr in lt:
                            values = [self.size_lt.get(i)[1] for i in self.size_lt.keys() if self.size_lt.get(i)[0] == curr]
                            if str(size) in values:
                                small = min([abs(int(i)) for i in values])
                                size = small -1
                    
                    if size >= 1:
                        return self.order_execution(curr, direction, size, target, stop, id, current_price, digits, strat[1], others)
        
        except Exception as e:
            print(str(e) + ' error on execution.condition()')
            logging.error(str(e) + ' error on execution.condition()')
            pass


    def order_execution(self, curr, direction, size, target, stop, id, current_price, digits, strat, others):
        if direction == 'sell':
            size = -size

        order = self.handle.order(curr, size, target, stop)

        if 'orderFillTransaction' in order.keys():
            if 'tradeOpened' in order.get('orderFillTransaction').keys():
                if order.get('orderFillTransaction')['tradeOpened']['tradeID'] in self.handle.positions():
                    size = self.handle.history(order.get('orderFillTransaction')['tradeOpened']['tradeID'])['trade']['currentUnits']
                    self.size_lt.update({id : [curr, str(abs(int(size)))]})

                    print(f"{id} {direction} {curr} at price: {round(current_price, digits)} , target: {round(target, digits)}, stop: {round(stop, digits)}, size: {size}")

                    return self.order_process(order, id, curr, size, strat, others)



    def order_process(self, order, id, curr, size, strat, others):

        self.plan.get(id).update(
        {
            'try_qty': self.plan[id]['try_qty'] - 1
            }
            )

        try:
            order_time = pd.to_datetime((str(int(order.get('orderFillTransaction').get('time').split('T')[1][0:2]) + 3)) +':'+ order.get('orderFillTransaction').get('time').split('T')[1][3:5]).time()
        except Exception as e:
            print(str(e) + ' on order_process()')
            logging.error(str(e))
            order_time = dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()
    
        self.orders.update({id:{

            'asset': curr,
            'date': pd.to_datetime(order.get('orderFillTransaction').get('time').split('T')[0]).date(),
            'entry_time': order_time,
            'tradeID': order.get('relatedTransactionIDs')[1],
            'entry_price': order.get('orderFillTransaction').get('price'),
            'qty': size,
            'target': order.get('orderCreateTransaction').get('takeProfitOnFill')['price'],
            'stop': order.get('orderCreateTransaction').get('stopLossOnFill')['price'],
            'margin': order.get('orderFillTransaction').get('tradeOpened')['initialMarginRequired'],
            'intraday_strat': strat,
            'events': cal_list(dt.datetime.now(tz=pytz.timezone("Europe/Moscow"))),
            'unrealizedPL': 0,
            'others': others,

        }})

        pd.to_pickle(self.orders, f'./orders')

        chart(self.plan, id, curr, self.intraday, (self.current_time()+100), dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date())
