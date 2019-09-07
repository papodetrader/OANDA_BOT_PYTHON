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

import logging
logging.basicConfig( filename= (f"./DATA/log/execution_{dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).date()}.log"),
                     filemode='w',
                     level=logging.ERROR,
                     format= '%(asctime)s - %(levelname)s - %(message)s',
                     datefmt= "%Y-%m-%d %H:%M:%S"
                   )


class trading_execution():

    def __init__ (self, plan, size_lt, trades, orders):
        self.orders = orders
        self.trades = trades
        self.size_lt = size_lt
        self.plan = plan

        self.handle = handler()
        self.strat = strategy(self.plan)
        self.ind = indicators()



    def current_time(self):
        x = dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).hour * 100 + dt.datetime.utcnow().minute
        return x



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



    def add_log(self, i):
        
        history = self.handle.history(self.orders.get(i)['tradeID'])['trade']

        try:
            close_time = pd.to_datetime(str((int(history['closeTime'].split('T')[1][0:2]) + 3)) +':'+ history['closeTime'].split('T')[1][3:5]).time()
        except Exception as e:
            logging.error(str(e) + f' error on add_log for {self.orders.get(i)} \n')
            close_time = dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()

        self.trades.update({self.orders.get(i)['tradeID']:{
            'date': self.orders.get(i)['date'],
            'margin': self.orders.get(i)['margin'],
            'entry_price': self.orders.get(i)['entry_price'],
            'qty': self.orders.get(i)['qty'],
            'stop': self.orders.get(i)['stop'],
            'target': self.orders.get(i)['target'],
            'entry_time': self.orders.get(i)['entry_time'],

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
            'closingID': history.get('closingTransactionIDs')
            }})

        pd.to_pickle(self.trades, f'./DATA/trades/trades_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')

        print(pd.DataFrame(self.trades.values(), self.trades.keys())[['asset', 'close_time', 'direction',
                                        'margin', 'plan_key', 'entry_price', 'qty', 'realizedPL',
                                        'status', 'stop', 'strat', 'target', 'entry_time', 'close_price']])

        
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
                self.size_lt.pop(self.orders.get(i)['asset'])
                self.orders.pop(i)
            pd.to_pickle(self.orders, f'./DATA/orders/orders_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')


        
        print(pd.DataFrame(self.trades.values(), self.trades.keys())[['asset', 'close_time', 'direction',
                                        'margin', 'plan_key', 'entry_price', 'qty', 'realizedPL',
                                        'status', 'stop', 'strat', 'target', 'entry_time', 'close_price']])

        print(f'\n Order Dictionary -> {pd.DataFrame(self.orders.values(), self.orders.keys())}')

        print(f'\n Open Positions -> {self.handle.positions()}')

        now = pd.to_timedelta(str(dt.datetime.now(tz=pytz.timezone('Europe/Moscow')).time()))
        next_day = dt.timedelta(hours=23, minutes=59, seconds=59)
        total_wait = next_day - now
        total_wait = (int(str(total_wait).split(' ')[2].split(':')[0]) * 3600) + (int(str(total_wait).split(' ')[2].split(':')[1]) * 60) + 60

        print(f'\n !!! Daily Risk - Reward achieved or End of Day and will resume in {total_wait}!!!')    

        time.sleep(total_wait)

        # import os
        # os._exit(0)



    def day_mgt(self):
        
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
                    self.orders.get(i).update({
                                'unrealizedPL': round(float(self.handle.history(self.orders.get(i)['tradeID'])['trade'].get('unrealizedPL')),2),
                            })

                    pd.to_pickle(self.orders, f'./DATA/orders/orders_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')

            if len(lt) > 0:
                for i in lt:
                    self.change_start(i)
                    self.add_log(i)
                    self.size_lt.pop(i)
                    self.orders.pop(i)
                pd.to_pickle(self.orders, f'./DATA/orders/orders_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')

            print('\n', pd.DataFrame(self.orders.values(), self.orders.keys()))


        if self.trades == {}:
            log_pl = 0
        else:
            log_pl = sum(pd.DataFrame(self.trades.values(), self.trades.keys())['realizedPL'])

            print('\n', pd.DataFrame(self.trades.values(), self.trades.keys())[['asset', 'close_time', 'direction',
                                                                    'margin', 'plan_key', 'entry_price', 'qty', 'realizedPL',
                                                                    'status', 'stop', 'strat', 'target', 'entry_time', 'close_price']])

        if (orders_pl + log_pl) < (-1 * daily_risk):
            self.close_all()

        elif (orders_pl + log_pl) > (3 * daily_risk):
            self.close_all()

        

    def exit_calc(self, curr, id, type='day'):

        if type == 'day':
            target = (self.plan[id]['profit'][0] / 10) * self.plan[id]['atr']
            stop_price = (self.plan[id]['stop'][0] / 10) * self.plan[id]['atr']

        else:
            target_df = self.handle.candle_data(curr, self.plan[id]['profit'][1], self.plan[id]['profit'][2] + 1 )
            target = self.ind.ATR(target_df, self.plan[id]['profit'][2], self.plan[id]['profit'][0])

            stop_df = self.handle.candle_data(curr, self.plan[id]['stop'][1], self.plan[id]['stop'][2] + 1 )
            stop_price = self.ind.ATR(stop_df, self.plan[id]['stop'][2], self.plan[id]['stop'][0])

        return target, stop_price



    def condition(self, id, curr):

        if ((self.plan[id]['try_qty'] >= 1) and 
            ((self.current_time() > self.plan[id]['start'] and self.current_time() < self.plan[id]['break_start']) 
            or (self.current_time() < self.plan[id]['end'] and self.current_time() > self.plan[id]['break_end'])) and 
            self.strat.master(id, self.plan[id]['strat_cond']) and (id not in self.orders.keys())): 

            target, stop_price = self.exit_calc(curr, id, self.plan[id]['profit'][3])
            current_price =  self.handle.candle_data(curr, 1, 1).close.values[0]
            digits = self.handle.account_instruments(curr)
            size = int(self.handle.std_curr(curr) * (self.plan[id]['size'] / stop_price))
            direction = self.plan[id]['direction']

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
                return self.order_execution(curr, direction, size, target, stop, id, current_price, digits)
        


    def order_execution(self, curr, direction, size, target, stop, id, current_price, digits):
        if direction == 'sell':
            size = -size

        order = self.handle.order(curr, size, target, stop)

        if 'tradeOpened' in order.get('orderFillTransaction').keys():
            if order.get('orderFillTransaction')['tradeOpened']['tradeID'] in self.handle.positions():
                size = self.handle.history(order.get('orderFillTransaction')['tradeOpened']['tradeID'])['trade']['currentUnits']
                self.size_lt.update({id : [curr, str(abs(int(size)))]})

                print(f"{id} {direction} {curr} at price: {round(current_price, digits)} , target: {round(target, digits)}, stop: {round(stop, digits)}, size: {size}")

                return self.order_process(order, id, curr, size)
            


    def order_process(self, order, id, curr, size):

        self.plan.get(id).update(
        {
            'try_qty': self.plan[id]['try_qty'] - 1
            }
            )

        self.orders.update({id:{
            'asset': curr,
            'date': pd.to_datetime(order.get('orderFillTransaction').get('time').split('T')[0]).date(),
            'entry_time': pd.to_datetime((str(int(order.get('orderFillTransaction').get('time').split('T')[1][0:2]) + 3)) +':'+ order.get('orderFillTransaction').get('time').split('T')[1][3:5]).time(),
            'tradeID': order.get('relatedTransactionIDs')[1],
            'entry_price': order.get('orderFillTransaction').get('price'),
            'qty': size,
            'target': order.get('orderCreateTransaction').get('takeProfitOnFill')['price'],
            'stop': order.get('orderCreateTransaction').get('stopLossOnFill')['price'],
            'margin': order.get('orderFillTransaction').get('tradeOpened')['initialMarginRequired'],
            'unrealizedPL': 0,
        }})

        pd.to_pickle(self.orders, f'./DATA/orders/orders_{dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date()}')

        chart(self.plan, id, curr, (self.current_time()+100), dt.datetime.now(tz=pytz.timezone("Europe/Moscow")).date())

