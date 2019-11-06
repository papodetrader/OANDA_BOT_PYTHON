from execution import trading_execution
from plan import build_plan, capital, daily_risk
from calendario import calendar
from variable import execution, plan, read_variables
import os
import time
import datetime as dt
import pandas as pd
import pickle
import pytz
import warnings
warnings.filterwarnings("ignore")

start_capital = capital

calendar = calendar()
print('\n', calendar[0])
print('\n', calendar[1], '\n')


while True:

    starttime = time.time()
    dt_date = dt.datetime.now(tz=pytz.timezone('Europe/Moscow'))

    if dt_date.weekday() in [0, 1, 2, 3, 4]:

        if execution.current_time() > 000 and execution.current_time() < 100:
            new_plan = (f'plan_{dt_date.date()}')
            if new_plan not in os.listdir('./DATA/plan/'):            
                build_plan()._get_new_data() #.run_daily()
                execution.trades = {}
                execution.orders = {}
                read_variables()
                plan = pd.read_pickle(f'./DATA/plan/{new_plan}')
                print(plan)

        elif execution.current_time() > max(pd.DataFrame(plan.values(), plan.keys()).end)+300 or execution.current_time() > 2200:
            execution.close_all()

        elif dt_date.minute == 4:

            try:
                balance = round(float(execution.handle.account_details()['NAV']), 2)

                if (start_capital - daily_risk) > balance:
                    execution.close_all()

                print('Financial Result Today: ', round(balance - start_capital), 2)

            except Exception as e:
                print(str(e))

        else:
            for id in plan: 
                curr = plan[id]['asset']
                
                execution.condition(id, curr)
            
            execution.day_mgt()

        print(f'stopped for {int((60.0 - ((time.time() - starttime) % 60.0)))} seconds at {dt_date.strftime("%H:%M")} \n')
        time.sleep(60.0 - ((time.time() - starttime) % 60.0))

    else:
        print(f'stopped for {int((3600.1 - ((time.time() - starttime) % 60.0)))} seconds at {dt_date.strftime("%H:%M")} \n')
        time.sleep(3600.1 - ((time.time() - starttime) % 60.0))

