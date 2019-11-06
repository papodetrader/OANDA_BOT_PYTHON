### THIS WILL NOT BE UPDATED ANY FURTHER - IS WORKING WITHOUT ANY MAJOR ISSUE AS KNOWLEDGED ###

# OANDA_BOT_PYTHON

strategy & indicat files most be setup by the user. There is some stuff there but just as sample of it.

The plan can be also setup easy with a dictionary as:

plan = {
    'EUR_USD_0': {
        atr = 0.00750
        break_lunch = [1100, 1500]
        trading_hours = [900, 1800]
        profit = [5, 30, 100, 'day']
        stop = [1.5, 30, 100, 'day']
        duration = pd.to_datetime(30, unit='m').time()
        try_qty = 3
        direction = 'sell'
        strat = {'strat5': 3}
        strat_cond = 'and'
        strat_name = 'trade_short'
        size = 100
    }
}


Anyone can add conditions to the strategy and indicators to run any new strategy.

Some links below of videos on Youtube I did to explain it... in Portuguese!

https://www.youtube.com/watch?v=rrOPu4SXv9w

https://www.youtube.com/watch?v=24IrbNi5bKo

https://www.youtube.com/watch?v=SCn7l_6FuWE
