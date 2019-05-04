import datetime

from pyalgotrade import dataseries, bar
from pyalgotrade.strategy.custom import LiveStrategy, OptimizerBacktestStrategy, SimpleBacktestStrategy

IS_LIVE = False
IS_TESTNET = True
IS_OPTIMIZATION = False
IS_TICK = False
IS_LONG = True
FREQUENCY = bar.Frequency.MINUTE * 20
POSITION_SIZE = 10
CASH = 1000
STRATEGY_ID = 'breakout_7_1min_long_1'

TELEGRAM_TOKEN = ''
API_KEY = "7rcEZRK1lpWsqibs6AnYnyje"
API_SECRET = "cIESaM4yDHuS82QqaOGzjgwGfs4henlFdizR0bOwPcGbdsPd"
SYMBOL = ["XBTUSD"]

START_DATE = datetime.datetime(2018, 12, 20)
END_DATE = datetime.datetime(2019, 1, 20)

lb = 1
i = -1

base_strategy = LiveStrategy if IS_LIVE else OptimizerBacktestStrategy if IS_OPTIMIZATION else SimpleBacktestStrategy
from algos.breakout.algo_breakout import backtest, bitmex_run, BreakoutLong

strategyClass = BreakoutLong if IS_LONG else BreakoutLong
dataseries.DEFAULT_MAX_LEN = 100 if IS_OPTIMIZATION else lb * 3

if __name__ == "__main__":
    if IS_LIVE:
        bitmex_run(strategyClass, IS_TESTNET, STRATEGY_ID, TELEGRAM_TOKEN, API_KEY, API_SECRET, CASH, IS_OPTIMIZATION,
                   IS_TICK,
                   START_DATE,
                   END_DATE, SYMBOL, FREQUENCY, POSITION_SIZE, lb, i)
    else:
        backtest(strategyClass, IS_OPTIMIZATION, IS_TICK, START_DATE, END_DATE, SYMBOL, FREQUENCY, POSITION_SIZE, lb, i)
