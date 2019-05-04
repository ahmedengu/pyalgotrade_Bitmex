import __main__
import numpy as np

from pyalgotrade import bar
from pyalgotrade.barfeed import csvfeed
from pyalgotrade.barfeed.csvfeed import GenericBarFeed
from pyalgotrade.bitcoincharts import barfeed as bitbf
from pyalgotrade.bitmex import barfeed
from pyalgotrade.bitmex import broker
from pyalgotrade.optimizer import local
from pyalgotrade.strategy.custom import OptimizerBacktestStrategy, \
    nearest_first_product
from pyalgotrade.talibext.indicator import *
from pyalgotrade.utils.telegrambot import start_bot

base_strategy = __main__.base_strategy if 'base_strategy' in dir(__main__) else OptimizerBacktestStrategy


def parameters_generator():
    brk = [None]
    instrument = [['XBTUSD']]
    position_size = [100]
    lb = [10]
    i = [-1]
    frequency = [bar.Frequency.MINUTE]
    return nearest_first_product(brk, instrument, frequency, position_size, lb, i)


def backtest(strategyClass, is_optimization, is_tick, start_date, end_date, symbol, frequency, position_size, lb, i):
    if is_tick:
        feed = bitbf.CSVTradeFeed()
        feed.addBarsFromCSV("../xbtusd_trades.csv", 'XBTUSD', fromDateTime=start_date,
                            toDateTime=end_date)
    else:
        feed = GenericBarFeed(bar.Frequency.MINUTE, maxLen=10000)
        feed.setColumnName('datetime', 'Date')
        feed.setDateTimeFormat('%Y-%m-%dT%H:%M:%S.%fZ')
        feed.setBarFilter(csvfeed.DateRangeFilter(start_date,
                                                  end_date))
        feed.addBarsFromCSV("XBTUSD", "../BITMEX_SPOT_BTC_USD_1MIN.csv")

    if is_optimization:
        strat = local.run(strategyClass, feed, parameters_generator(), workerCount=None,
                          logLevel=10, batchSize=1)
        print(strat.getResult(), strat.getParameters())
    else:
        strat = strategyClass(feed, None, symbol, int(frequency), position_size, lb, i)

        strat.run()


def bitmex_run(strategyClass, is_testnet, strategy_id, telegram_token, api_key, api_secret, cash, is_optimization,
               is_tick, start_date,
               end_date, symbol, frequency, position_size, lb, i):
    telebot = None
    while True:
        try:
            barFeed = barfeed.LiveTradeFeed(test=is_testnet, symbol=symbol, api_key=api_key, api_secret=api_secret)
            brk = broker.livebroker.LiveBroker(strategy_id=strategy_id, barFeed=barFeed, cash=cash)
            strat = strategyClass(barFeed, brk, symbol, int(frequency), position_size, lb, i)
            if telegram_token is not None and len(telegram_token) > 0:
                telebot = start_bot(telegram_token, strategy_id, strat)
            strat.run()
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            print(e)
        finally:
            if telebot is not None:
                telebot.stop()


class BreakoutLong(base_strategy):
    def __init__(self, feed, brk, instrument, frequency, position_size, lb, i):
        self.is_bitmex = brk is not None
        if self.is_bitmex:
            super(BreakoutLong, self).__init__(feed, brk, instrument)
        else:
            super(BreakoutLong, self).__init__(feed, instrument)
            self._resampledBF = feed

        self._resampledBF = self.resampleBarFeed(frequency,
                                                 self.onBars) if feed.getFrequency() != frequency else feed
        self.__feed = feed
        self.__instrument = instrument[0]
        self.__posSize = position_size

        self.lb = lb
        self.i = i

        self.maxLength = lb
        self.resampled_bars = self._resampledBF[self.__instrument]
        self.running = True
        self.long = True

    def onBars(self, bars):
        if self.__instrument not in bars or len(self.resampled_bars) < self.maxLength * 3 or 'old' in bars[
            self.__instrument].getExtraColumns() or not self.running:
            return

        bar = bars[self.__instrument]

        if bar.getFrequency() == self._resampledBF.getFrequency():
            super().log_orderfill(self._resampledBF.getFrequency())

            # prices_high = bar_ds_high_to_numpy(self.resampled_bars, self.lb)
            # prices_low = bar_ds_low_to_numpy(self.resampled_bars, self.lb)
            #
            # ph = np.max(prices_high)
            # pl = np.min(prices_low)

            prices_close = bar_ds_close_to_numpy(self.resampled_bars, self.lb)
            ph = np.min(prices_close)
            pl = np.max(prices_close)

            self.entry("Long", self.__instrument, self.long, self.__posSize, ph - self.i)
            self.entry("Lng", self.__instrument, not self.long, self.__posSize, pl + self.i)
