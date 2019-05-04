# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import datetime
import time

import bitmex
from six.moves import queue

from pyalgotrade import bar
from pyalgotrade import barfeed
from pyalgotrade import observer
from pyalgotrade.bar import BasicBar
from pyalgotrade.bitmex import common
from pyalgotrade.bitmex import wsclient


class TradeBar(bar.Bar):
    # Optimization to reduce memory footprint.
    __slots__ = ('__dateTime', '__tradeId', '__price', '__amount')

    def __init__(self, dateTime, trade):
        self.__dateTime = dateTime
        self.__tradeId = trade.getId()
        self.__price = trade.getPrice()
        self.__amount = trade.getAmount()
        self.__buy = trade.isBuy()

    def __setstate__(self, state):
        (self.__dateTime, self.__tradeId, self.__price, self.__amount) = state

    def __getstate__(self):
        return (self.__dateTime, self.__tradeId, self.__price, self.__amount)

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted:
            raise Exception("Adjusted close is not available")

    def getTradeId(self):
        return self.__tradeId

    def getFrequency(self):
        return bar.Frequency.TRADE

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        return self.__price

    def getHigh(self, adjusted=False):
        return self.__price

    def getLow(self, adjusted=False):
        return self.__price

    def getClose(self, adjusted=False):
        return self.__price

    def getVolume(self):
        return self.__amount

    def getAdjClose(self):
        return None

    def getTypicalPrice(self):
        return self.__price

    def getPrice(self):
        return self.__price

    def getUseAdjValue(self):
        return False

    def isBuy(self):
        return self.__buy

    def isSell(self):
        return not self.__buy


class LiveTradeFeed(barfeed.BaseBarFeed):
    """A real-time BarFeed that builds bars from live trades.

    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        Note that a Bar will be created for every trade, so open, high, low and close values will all be the same.
    """

    QUEUE_TIMEOUT = 0.01

    def __init__(self, symbol=["XBTUSD"], resample_frequancy=60, test=True, config=None, api_key=None, api_secret=None,
                 maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__barDicts = []
        symbol = symbol if isinstance(symbol, list) else symbol.split(',')
        for aSymbol in reversed(symbol):
            self.registerInstrument(aSymbol)
            common.btc_symbol = aSymbol
        self.__prevTradeDateTime = None
        self.__thread = None
        self.__wsClientConnected = False
        self.__enableReconnection = True
        self.__stopped = False
        self.endpoint = "https://testnet.bitmex.com/api/v1" if test else "https://www.bitmex.com/api/v1"
        self.__httpClient = None
        for _ in range(3):
            try:
                self.__httpClient = bitmex.bitmex(test=test, config=config, api_key=api_key, api_secret=api_secret)
                break
            except Exception as e:
                common.logger.info("http error: " + str(e))
        if self.__httpClient is None:
            raise Exception("http error")
        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret
        self.__orderBookUpdateEvent = observer.Event()
        self.__onMargin = observer.Event()
        self.__onExecution = observer.Event()
        self.__onOrder = observer.Event()
        self.__onPostition = observer.Event()

        self.resample_frequancy = resample_frequancy
        if resample_frequancy / 60 >= 60 * 60 * 24:
            self.bin_size = '1d'
        elif resample_frequancy / 60 >= 60 * 60:
            self.bin_size = '1h'
        elif resample_frequancy / 60 >= 60 * 5:
            self.bin_size = '5m'
        else:
            self.bin_size = '1m'

    def getHttpClient(self):
        return self.__httpClient

    # Factory method for testing purposes.
    def buildWebSocketClientThread(self):
        return wsclient.WebSocketClientThread(endpoint=self.endpoint, symbol=self.symbol, api_key=self.api_key,
                                              api_secret=self.api_secret)

    def getCurrentDateTime(self):
        return self.__prevTradeDateTime

    def enableReconection(self, enableReconnection):
        self.__enableReconnection = enableReconnection

    def __initializeClient(self):
        common.logger.info("Initializing websocket client.")
        assert self.__wsClientConnected is False, "Websocket client already connected"

        try:
            # Start the thread that runs the client.
            self.__thread = self.buildWebSocketClientThread()
            self.__thread.start()
        except Exception as e:
            common.logger.exception("Error connecting : %s" % str(e))

        # Wait for initialization to complete.
        while not self.__wsClientConnected and self.__thread is not None and self.__thread.is_alive():
            self.__dispatchImpl([wsclient.WebSocketClient.Event.CONNECTED])

        if self.__wsClientConnected:
            common.logger.info("Initialization ok.")
        else:
            common.logger.error("Initialization failed.")
        return self.__wsClientConnected

    def __onConnected(self):
        bins = {}
        for symbol in self.symbol:
            for _ in range(3):
                try:
                    bins[symbol] = \
                        self.__httpClient.Trade.Trade_getBucketed(binSize=self.bin_size, symbol=symbol, reverse=True,
                                                                  count=750, partial=True).result()[0]
                    break
                except Exception as e:
                    common.logger.error('__onConnected ' + str(e))

        for idx in range(len(bins[self.symbol[0]]) - 1, -1, -1):
            bar_time = bins[self.symbol[0]][idx]['timestamp'].replace(tzinfo=None) - datetime.timedelta(
                seconds=self.resample_frequancy)
            if self.__prevTradeDateTime is None or bar_time > self.__prevTradeDateTime:
                barDict = {}
                for symbol in self.symbol:
                    if idx < len(bins[symbol]) and bins[symbol][idx]['open'] is not None and bins[symbol][idx][
                        'close'] is not None:
                        self.__prevTradeDateTime = bar_time
                        bins[symbol][idx]['high'] = max(bins[symbol][idx]['open'], bins[symbol][idx]['high'],
                                                        bins[symbol][idx]['close'])
                        bins[symbol][idx]['low'] = min(bins[symbol][idx]['open'], bins[symbol][idx]['low'],
                                                       bins[symbol][idx]['close'])

                        barDict.update({
                            symbol: BasicBar(bar_time, bins[symbol][idx]['open'], bins[symbol][idx]['high'],
                                             bins[symbol][idx]['low'],
                                             bins[symbol][idx]['close'],
                                             bins[symbol][idx]['volume'], None, self.resample_frequancy, {"old": True})
                        })

                self.__barDicts.append(barDict)

        common.logger.info('WS Connected')
        self.__wsClientConnected = True

    def __onDisconnected(self):
        self.__wsClientConnected = False

        if self.__enableReconnection:
            initialized = False
            common.logger.info("__onDisconnected")
            while not self.__stopped and not initialized:
                common.logger.info("Reconnecting")
                initialized = self.__initializeClient()
                if not initialized:
                    time.sleep(1)
        else:
            common.logger.info("__enableReconnection is false")
            self.__stopped = True

    def __dispatchImpl(self, eventFilter):
        ret = False
        try:
            eventType, eventData = self.__thread.getQueue().get(True, LiveTradeFeed.QUEUE_TIMEOUT)
            if eventFilter is not None and eventType not in eventFilter:
                return False

            ret = True
            if eventType == wsclient.WebSocketClient.Event.TRADE:
                self.__onTrade(eventData)
            elif eventType == wsclient.WebSocketClient.Event.ORDER_BOOK_UPDATE:
                self.__orderBookUpdateEvent.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.MARGIN:
                self.__onMargin.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.POSITION:
                self.__onPostition.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.ORDER:
                self.__onOrder.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.EXECUTION:
                self.__onExecution.emit(eventData)
            elif eventType == wsclient.WebSocketClient.Event.CONNECTED:
                self.__onConnected()
            elif eventType == wsclient.WebSocketClient.Event.DISCONNECTED:
                self.__onDisconnected()
            else:
                ret = False
                common.logger.error("Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except queue.Empty:
            pass
        return ret

    # Bar datetimes should not duplicate. In case trade object datetimes conflict, we just move one slightly forward.
    def __getTradeDateTime(self, trade):
        ret = trade.getDateTime()
        if self.__prevTradeDateTime is not None and ret < self.__prevTradeDateTime:
            ret = self.__prevTradeDateTime
        if ret == self.__prevTradeDateTime:
            ret += datetime.timedelta(microseconds=1)
        self.__prevTradeDateTime = ret
        return ret

    def __onTrade(self, trade):
        # Build a bar for each trade.
        barDict = {
            trade._Event__data['symbol']: TradeBar(self.__getTradeDateTime(trade), trade)
        }
        self.__barDicts.append(barDict)

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        if len(self.__barDicts):
            ret = bar.Bars(self.__barDicts.pop(0))
        return ret

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        super(LiveTradeFeed, self).start()
        if self.__thread is not None:
            raise Exception("Already running")
        elif not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        ret = False
        if self.__dispatchImpl(None):
            ret = True
        if super(LiveTradeFeed, self).dispatch():
            ret = True
        return ret

    # This should not raise.
    def stop(self):
        try:
            self.__stopped = True
            if self.__thread is not None and self.__thread.is_alive():
                common.logger.info("Shutting down websocket client.")
                self.__thread.stop()
        except Exception as e:
            common.logger.error("Error shutting down client: %s" % (str(e)))

    # This should not raise.
    def join(self):
        if self.__thread is not None:
            self.__thread.join()

    def eof(self):
        return self.__stopped

    def getOrderBookUpdateEvent(self):
        """
        Returns the event that will be emitted when the orderbook gets updated.

        Eventh handlers should receive one parameter:
         1. A :class:`pyalgotrade.bitmex.wsclient.OrderBookUpdate` instance.

        :rtype: :class:`pyalgotrade.observer.Event`.
        """
        return self.__orderBookUpdateEvent
