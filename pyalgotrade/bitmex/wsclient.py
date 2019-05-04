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
import json
import re

from six.moves import queue

from pyalgotrade.bitmex import bitmexWS
from pyalgotrade.bitmex import common
from pyalgotrade.websocket import client


class Margin(bitmexWS.Event):
    """A trade event."""

    def __init__(self, eventDict):
        super(Margin, self).__init__(eventDict)
        self.__dateTime = datetime.datetime.utcnow()

    def getDateTime(self):
        return self.__dateTime

    def getId(self):
        return self.getData()["account"]

    def getBalance(self):
        return self.getData()["availableMargin"]

    def getTimestamp(self):
        if type(self._Event__data['timestamp']) == str:
            self._Event__data['timestamp'] = datetime.datetime.strptime(self._Event__data['timestamp'],
                                                                        "%Y-%m-%dT%H:%M:%S.%fZ")
        return self._Event__data['timestamp'].replace(tzinfo=None)

    def update(self, eventDict):
        self._Event__eventDict.update(eventDict)
        self._Event__data.update(eventDict.get("data"))
        self.__dateTime = self.getTimestamp()


class Execution(bitmexWS.Event):
    """A trade event."""

    def __init__(self, eventDict):
        super(Execution, self).__init__(eventDict)
        self.__dateTime = self.getTimestamp()

    def getDateTime(self):
        return self.__dateTime

    def getId(self):
        return self.getData()["orderID"]

    def getExecId(self):
        return self.getData()["execID"]

    def getSymbol(self):
        return self.getData()["symbol"]

    def isBuy(self):
        return self.getData()["side"] == 'Buy'

    def isSell(self):
        return self.getData()["side"] == 'Sell'

    def getPrice(self):
        return self.getData()["price"]

    def getAmount(self):
        return self.getData()["orderQty"]

    def getTimestamp(self):
        if type(self._Event__data['timestamp']) == str:
            self._Event__data['timestamp'] = datetime.datetime.strptime(self._Event__data['timestamp'],
                                                                        "%Y-%m-%dT%H:%M:%S.%fZ")
        return self._Event__data['timestamp'].replace(tzinfo=None)

    def getFilledQty(self):
        return abs(self.getData()['lastQty']) if 'lastQty' in self.getData() and self.getData()[
            'lastQty'] is not None else abs(self.getData()['cumQty'])

    def getExtra(self):
        search = re.search(r"\s*([{\[].*?[}\]])$", self.getData()["text"])
        if search is None:
            return {}
        return json.loads(search.group(1))


class Position(bitmexWS.Event):
    """A trade event."""

    def __init__(self, eventDict):
        super(Position, self).__init__(eventDict)
        self.__dateTime = self.getTimestamp()

    def getDateTime(self):
        return self.__dateTime

    def getSymbol(self):
        return self.getData()["symbol"]

    def isBuy(self):
        return self.getData()["currentCost"] > 0

    def isSell(self):
        return self.getData()["currentCost"] < 0

    def isOpen(self):
        return self.getData()["isOpen"]

    def getPrice(self):
        return self.getData()["currentCost"]

    def getAmount(self):
        return self.getData()["currentQty"]

    def getTimestamp(self):
        if type(self._Event__data['timestamp']) == str:
            self._Event__data['timestamp'] = datetime.datetime.strptime(self._Event__data['timestamp'],
                                                                        "%Y-%m-%dT%H:%M:%S.%fZ")

        return self._Event__data['timestamp'].replace(tzinfo=None)


class Order(bitmexWS.Event):
    """A trade event."""

    def __init__(self, eventDict):
        super(Order, self).__init__(eventDict)
        self.__dateTime = self.getTimestamp()

    def getDateTime(self):
        return self.__dateTime

    def getId(self):
        return self.getData()["orderID"]

    def getSymbol(self):
        return self.getData()["symbol"]

    def isBuy(self):
        return self.getData()["side"] == 'Buy'

    def isSell(self):
        return self.getData()["side"] == 'Sell'

    def getType(self):
        return self.getData()["ordType"]

    def getStatus(self):
        return self.getData()["ordStatus"]

    def getPrice(self):
        return self.getData()["price"]

    def getStopPrice(self):
        return self.getData()["stopPx"]

    def getAmount(self):
        return self.getData()["orderQty"]

    def getFilledQty(self):
        return abs(self.getData()['lastQty']) if 'lastQty' in self.getData() and self.getData()[
            'lastQty'] is not None else abs(self.getData()['cumQty'])

    def getTimestamp(self):
        if type(self._Event__data['timestamp']) == str:
            self._Event__data['timestamp'] = datetime.datetime.strptime(self._Event__data['timestamp'],
                                                                        "%Y-%m-%dT%H:%M:%S.%fZ")

        return self._Event__data['timestamp'].replace(tzinfo=None)

    def getExtra(self):
        search = re.search(r"\s([{\[].*?[}\]])$", self.getData()["text"])
        if search is None:
            return {}
        return json.loads(search.group(1))


class Trade(bitmexWS.Event):
    """A trade event."""

    def __init__(self, eventDict):
        super(Trade, self).__init__(eventDict)
        self.__dateTime = self.getTimestamp()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getId(self):
        """Returns the trade id."""
        return self.getData()["trdMatchID"]

    def getPrice(self):
        """Returns the trade price."""
        return self.getData()["price"]

    def getAmount(self):
        """Returns the trade amount."""
        return self.getData()["size"]

    def isBuy(self):
        """Returns True if the trade was a buy."""
        return self.getData()["side"] == 'Buy'

    def isSell(self):
        """Returns True if the trade was a sell."""
        return self.getData()["side"] == 'Sell'

    def getTimestamp(self):
        if type(self._Event__data['timestamp']) == str:
            self._Event__data['timestamp'] = datetime.datetime.strptime(self._Event__data['timestamp'],
                                                                        "%Y-%m-%dT%H:%M:%S.%fZ")

        return self._Event__data['timestamp'].replace(tzinfo=None)


class OrderBookUpdate(bitmexWS.Event):
    """An order book update event."""

    def __init__(self, eventDict):
        super(OrderBookUpdate, self).__init__(eventDict)
        self.__dateTime = datetime.datetime.utcnow()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getBids(self):
        """Returns a list with the top 25 ask prices."""
        return sorted([bid for bid in self.getData() if bid['side'] == 'Buy' and 'price' in bid and 'size' in bid],
                      key=lambda k: k['price'], reverse=True)

    def getBidPrices(self):
        """Returns a list with the top 25 bid prices."""
        return [bid['price'] for bid in self.getBids()]

    def getBidVolumes(self):
        """Returns a list with the top 25 bid volumes."""
        return [bid['size'] for bid in self.getBids()]

    def getAsks(self):
        """Returns a list with the top 25 ask prices."""
        return sorted([ask for ask in self.getData() if ask['side'] == 'Sell' and 'price' in ask and 'size' in ask],
                      key=lambda k: k['price'])

    def getAskPrices(self):
        """Returns a list with the top 25 ask prices."""
        return [ask['price'] for ask in self.getAsks()]

    def getAskVolumes(self):
        """Returns a list with the top 25 ask volumes."""
        return [ask['size'] for ask in self.getData()]


class WebSocketClient(bitmexWS.WebSocketClient):
    class Event:
        TRADE = 1
        ORDER_BOOK_UPDATE = 2
        CONNECTED = 3
        DISCONNECTED = 4
        MARGIN = 5
        ORDER = 6
        POSITION = 7
        EXECUTION = 8

    def __init__(self, queue, endpoint, symbol, api_key=None, api_secret=None):
        super(WebSocketClient, self).__init__(endpoint=endpoint, symbol=symbol, api_key=api_key, api_secret=api_secret)
        self.__queue = queue
        self.last_order_book = {}
        self.aMargin = Margin({"data": {}})
        for aSymbol in symbol:
            self.last_order_book.update({aSymbol: None})

    def onMessage(self, msg):
        event = msg.get("table")
        if event == "trade":
            dataArr = msg['data']
            for idx, aData in enumerate(dataArr):
                msg['data'] = aData
                self.onTrade(
                    Trade(msg))
        elif event == "margin":
            msg['data'] = msg['data'][0]
            self.aMargin.update(msg)
            self.onMargin(self.aMargin)
        elif event == "order":
            dataArr = msg['data']
            for idx, aData in enumerate(dataArr):
                msg['data'] = aData
                self.onOrder(Order(msg))
        elif event == "position":
            dataArr = msg['data']
            for idx, aData in enumerate(dataArr):
                msg['data'] = aData
                self.onPosition(Position(msg))
        elif event == "execution":
            dataArr = msg['data']
            for idx, aData in enumerate(dataArr):
                msg['data'] = aData
                self.onExecution(Execution(msg))
        elif event is not None and 'orderBook' in event:
            symbol_ = msg['data'][0]['symbol']
            if msg.get('action') == 'partial':
                self.last_order_book[symbol_] = OrderBookUpdate(msg)
            elif self.last_order_book[symbol_] is not None:
                if msg.get('action') == 'insert':
                    self.last_order_book[symbol_]._Event__data += msg['data']
                elif msg.get('action') == 'update':
                    for idx in range(len(self.last_order_book[symbol_]._Event__data)):
                        for newOrderBook in msg.get('data'):
                            if self.last_order_book[symbol_]._Event__data[idx]['id'] == newOrderBook['id']:
                                self.last_order_book[symbol_]._Event__data[idx].update(newOrderBook)
                elif msg.get('action') == 'delete':
                    for old_order in self.last_order_book[symbol_]._Event__data:
                        for newOrderBook in msg.get('data'):
                            if old_order['id'] == newOrderBook['id']:
                                self.last_order_book[symbol_]._Event__data.remove(old_order)
            self.onOrderBookUpdate({symbol_: self.last_order_book[symbol_]})
        else:
            super(WebSocketClient, self).onMessage(msg)

    def onClosed(self, code, reason):
        common.logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onDisconnectionDetected(self):
        common.logger.warning("Disconnection detected.")
        try:
            self.stopClient()
        except Exception as e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
        self.__queue.put((WebSocketClient.Event.DISCONNECTED, None))

    def onConnectionEstablished(self, event):
        common.logger.info("Connection established.")
        self.__queue.put((WebSocketClient.Event.CONNECTED, None))

    def onError(self, event):
        common.logger.error("Error: %s" % (event))

    def onUnknownEvent(self, event):
        common.logger.warning("Unknown event: %s" % (event))

    def onMargin(self, margin):
        self.__queue.put((WebSocketClient.Event.MARGIN, margin))

    def onPosition(self, position):
        self.__queue.put((WebSocketClient.Event.POSITION, position))

    def onExecution(self, execution):
        self.__queue.put((WebSocketClient.Event.EXECUTION, execution))

    def onOrder(self, order):
        self.__queue.put((WebSocketClient.Event.ORDER, order))

    def onTrade(self, trade):
        self.__queue.put((WebSocketClient.Event.TRADE, trade))

    def onOrderBookUpdate(self, orderBookUpdate):
        self.__queue.put((WebSocketClient.Event.ORDER_BOOK_UPDATE, orderBookUpdate))


class WebSocketClientThread(client.WebSocketClientThreadBase):
    """
    This thread class is responsible for running a WebSocketClient.
    """

    def __init__(self, endpoint, symbol, api_key=None, api_secret=None):
        super(WebSocketClientThread, self).__init__()
        self.__queue = queue.Queue()
        self.__wsClient = None
        self.endpoint = endpoint
        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret

    def getQueue(self):
        return self.__queue

    def run(self):
        super(WebSocketClientThread, self).run()

        # We create the WebSocketClient right in the thread, instead of doing so in the constructor,
        # because it has thread affinity.
        try:
            self.__wsClient = WebSocketClient(self.__queue, endpoint=self.endpoint, symbol=self.symbol,
                                              api_key=self.api_key,
                                              api_secret=self.api_secret)
            self.__wsClient.connect()
            self.__wsClient.startClient()
        except Exception:
            common.logger.exception("Failed to connect: %s")

    def stop(self):
        try:
            if self.__wsClient is not None:
                common.logger.info("Stopping websocket client.")
                self.__wsClient.stopClient()
        except Exception as e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
