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
import time
import uuid

import six

from pyalgotrade import broker
from pyalgotrade.bitmex import common
from pyalgotrade.bitmex.wsclient import Execution
from pyalgotrade.strategy.position import ShortPosition, LongPosition


def build_order_from_open_order(openOrder, instrumentTraits, state=broker.Order.State.ACCEPTED):
    if openOrder.isBuy():
        if 'Close' in openOrder.getData().get('execInst', '') or openOrder.getExtra().get('close'):
            action = broker.Order.Action.BUY_TO_COVER
        else:
            action = broker.Order.Action.BUY
    elif openOrder.isSell():
        if 'Close' in openOrder.getData().get('execInst', '') or openOrder.getExtra().get('close'):
            action = broker.Order.Action.SELL
        else:
            action = broker.Order.Action.SELL_SHORT
    else:
        raise Exception("Invalid order side")

    if openOrder.getType() in ['Market', 'MarketIfTouched', 'MarketWithLeftOverAsLimit']:
        ret = broker.MarketOrder(action, common.btc_symbol, openOrder.getAmount(), True,
                                 instrumentTraits, openOrder.getExtra())
    elif openOrder.getType() in ['Limit', 'LimitIfTouched']:
        ret = broker.LimitOrder(action, common.btc_symbol, openOrder.getPrice(), openOrder.getAmount(),
                                instrumentTraits, openOrder.getExtra())
    elif openOrder.getType() == 'StopLimit':
        ret = broker.StopLimitOrder(action, common.btc_symbol, openOrder.getStopPrice(), openOrder.getPrice(),
                                    openOrder.getAmount(),
                                    instrumentTraits, openOrder.getExtra())
    elif openOrder.getType() == 'Stop':
        ret = broker.StopOrder(action, common.btc_symbol, openOrder.getStopPrice(), openOrder.getAmount(),
                               instrumentTraits, openOrder.getExtra())
    else:
        raise Exception("Invalid order type")

    ret.setSubmitted(openOrder.getId(), openOrder.getDateTime())
    ret.setState(state)
    return ret


class Order(object):
    def __init__(self, jsonDict):
        self.__jsonDict = jsonDict

    def getDict(self):
        return self.__jsonDict

    def getData(self):
        return self.__jsonDict

    def getId(self):
        return self.__jsonDict['orderID']

    def isBuy(self):
        return self.__jsonDict["side"] == 'Buy'

    def isSell(self):
        return self.__jsonDict["side"] == 'Sell' or self.__jsonDict["side"] == ''

    def getPrice(self):
        return self.__jsonDict["price"]

    def getStopPrice(self):
        return self.__jsonDict["stopPx"]

    def getAmount(self):
        return self.__jsonDict['orderQty']

    def getType(self):
        return self.__jsonDict["ordType"]

    def getExtra(self):
        search = re.search(r"\s*([{\[].*?[}\]])$", self.getData()["text"])
        if search is None:
            return {}
        return json.loads(search.group(1))

    def getDateTime(self):
        if type(self.__jsonDict['timestamp']) == str:
            self.__jsonDict['timestamp'] = datetime.datetime.strptime(self.__jsonDict['timestamp'],
                                                                      "%Y-%m-%dT%H:%M:%S.%fZ")
        return self.__jsonDict['timestamp'].replace(tzinfo=None)


class LiveBroker(broker.Broker):
    """A bitmex live broker.

    :param clientId: Client id.
    :type clientId: string.
    :param key: API key.
    :type key: string.
    :param secret: API secret.
    :type secret: string.


    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
        * API access permissions should include:

          * Account balance
          * Open orders
          * Buy limit order
          * User transactions
          * Cancel order
          * Sell limit order
    """

    def __init__(self, strategy_id, barFeed, cash=0):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__httpClient = barFeed.getHttpClient()
        self.__barFeed = barFeed
        barFeed.getOrderBookUpdateEvent().subscribe(self.__onOrderBookUpdate)
        self.__cash = cash
        self.__bitmex_cash = 0
        self.__bitmex_shares = {}
        self.__bitmex_equity = 0
        self.__equity = 0
        self.__shares = {}
        self.__activeOrders = {}
        self.__instrumentPrice = {}
        self.strategy_id = strategy_id
        self.__strategy = None
        self.__allowNegativeCash = False
        self.orderBookUpdate = {}

    def __onOrderBookUpdate(self, orderBookUpdate):
        for key in orderBookUpdate:
            self.orderBookUpdate[key] = orderBookUpdate[key]

    def getStrategyId(self):
        return self.strategy_id

    def getStrategy(self):
        return self.__strategy

    def setStrategy(self, strategy):
        self.__strategy = strategy

    def join(self):
        pass

    def _getPriceForInstrument(self, instrument):
        ret = None

        # Try gettting the price from the last bar first.
        lastBar = self.__barFeed.getLastBar(instrument)
        if lastBar is not None:
            ret = lastBar.getPrice()
        else:
            # Try using the instrument price set by setShares if its available.
            ret = self.__instrumentPrice.get(instrument)

        return ret

    def getEquity(self):
        """Returns the portfolio value (cash + shares * price)."""

        ret = self.getCash()
        for instrument, shares in six.iteritems(self.__shares):
            instrumentPrice = self._getPriceForInstrument(instrument)
            if instrumentPrice is not None:
                ret += instrumentPrice * shares
        return ret

    def setShares(self, instrument, quantity, price):
        """
        Set existing shares before the strategy starts executing.

        :param instrument: Instrument identifier.
        :param quantity: The number of shares for the given instrument.
        :param price: The price for each share.
        """

        self.__shares[instrument] = quantity
        self.__instrumentPrice[instrument] = price

    def getActiveInstruments(self):
        return [instrument for instrument, shares in six.iteritems(self.__shares) if shares != 0]

    def get_synced_position(self, strategy, symbol):
        self.__strategy = strategy
        __allowNegativeCash = self.__allowNegativeCash
        self.__allowNegativeCash = True
        all_orders = self.__httpClient.Order.Order_getOrders(symbol=symbol, reverse=True, count=500).result()[
            0]
        all_orders += \
            self.__httpClient.Order.Order_getOrders(symbol=symbol, reverse=True, count=500, start=500).result()[
                0]

        positions = {}
        for anOrder in reversed(all_orders):
            execution = Execution({"data": anOrder})

            if (anOrder['ordStatus'] in ['Filled', 'PartiallyFilled', 'New'] or execution.getFilledQty() != 0) \
                    and execution.getExtra().get('strId') == self.strategy_id:

                built_order = build_order_from_open_order(Order(execution.getData()),
                                                          self.getInstrumentTraits(common.btc_symbol),
                                                          state=broker.Order.State.ACCEPTED)
                self._registerOrder(built_order)
                extra = built_order.getExtra()
                extra.update({'notify': False, "old": True})

                id = extra.get('id')
                if positions.get(id, None) is None:
                    if built_order.getAction() == broker.Order.Action.BUY:
                        positions[id] = LongPosition(strategy, built_order, None, None, None, True, False,
                                                     extra)
                    elif built_order.getAction() == broker.Order.Action.SELL_SHORT:
                        positions[id] = ShortPosition(strategy, built_order, None, None, None, True, False,
                                                      extra)

                elif built_order.getAction() in [broker.Order.Action.SELL,
                                                 broker.Order.Action.BUY_TO_COVER]:
                    positions[id]._Position__submitAndRegisterOrder(built_order)
                    positions[id]._Position__exitOrder = built_order
                elif built_order.getAction() in [broker.Order.Action.BUY,
                                                 broker.Order.Action.SELL_SHORT]:
                    positions[id]._Position__submitAndRegisterOrder(built_order)
                    positions[id]._Position__entryOrder = built_order
                    positions[id]._Position__exitOrder = None

                self.onExecution(execution)

                for key, val in positions.items():
                    if val is not None and (not val.isOpen() or val.getShares() > 0):
                        positions[key] = None

        for key, val in positions.items():
            if val is not None:
                positions[key].updateExtra({"notify": True})
                if positions[key].entryFilled():
                    self.__strategy.onEnterOk(positions[key])

        self.__allowNegativeCash = __allowNegativeCash
        return positions

    def onExecution(self, data):
        order = self.__activeOrders.get(data.getId())
        if order is not None:
            self.onOrder(data)
            if order.isSubmitted():
                try:
                    order.switchState(broker.Order.State.ACCEPTED)
                    self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))
                except Exception as e:
                    common.logger.info('onExecution accept: ' + str(e))

            fee = 0
            fillPrice = data.getPrice()
            btcAmount = data.getFilledQty()
            dateTime = data.getDateTime()
            old_state = order.getState()

            # Update the order.
            if btcAmount > 0 and (
                    order.getExecutionInfo() is None or order.getExecutionInfo().getDateTime() != dateTime) and 'Trade' in \
                    data.getData().get('execType', 'Trade'):
                self.updateCash(order, fillPrice, btcAmount)
                orderExecutionInfo = broker.OrderExecutionInfo(fillPrice, btcAmount, fee, dateTime)
                order.addExecutionInfo(orderExecutionInfo)

            if data.getData()['ordStatus'] == 'New' and data.getFilledQty() == 0:
                new_state = broker.Order.State.ACCEPTED
                orderExecutionInfo = "New"
            elif order.isFilled():
                new_state = broker.Order.State.FILLED
            elif order.getFilled() != 0:
                new_state = broker.Order.State.PARTIALLY_FILLED
            elif data.getData()['ordStatus'] in ['Canceled', 'Rejected'] and order.getFilled() == 0:
                new_state = broker.Order.State.CANCELED
                orderExecutionInfo = data.getData()['text']
            else:
                common.logger.error('onExecution', data.getData()['ordStatus'])

            try:
                if new_state != order.getState():
                    order.switchState(new_state)
            except Exception as e:
                common.logger.info('onExecution switchstate: ' + str(e))
            if old_state != order.getState():
                try:
                    self.notifyOrderEvent(broker.OrderEvent(order, new_state - 1, orderExecutionInfo))
                except Exception as e:
                    common.logger.error('onExecution notifyOrderEvent: ' + str(e))
                    self.notifyOrderEvent(broker.OrderEvent(order, new_state - 1, orderExecutionInfo))
            if not order.isActive() and order.getId() in self.__activeOrders:
                self._unregisterOrder(order)

    def onOrder(self, data):
        order = self.__activeOrders.get(data.getId())

        if order is not None:
            if data.getData().get('action', None) == 'delete':
                self._unregisterOrder(order)
            else:
                if data.getData().get('orderQty', None) is not None:
                    order._Order__quantity = data.getData()['orderQty']
                if data.getData().get('triggered', '') != '':
                    if type(order).__name__ == 'StopLimitOrder':
                        order._StopLimitOrder__stopHit = True
                    elif type(order).__name__ == 'StopOrder':
                        order._StopOrder__stopHit = True
                if data.getData().get('price', None) is not None:
                    if type(order).__name__ == 'LimitOrder':
                        order._LimitOrder__limitPrice = data.getData()['price']
                    elif type(order).__name__ == 'StopLimitOrder':
                        order._StopLimitOrder__limitPrice = data.getData()['price']
                    elif type(order).__name__ == 'StopOrder':
                        order._StopOrder__limitPrice = data.getData()['price']
                if data.getData().get('stopPx', None) is not None:
                    if type(order).__name__ == 'StopLimitOrder':
                        order._StopLimitOrder__stopPrice = data.getData()['stopPx']
                    elif type(order).__name__ == 'StopOrder':
                        order._StopOrder__stopPrice = data.getData()['stopPx']
                if 'side' in data.getData():
                    if data.getData()['side'] == 'Buy':
                        if 'Close' in data.getData().get('execInst', '') or data.getExtra().get('close'):
                            order._Order__action = broker.Order.Action.BUY_TO_COVER
                        else:
                            order._Order__action = broker.Order.Action.BUY
                    else:
                        if 'Close' in data.getData().get('execInst', '') or data.getExtra().get('close'):
                            order._Order__action = broker.Order.Action.SELL
                        else:
                            order._Order__action = broker.Order.Action.SELL_SHORT

                if data.getData().get('ordStatus', 'Canceled') not in ['New', 'Filled', 'PartiallyFilled', 'Canceled']:
                    common.logger.error('order update', data.getData()['ordStatus'])

    def onPostition(self, data):
        self.__bitmex_shares[data.getData()['currency'].upper()] = data.getData()['currentQty']
        if 'unrealisedRoePcnt' in data.getData():
            self.___bitmex_equity = data.getData()['unrealisedRoePcnt']

    def onMargin(self, data):
        self.__bitmex_cash = data.getBalance()

    def addBitmexFeed(self, feed):
        feed._LiveTradeFeed__onExecution.subscribe(self.onExecution)
        feed._LiveTradeFeed__onOrder.subscribe(self.onOrder)
        feed._LiveTradeFeed__onPostition.subscribe(self.onPostition)
        feed._LiveTradeFeed__onMargin.subscribe(self.onMargin)

    def _registerOrder(self, order):
        if order.getId() is not None and order.getId() not in self.__activeOrders:
            self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        if order.getId() is not None and order.getId() in self.__activeOrders:
            del self.__activeOrders[order.getId()]

    # BEGIN observer.Subject interface
    def start(self):
        super(LiveBroker, self).start()

    def stop(self):
        self.__stop = True
        common.logger.info("Shutting down trade monitor.")

    def eof(self):
        if self.__stop:
            common.logger.info("eof stop.")

        return self.__stop

    def dispatch(self):
        pass
        # # Switch orders from SUBMITTED to ACCEPTED.
        # ordersToProcess = list(self.__activeOrders.values())
        # for order in ordersToProcess:
        #     if order.isSubmitted():
        #         order.switchState(broker.Order.State.ACCEPTED)
        #         self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # END observer.Subject interface

    # BEGIN broker.Broker interface

    def getCash(self, includeShort=True):
        ret = self.__cash
        if not includeShort and self.__barFeed.getCurrentBars() is not None:
            bars = self.__barFeed.getCurrentBars()
            for instrument, shares in six.iteritems(self.__shares):
                if shares < 0:
                    instrumentPrice = self._getBar(bars, instrument).getPrice()
                    ret += instrumentPrice * shares
        return ret

    def setCash(self, cash):
        self.__cash = cash

    def updateCash(self, order, price=None, quantity=None):
        if order is None:
            return
        resultingCash, sharesDelta = self.calculate_cash(order, price, quantity)

        # Check that we're ok on cash after the commission.
        if resultingCash >= 0 or self.__allowNegativeCash:
            # Commit the order execution.
            self.__cash = resultingCash
            updatedShares = order.getInstrumentTraits().roundQuantity(
                self.getShares(order.getInstrument()) + sharesDelta
            )
            if updatedShares == 0:
                del self.__shares[order.getInstrument()]
            else:
                self.__shares[order.getInstrument()] = updatedShares

        else:
            common.logger.debug("Not enough cash to fill %s order [%s] for %s share/s" % (
                order.getInstrument(),
                order.getId(),
                order.getRemaining()
            ))

    def calculate_cash(self, order, price=None, quantity=None):
        modify_quantity = False
        if price is None:
            price = order.getPrice() if 'getPrice' in dir(order) else order.getLimitPrice() if 'getLimitPrice' in dir(
                order) else self._getPriceForInstrument(order.getInstrument())
            quantity = order.getQuantity()
            modify_quantity = True

        quantity = quantity / price
        if order.isBuy():
            cost = price * quantity * -1
            assert (cost < 0)
            sharesDelta = quantity
        elif order.isSell():
            cost = price * quantity
            assert (cost > 0)
            sharesDelta = quantity * -1
        resultingCash = self.getCash() + cost

        if resultingCash < 0 and modify_quantity and (self.getCash() / price) * price >= 10:
            order._Order__quantity = (self.getCash() / price) * price
            resultingCash = 0

        return resultingCash, sharesDelta

    def setAllowNegativeCash(self, allowNegativeCash):
        self.__allowNegativeCash = allowNegativeCash

    def getInstrumentTraits(self, instrument):
        return common.BTCTraits()

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        return self.__shares

    def getActiveOrders(self, instrument=None):
        return list(self.__activeOrders.values())

    def submitOrder(self, order):
        if (order.isInitial() and order.getStopHit() or 'temp' in (order.getId() or '')):
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            side = 'Buy' if order.isBuy() else 'Sell'
            limitPrice = order.getLimitPrice() if 'getLimitPrice' in dir(order) else None

            if order.getType() in [broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT]:
                ordType = "Limit"
            elif order.getType() in [broker.Order.Type.MARKET, broker.Order.Type.STOP]:
                ordType = "Market"
            elif order.getType() == broker.Order.Type.MARKET_IF_TOUCHED:
                ordType = "MarketIfTouched"
            elif order.getType() == broker.Order.Type.MARKET_WITH_LEFT_OVER_AS_LIMIT:
                ordType = "MarketWithLeftOverAsLimit"
            elif order.getType() == broker.Order.Type.LIMIT_IF_TOUCHED:
                ordType = "LimitIfTouched"

            extra = order.getExtra()
            execInst = []
            if order.getType() in [broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT,
                                   broker.Order.Type.LIMIT_IF_TOUCHED]:
                execInst.append('ParticipateDoNotInitiate')
            # if extra.get('exit', False):
            #     execInst.append('Close')
            execInst = ','.join(execInst)

            extra.update({'strId': self.strategy_id})
            if order.getAction() in [broker.Order.Action.BUY_TO_COVER, broker.Order.Action.SELL]:
                extra.update({'close': True})

            oldPrice = limitPrice
            for _ in range(5):
                if oldPrice is not None:
                    bids = self.orderBookUpdate[order.getInstrument()].getBidPrices()
                    asks = self.orderBookUpdate[order.getInstrument()].getAskPrices()
                    if order.getAction() in [broker.Order.Action.SELL_SHORT,
                                             broker.Order.Action.SELL] and oldPrice < len(asks):
                        limitPrice = asks[oldPrice]
                    elif order.getAction() in [broker.Order.Action.BUY_TO_COVER,
                                               broker.Order.Action.BUY] and oldPrice < len(bids):
                        limitPrice = bids[oldPrice]

                if (self.calculate_cash(order, limitPrice, order.getQuantity())[
                        0] < 0 and not self.__allowNegativeCash) and order.getAction() in [
                    broker.Order.Action.SELL_SHORT,
                    broker.Order.Action.BUY]:
                    raise Exception("cash is negative")

                try:
                    order_result = \
                        self.__httpClient.Order.Order_new(symbol=order.getInstrument(), orderQty=order.getQuantity(),
                                                          price=limitPrice, side=side, ordType=ordType,
                                                          execInst=execInst,
                                                          text=json.dumps(extra)).result()[0]
                    bitmexOrder = Order(order_result)

                    if order_result['ordStatus'] in ['New', 'Filled',
                                                     'PartiallyFilled']:
                        common.logger.info('order submeted: ' + bitmexOrder.getId())
                        register = order.getId() is not None
                        pos = None
                        if register:
                            self._unregisterOrder(order)
                            pos = self.getStrategy().getOrderToPosition()[order.getId()]
                            self.getStrategy().unregisterPositionOrder(pos, order)

                        order.setSubmitted(bitmexOrder.getId(), bitmexOrder.getDateTime())
                        self._registerOrder(order)
                        # Switch from INITIAL -> SUBMITTED
                        # IMPORTANT: Do not emit an event for this switch because when using the position interface
                        # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
                        order.switchState(broker.Order.State.SUBMITTED)
                        if register:
                            self.getStrategy().registerPositionOrder(pos, order)
                        return
                    else:
                        time.sleep(1)

                except Exception as e:
                    common.logger.error('submitOrder: ' + str(e))
                    if 'Too Many'.lower() in str(e).lower() or 'Service Unavailable'.lower() in str(
                            e).lower() or 'expired'.lower() in str(e).lower():
                        time.sleep(4)
                    elif 'bad'.lower() in str(e).lower() or 'Invalid'.lower() in str(
                            e).lower() or 'Authorization Required'.lower() in str(e).lower():
                        break
        else:
            if not order.isInitial():
                raise Exception("The order was already processed")
            elif not order.getStopHit():
                order.setSubmitted('temp_' + uuid.uuid4().hex, datetime.datetime.utcnow())
                self._registerOrder(order)
                return

        order.switchState(broker.Order.State.CANCELED)

    def createMarketOrder(self, action, instrument, quantity, onClose=False, extra={}):
        instrumentTraits = self.getInstrumentTraits(instrument)
        quantity = instrumentTraits.roundQuantity(quantity)

        return broker.MarketOrder(action, instrument, quantity, True, instrumentTraits, extra=extra)

    def createLimitOrder(self, action, instrument, limitPrice, quantity, extra={}):
        instrumentTraits = self.getInstrumentTraits(instrument)
        quantity = instrumentTraits.roundQuantity(quantity)
        return broker.LimitOrder(action, instrument, limitPrice, quantity, instrumentTraits, extra=extra)

    def createStopOrder(self, action, instrument, stopPrice, quantity, extra={}):
        instrumentTraits = self.getInstrumentTraits(instrument)
        quantity = instrumentTraits.roundQuantity(quantity)
        return broker.StopOrder(action, instrument, stopPrice, quantity, instrumentTraits, extra=extra)

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity, extra={}):
        instrumentTraits = self.getInstrumentTraits(instrument)
        quantity = instrumentTraits.roundQuantity(quantity)
        return broker.StopLimitOrder(action, instrument, stopPrice, limitPrice, quantity, instrumentTraits, extra=extra)

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            common.logger.error("The order is not active anymore")
            return
        if activeOrder.isFilled():
            common.logger.error("Can't cancel order that has already been filled")
            return
        if 'temp' in activeOrder.getId():
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "temp order"))
            return

        for _ in range(5):
            try:
                canceled_order = self.__httpClient.Order.Order_cancel(orderID=order.getId()).result()[0][0]

                if canceled_order['ordStatus'] == 'Canceled':
                    try:
                        self._unregisterOrder(order)
                        order.switchState(broker.Order.State.CANCELED)
                        self.notifyOrderEvent(
                            broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))
                        common.logger.info('order canceled: ' + order.getId())
                    except Exception as e:
                        common.logger.error('cancelOrder1: ' + str(e))
                else:
                    self.onExecution(Execution({"data": canceled_order}))
                break
            except Exception as e:
                common.logger.error('cancelOrder: ' + str(e))
                if 'Too Many'.lower() in str(e).lower() or 'Service Unavailable'.lower() in str(
                        e).lower() or 'expired'.lower() in str(e).lower():
                    time.sleep(4)
                elif 'bad'.lower() in str(e).lower() or 'Invalid'.lower() in str(e).lower():
                    break
                elif 'not found'.lower() in str(e).lower():
                    self._unregisterOrder(order)
                    order.switchState(broker.Order.State.CANCELED)
                    self.notifyOrderEvent(
                        broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "not found cancellation"))
                    break

    def updateOrder(self, order, stopPrice=None, limitPrice=None, quantity=None, goodTillCanceled=True, extra={}):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            common.logger.error("The order is not active anymore")
            return
        if stopPrice is None and limitPrice is None and quantity is None:
            common.logger.error("Nothing to update")
            return

        if stopPrice is not None:
            stopPrice = round((stopPrice) * 2) / 2

        if 'temp' in activeOrder.getId():
            common.logger.info('update Order: ' + order.getId())
            if quantity is not None:
                order._Order__quantity = quantity
            if limitPrice is not None:
                if type(order).__name__ == 'LimitOrder':
                    order._LimitOrder__limitPrice = limitPrice
                elif type(order).__name__ == 'StopLimitOrder':
                    order._StopLimitOrder__limitPrice = limitPrice
                elif type(order).__name__ == 'StopOrder':
                    order._StopOrder__limitPrice = limitPrice
            if stopPrice is not None:
                if type(order).__name__ == 'StopLimitOrder':
                    activeOrder._StopLimitOrder__stopPrice = stopPrice
                    activeOrder._StopLimitOrder__stopHit = False
                elif type(activeOrder).__name__ == 'StopOrder':
                    activeOrder._StopOrder__stopPrice = stopPrice
                    activeOrder._StopOrder__stopHit = False
            return

        if limitPrice is not None:
            bids = self.orderBookUpdate[order.getInstrument()].getBidPrices()
            asks = self.orderBookUpdate[order.getInstrument()].getAskPrices()
            if order.getAction() in [broker.Order.Action.SELL_SHORT, broker.Order.Action.SELL] and limitPrice < len(
                    asks):
                limitPrice = asks[limitPrice]
            elif order.getAction() in [broker.Order.Action.BUY_TO_COVER, broker.Order.Action.BUY] and limitPrice < len(
                    bids):
                limitPrice = bids[limitPrice]

        for _ in range(5):
            try:
                data = Order(
                    self.__httpClient.Order.Order_amend(orderID=order.getId(), price=limitPrice,
                                                        orderQty=quantity).result()[0])
                common.logger.info('update Order: ' + order.getId())

                self.onOrder(data)
                break
            except Exception as e:
                common.logger.error('updateOrder: ' + str(e) + ', ' + order.getId())
                if 'Too Many'.lower() in str(e).lower() or 'Service Unavailable'.lower() in str(
                        e).lower() or 'expired'.lower() in str(e).lower():
                    time.sleep(4)
                elif 'bad'.lower() in str(e).lower() or 'Invalid'.lower() in str(e).lower():
                    if 'trigger'.lower() in str(e).lower():
                        if type(order).__name__ == 'StopLimitOrder':
                            activeOrder._StopLimitOrder__stopHit = True
                        elif type(activeOrder).__name__ == 'StopOrder':
                            activeOrder._StopOrder__stopHit = True
                    break
