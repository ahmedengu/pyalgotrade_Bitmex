import heapq
import os

from pyalgotrade import strategy, plotter
from pyalgotrade.broker.fillstrategy import get_stop_price_trigger
from pyalgotrade.stratanalyzer import returns, sharpe, drawdown, trades


def nearest_first_product(*sequences):
    start = (0,) * len(sequences)
    queue = [(0, start)]
    seen = set([start])
    while queue:
        priority, indexes = heapq.heappop(queue)
        yield tuple(seq[index] for seq, index in zip(sequences, indexes))
        for i in range(len(sequences)):
            if indexes[i] < len(sequences[i]) - 1:
                lst = list(indexes)
                lst[i] += 1
                new_indexes = tuple(lst)
                if new_indexes not in seen:
                    new_priority = sum(index * index for index in new_indexes)
                    heapq.heappush(queue, (new_priority, new_indexes))
                    seen.add(new_indexes)


def onEnterOkCustom(self, position):
    self.info(type(position).__name__ + " opened at %s, Shares: %s" % (
        position.getEntryOrder().getExecutionInfo().getPrice(), position.getShares()))


def onExitOkCustom(self, position):
    remove_position(self, position)

    self.info(type(position).__name__ + " closed at %s, PnL: $%.2f, Return: %%%.10f" % (
        position.getExitOrder().getExecutionInfo().getPrice(), position.getPnL(), position.getReturn()))
    with open(self.strategy_id + '.csv', 'a') as f:
        f.write(str(position._Position__entryDateTime) + ',' + type(
            position).__name__ + ',Entry,' + position.getEntryOrder().getInstrument() + ',' + str(
            position.getEntryOrder().getExecutionInfo().getPrice()) + ',' + str(
            position.getEntryOrder().getExecutionInfo().getQuantity()) + ',-,-\n')

        f.write(str(position._Position__exitDateTime) + ',' + type(
            position).__name__ + ',Exit,' + position.getExitOrder().getInstrument() + ',' + str(
            position.getExitOrder().getExecutionInfo().getPrice()) + ',-,' + str(position.getPnL()) + ',' + str(
            self.getResult()) + '\n')


def remove_position(self, position):
    instrument = position.getEntryOrder().getInstrument() if position.getEntryOrder() is not None else None
    if not position.isOpen() and instrument is not None and instrument in self.positions:
        for key, val in self.positions[instrument].items():
            if val == position:
                self.positions[instrument][key] = None


def onEnterCanceledCustom(self, position):
    remove_position(self, position)
    self.info(type(position).__name__ + " entry canceled")


def onExitCanceledCustom(self, position):
    self.info(type(position).__name__ + " exit canceled")


def entryLogic(self, id, symbol, long, qty, stop, is_market, bidIndex, askIndex, comment, when):
    if when:
        entryPrice = None if is_market else bidIndex if long else askIndex
        exitPrice = None if is_market else askIndex if long else bidIndex
        extra = {"id": id}
        if comment is not None:
            extra.update({'comment': comment})
        if symbol not in self.positions:
            self.positions[symbol] = {}

        opposite_position = False
        for key, val in self.positions[symbol].items():
            if val is not None and bool('Short' in type(val).__name__) == bool(long):
                if val.exitActive():
                    if 'Limit' not in type(val.getExitOrder()).__name__ or abs(
                            val.getExitOrder().getLimitPrice() - self.getLastPrice(symbol)) >= 2:
                        if stop is not None and 'Stop' in type(
                                val.getExitOrder()).__name__ and val.getExitOrder().getStopHit():
                            val.cancelExit()
                            if val.isOpen():
                                val.exitStopLimit(stop, exitPrice)
                        else:
                            val.updateExit(stop, exitPrice)
                    opposite_position = True
                elif val.getShares() != 0:
                    val.exitStopLimit(stop, exitPrice)
                    opposite_position = True
                elif val.entryActive() and not val.getEntryOrder().getStopHit():
                    opposite_position = True
                else:
                    val.cancelEntry()

        if opposite_position and not is_market:
            for key, val in self.positions[symbol].items():
                if val is not None and bool('Short' in type(val).__name__) == bool(long):
                    if val.isOpen():
                        return

        if self.positions[symbol].get(id, None) is not None and self.positions[symbol][id].entryActive():
            if 'Limit' not in type(self.positions[symbol][id].getEntryOrder()).__name__ or abs(
                    self.positions[symbol][id].getEntryOrder().getLimitPrice() - self.getLastPrice(symbol)) >= 2:
                if stop is not None and 'Stop' in type(self.positions[symbol][id].getEntryOrder()).__name__ and \
                        self.positions[symbol][id].getEntryOrder().getStopHit():
                    self.positions[symbol][id].cancelEntry()
                else:
                    self.positions[symbol][id].updateEntry(stop, entryPrice)

        if self.positions[symbol].get(id, None) is None or not self.positions[symbol].get(id, None).isOpen():
            self.positions[symbol][id] = self.enterLongStopLimit(symbol, stop, entryPrice,
                                                                 qty, True, extra=extra) if long else \
                self.enterShortStopLimit(symbol, stop, entryPrice, qty, True, extra=extra)


def exitLogic(self, id, symbol, stop, is_market, bidIndex, askIndex, comment, when):
    if when and self.positions[symbol].get(id, None) is not None:
        long = 'Long' in type(self.positions[symbol][id]).__name__
        exitPrice = None if is_market else askIndex if long else bidIndex
        extra = {"id": id}
        if comment is not None:
            extra.update({'comment': comment})

        if self.positions[symbol][id].exitActive():
            if 'Limit' not in type(self.positions[symbol][id].getExitOrder()).__name__ or abs(
                    self.positions[symbol][id].getExitOrder().getLimitPrice() - self.getLastPrice(symbol)) >= 2:
                if stop is not None and 'Stop' in type(self.positions[symbol][id].getExitOrder()).__name__ and \
                        self.positions[symbol][id].getExitOrder().getStopHit():
                    self.positions[symbol][id].cancelExit()
                else:
                    self.positions[symbol][id].updateExit(stop, exitPrice, extra=extra)

        if not self.positions[symbol][id].exitActive() and self.positions[symbol][id].isOpen():
            self.positions[symbol][id].exitStopLimit(stop, exitPrice, extra=extra)


def onFinish(self, bars):
    retAnalyzer = self.getNamedAnalyzer('retAnalyzer')
    sharpeRatioAnalyzer = self.getNamedAnalyzer('sharpeRatioAnalyzer')
    drawDownAnalyzer = self.getNamedAnalyzer('drawDownAnalyzer')
    tradesAnalyzer = self.getNamedAnalyzer('tradesAnalyzer')

    self.info("Initial portfolio value: $%.2f" % self.initial_capital)
    self.info("Final portfolio value: $%.2f" % self.getResult())
    self.info("Net profit: %.2f %%" % ((self.getResult() / self.initial_capital) * 100))
    self.info("Cumulative returns: %.2f %%" % (retAnalyzer.getCumulativeReturns()[-1] * 100))
    self.info("Sharpe ratio: %.2f" % (sharpeRatioAnalyzer.getSharpeRatio(0.05)))
    self.info("Max. drawdown: %.2f %%" % (drawDownAnalyzer.getMaxDrawDown() * 100))
    self.info("Longest drawdown duration: %s" % (drawDownAnalyzer.getLongestDrawDownDuration()))

    self.info("Total trades: %d" % (tradesAnalyzer.getCount()))
    if tradesAnalyzer.getCount() > 0:
        profits = tradesAnalyzer.getAll()
        self.info("Avg. profit: $%.4f" % (profits.mean()))
        self.info("Profits std. dev.: $%.4f" % (profits.std()))
        self.info("Max. profit: $%.4f" % (profits.max()))
        self.info("Min. profit: $%.4f" % (profits.min()))
        all_returns = tradesAnalyzer.getAllReturns()
        self.info("Avg. return: %.4f %%" % (all_returns.mean() * 100))
        self.info("Returns std. dev.: %.4f %%" % (all_returns.std() * 100))
        self.info("Max. return: %.4f %%" % (all_returns.max() * 100))
        self.info("Min. return: %.4f %%" % (all_returns.min() * 100))

    self.info("Profitable trades: %d" % (tradesAnalyzer.getProfitableCount()))
    if tradesAnalyzer.getProfitableCount() > 0:
        profits = tradesAnalyzer.getProfits()
        self.info("Avg. profit: $%.4f" % (profits.mean()))
        self.info("Profits std. dev.: $%.4f" % (profits.std()))
        self.info("Max. profit: $%.4f" % (profits.max()))
        self.info("Min. profit: $%.4f" % (profits.min()))
        all_returns = tradesAnalyzer.getPositiveReturns()
        self.info("Avg. return: %.4f %%" % (all_returns.mean() * 100))
        self.info("Returns std. dev.: %.4f %%" % (all_returns.std() * 100))
        self.info("Max. return: %.4f %%" % (all_returns.max() * 100))
        self.info("Min. return: %.4f %%" % (all_returns.min() * 100))

    self.info("Unprofitable trades: %d" % (tradesAnalyzer.getUnprofitableCount()))
    if tradesAnalyzer.getUnprofitableCount() > 0:
        losses = tradesAnalyzer.getLosses()
        self.info("Avg. loss: $%.4f" % (losses.mean()))
        self.info("Losses std. dev.: $%.4f" % (losses.std()))
        self.info("Max. loss: $%.4f" % (losses.min()))
        self.info("Min. loss: $%.4f" % (losses.max()))
        all_returns = tradesAnalyzer.getNegativeReturns()
        self.info("Avg. return: %.4f %%" % (all_returns.mean() * 100))
        self.info("Returns std. dev.: %.4f %%" % (all_returns.std() * 100))
        self.info("Max. return: %.4f %%" % (all_returns.max() * 100))
        self.info("Min. return: %.4f %%" % (all_returns.min() * 100))

    self.plt.plot()


class LiveStrategy(strategy.BaseStrategy):
    def __init__(self, feed, brk, instrument):
        super(LiveStrategy, self).__init__(feed, brk)
        brk.addBitmexFeed(feed)
        feed.getNewValuesEvent().subscribe(self.__onBarsImpl)
        self.initial_capital = self.getResult()

        brk.setStrategy(self)
        self.positions = {}
        for symbol in instrument:
            self.positions[symbol] = brk.get_synced_position(self, symbol)

        retAnalyzer = returns.Returns()
        self.attachAnalyzerEx(retAnalyzer, 'retAnalyzer')
        sharpeRatioAnalyzer = sharpe.SharpeRatio()
        self.attachAnalyzerEx(sharpeRatioAnalyzer, 'sharpeRatioAnalyzer')
        drawDownAnalyzer = drawdown.DrawDown()
        self.attachAnalyzerEx(drawDownAnalyzer, 'drawDownAnalyzer')
        tradesAnalyzer = trades.Trades()
        self.attachAnalyzerEx(tradesAnalyzer, 'tradesAnalyzer')

        self.plt = plotter.StrategyPlotter(self)
        self.plt.getOrCreateSubplot("returns").addDataSeries("Net return",
                                                             retAnalyzer.getReturns())
        self.plt.getOrCreateSubplot("returns").addDataSeries("Cum. return",
                                                             retAnalyzer.getCumulativeReturns())

        self.strategy_id = brk.getStrategyId()
        if not os.path.exists(self.strategy_id + '.csv'):
            with open(self.strategy_id + '.csv', 'w') as f:
                f.write('Date,Type,Signal,Symbol,Price,Shares,PnL,Cash\n')
        if not os.path.exists(self.strategy_id + '_orders.csv'):
            with open(self.strategy_id + '_orders.csv', 'w') as f:
                f.write('Date,OrderDate,OrderId,Type,Signal,Symbol,Price,Size,Filled,Timedelta,StopHit\n')

    def cancel_and_exit(self, is_market):
        for key, symbol in self.positions.items():
            if symbol is not None:
                for key, val in symbol:
                    if val is not None:
                        val.cancelExit()
        for key, symbol in self.positions.items():
            if symbol is not None:
                for key, val in symbol:
                    if val is not None:
                        val.cancelEntry()

        ask = None if is_market else 0
        bid = None if is_market else 0

        for key, symbol in self.positions.items():
            if symbol is not None:
                for key, val in symbol:
                    if val is not None:
                        if 'Long' in type(val).__name__:
                            val.exitLimit(ask, extra={"exit": True})
                        else:
                            val.exitLimit(bid, extra={"exit": True})

    def getPositions(self):
        return self.positions

    def entry(self, id, symbol, long, qty, stop=None, is_market=False, bidIndex=0, askIndex=0, comment=None, when=True):
        entryLogic(self, id, symbol, long, qty, stop, is_market, bidIndex, askIndex, comment, when)

    def exit(self, id, symbol, stop=None, is_market=False, bidIndex=0, askIndex=0, comment=None, when=True):
        exitLogic(self, id, symbol, stop, is_market, bidIndex, askIndex, comment, when)

    def onEnterOk(self, position):
        onEnterOkCustom(self, position)

    def onEnterCanceled(self, position):
        onEnterCanceledCustom(self, position)

    def onExitOk(self, position):
        onExitOkCustom(self, position)

    def onExitCanceled(self, position):
        onExitCanceledCustom(self, position)

    def onFinish(self, bars):
        onFinish(self, bars)

    def log_orderfill(self, frequency):
        for key, val in self.positions.items():
            if val is not None:
                for key, position in val.items():
                    if position is not None:
                        if position.entryActive() and position.getEntryOrder().getSubmitDateTime() is not None:
                            with open(self.strategy_id + '_orders.csv', 'a') as f:
                                f.write(str(self.getCurrentDateTime()) + ',' + str(
                                    position.getEntryOrder().getSubmitDateTime()) + ',' + position.getEntryOrder().getId() + ',' + type(
                                    position).__name__ + ',Entry,' + position.getEntryOrder().getInstrument() + ',' + str(
                                    self.getOrderPrice(position.getEntryOrder())) + ',' + str(
                                    position.getEntryOrder().getQuantity()) + ',' + str(
                                    position.getEntryOrder().getFilled()) + ',' + str(
                                    self.getCurrentDateTime() - position.getEntryOrder().getSubmitDateTime()) + ',' + str(
                                    position.getEntryOrder().getStopHit()) + '\n')
                        if position.exitActive() and position.getExitOrder().getSubmitDateTime() is not None:
                            with open(self.strategy_id + '_orders.csv', 'a') as f:
                                f.write(str(self.getCurrentDateTime()) + ',' + str(
                                    position.getExitOrder().getSubmitDateTime()) + ',' + position.getExitOrder().getId() + ',' + type(
                                    position).__name__ + ',Exit,' + position.getExitOrder().getInstrument() + ',' + str(
                                    self.getOrderPrice(position.getExitOrder())) + ',' + str(
                                    position.getExitOrder().getQuantity()) + ',' + str(
                                    position.getExitOrder().getFilled()) + ',' + str(
                                    self.getCurrentDateTime() - position.getExitOrder().getSubmitDateTime()) + ',' + str(
                                    position.getExitOrder().getStopHit()) + '\n')

    def getOrderPrice(self, order):
        try:
            return order.getPrice()
        except:
            try:
                if order.getStopHit():
                    return order.getLimitPrice()
                else:
                    return order.getStopPrice()
            except:
                return -1

    def fillStopOrder(self, broker_, order, bar):
        # First check if the stop price was hit so the market order becomes active.
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getStrategy().getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

            if order.getStopHit():
                broker_.submitOrder(order)
                return True
        return None

    def fillStopLimitOrder(self, broker_, order, bar):
        return self.fillStopOrder(broker_, order, bar)

    def __onBarsImpl(self, dateTime, bars):
        ordersToProcess = self.getBroker().getActiveOrders()

        for order in ordersToProcess:

            bar_ = bars.getBar(order.getInstrument())
            if bar_ is not None:
                if order.isInitial() and not order.getStopHit():
                    order.process(self.getBroker(), bar_)


class SimpleBacktestStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument):
        super(SimpleBacktestStrategy, self).__init__(feed)
        self.initial_capital = self.getResult()
        self.positions = {}
        for symbol in instrument:
            self.positions[symbol] = {}

        retAnalyzer = returns.Returns()
        self.attachAnalyzerEx(retAnalyzer, 'retAnalyzer')
        sharpeRatioAnalyzer = sharpe.SharpeRatio()
        self.attachAnalyzerEx(sharpeRatioAnalyzer, 'sharpeRatioAnalyzer')
        drawDownAnalyzer = drawdown.DrawDown()
        self.attachAnalyzerEx(drawDownAnalyzer, 'drawDownAnalyzer')
        tradesAnalyzer = trades.Trades()
        self.attachAnalyzerEx(tradesAnalyzer, 'tradesAnalyzer')

        self.plt = plotter.StrategyPlotter(self)
        self.plt.getOrCreateSubplot("returns").addDataSeries("Net return",
                                                             retAnalyzer.getReturns())
        self.plt.getOrCreateSubplot("returns").addDataSeries("Cum. return",
                                                             retAnalyzer.getCumulativeReturns())

        self.strategy_id = 'backtest'
        if not os.path.exists(self.strategy_id + '.csv'):
            with open(self.strategy_id + '.csv', 'w') as f:
                f.write('Date,Type,Signal,Symbol,Price,Shares,PnL,Cash\n')

    def getPositions(self):
        return self.positions

    def entry(self, id, symbol, long, qty, stop=None, is_market=False, bidIndex=None, askIndex=None, comment=None,
              when=True):
        entryLogic(self, id, symbol, long, qty, stop, is_market, bidIndex, askIndex, comment, when)

    def exit(self, id, symbol, stop=None, is_market=False, bidIndex=None, askIndex=None, comment=None, when=True):
        exitLogic(self, id, symbol, stop, is_market, bidIndex, askIndex, comment, when)

    def onEnterOk(self, position):
        onEnterOkCustom(self, position)

    def onEnterCanceled(self, position):
        onEnterCanceledCustom(self, position)

    def onExitOk(self, position):
        onExitOkCustom(self, position)

    def onExitCanceled(self, position):
        onExitCanceledCustom(self, position)

    def onFinish(self, bars):
        onFinish(self, bars)

    def log_orderfill(self, frequency):
        pass


class OptimizerBacktestStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument):
        super(OptimizerBacktestStrategy, self).__init__(feed)
        self.positions = {}
        for symbol in instrument:
            self.positions[symbol] = {}

    def entry(self, id, symbol, long, qty, stop=None, is_market=False, bidIndex=None, askIndex=None, comment=None,
              when=True):
        entryLogic(self, id, symbol, long, qty, stop, is_market, bidIndex, askIndex, comment, when)

    def exit(self, id, symbol, stop=None, is_market=False, bidIndex=None, askIndex=None, comment=None, when=True):
        exitLogic(self, id, symbol, stop, is_market, bidIndex, askIndex, comment, when)

    def onEnterCanceled(self, position):
        remove_position(self, position)

    def log_orderfill(self, frequency):
        pass
