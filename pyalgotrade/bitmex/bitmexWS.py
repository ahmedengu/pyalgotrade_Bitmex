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

import json
import threading

import websocket

from six.moves.urllib.parse import urlencode

import pyalgotrade
from pyalgotrade.websocket import client
import pyalgotrade.logger
import time, urllib, hmac, hashlib
from pyalgotrade.bitmex import common

logger = pyalgotrade.logger.getLogger("bitmexWS")


class Event(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict
        self.__data = eventDict.get("data")
        # if self.__data is not None and dataIsJSON:
        #     self.__data = json.loads(self.__data)

    def __str__(self):
        return str(self.__eventDict)

    def getDict(self):
        return self.__eventDict

    def getData(self):
        return self.__data

    def getType(self):
        return self.__eventDict.get("table")

    def getAction(self):
        return self.__eventDict.get("action")


class PingKeepAliveMgr(client.KeepAliveMgr):
    def __init__(self, wsClient, maxInactivity, responseTimeout):
        super(PingKeepAliveMgr, self).__init__(wsClient, maxInactivity, responseTimeout)

    # Override to send the keep alive msg.
    def sendKeepAlive(self):
        logger.debug("Sending ping.")
        self.getWSClient().sendPing()

    # Return True if the response belongs to a keep alive message, False otherwise.
    def handleResponse(self, msg):
        ret = msg.get("table") == "pong"
        if ret:
            logger.debug("Received pong.")
        return ret


class WebSocketClient(client.WebSocketClientBase):

    def generate_nonce(self):
        return int(round(time.time() * 1000))

    # Generates an API signature.
    # A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
    # Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    # and the data, if present, must be JSON without whitespace between keys.
    #
    # For example, in psuedocode (and in real code below):
    #
    # verb=POST
    # url=/api/v1/order
    # nonce=1416993995705
    # data={"symbol":"XBTZ14","quantity":1,"price":395.01}
    # signature = HEX(HMAC_SHA256(secret, 'POST/api/v1/order1416993995705{"symbol":"XBTZ14","quantity":1,"price":395.01}'))
    def generate_signature(self, secret, verb, url, nonce, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsedURL = urllib.parse.urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        # print "Computing HMAC: %s" % verb + path + str(nonce) + data
        message = (verb + path + str(nonce) + data).encode('utf-8')

        signature = hmac.new(secret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()
        return signature

    def __get_auth(self, api_key=None, api_secret=None):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if api_key:
            common.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            nonce = self.generate_nonce()
            return [
                ("api-nonce", str(nonce)),
                ("api-signature", self.generate_signature(api_secret, 'GET', '/realtime', nonce, '')),
                ("api-key", api_key)
            ]
        else:
            common.logger.info("Not authenticating.")
            return []

    def __get_url(self, endpoint, symbol):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["orderBookL2_25", "trade"]
        genericSubs = ["execution", "order", "position", "margin"]
        subscriptions = []
        for aSymbol in symbol:
            subscriptions += [sub + ':' + aSymbol for sub in symbolSubs]
        subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)

    def __on_message(self, ws, message):
        '''Handler for parsing WS messages.'''
        logger.info(message)

    def __on_error(self, ws, error):
        logger.debug("Error : %s" % error)

    def __on_open(self, ws):
        '''Called when the WS opens.'''
        logger.info("Websocket Opened.")

    def __on_close(self, ws):
        '''Called on websocket close.'''
        logger.info('Websocket Closed')

    def __init__(self, endpoint, symbol, api_key=None, api_secret=None, maxInactivity=10, responseTimeout=10):
        super(WebSocketClient, self).__init__(url=self.__get_url(endpoint, symbol),
                                              headers=self.__get_auth(api_key, api_secret))

        self.setKeepAliveMgr(PingKeepAliveMgr(self, maxInactivity, responseTimeout))

    def sendEvent(self, eventType, eventData):
        msgDict = {"event": eventType}
        if eventData:
            msgDict["data"] = eventData
        msg = json.dumps(msgDict)
        self.send(msg, False)

    def sendPing(self):
        self.send("ping", False)

    def sendPong(self):
        self.send("pong", False)

    def onMessage(self, msg):
        if msg.get('error') is not None:
            self.onError(Event(msg))
        elif msg.get("table") == "ping":
            self.sendPong()
        elif msg.get('info') is not None:
            self.onConnectionEstablished(Event(msg))
        elif msg.get('subscribe') is not None:
            self.onSubscriptionSucceeded(Event(msg))
        elif msg.get('table') is None:
            self.onUnknownEvent(Event(msg))

    def onConnectionEstablished(self, event):
        pass

    def onSubscriptionSucceeded(self, event):
        pass

    def onError(self, event):
        raise NotImplementedError()

    def onUnknownEvent(self, event):
        raise NotImplementedError()
