from datetime import datetime
from decimal import Decimal

class Candle:
    def __init__(self, symbol, timestamp, timeframe, bids, asks, ask_volumes, bid_volumes):
        #minInd = bids.index(min(bids))
        #maxInd = bids.index(max(bids))
        self.symbol = symbol
        self.timestamp = timestamp
        self.timeframe = timeframe
        self.open_price = bids[0]
        self.close_price = bids[len(bids) -1]
        self.ask_price = asks[len(asks) - 1]
        self.high = max(bids)
        self.low = min(bids)
        self.ask_volume = sum(ask_volumes)
        self.bid_volume = sum(bid_volumes)

    def __str__(self):
        return str(datetime.fromtimestamp(self.timestamp)) + " [" + str(self.timestamp) + "] " \
               + "-- " + self.symbol + " -- " \
               + "{ H:" + str(self.high) + " L:" + str(self.low) + " O: " \
               + str(self.open_price) + " C: " + str(self.close_price) + " }"

    def __eq__(self, other):
        return self.symbol == other.symbol \
               and self.timestamp == other.timestamp \
               and self.timeframe == other.timeframe \
               and self.close_price == other.close_price \
               and self.ask_price == other.ask_price \
               and self.open_price == other.open_price \
               and self.high == other.high \
               and self.low == other.low

    def __repr__(self):
        return self.__str__()
