import csv
import time
from datetime import datetime,  timedelta 
from os.path import join
import redis
from .candle import Candle
from .utils import TimeFrame, stringify, Logger, valid_symbol
import traceback
import json
from decimal import Decimal

TEMPLATE_FILE_NAME = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}.csv"


def format_float(number):
    return format(number, '.6f')

class CSVFormatter(object):
    COLUMN_TIME = 0
    COLUMN_ASK = 1
    COLUMN_BID = 2
    COLUMN_ASK_VOLUME = 3
    COLUMN_BID_VOLUME = 4

def write_tick(writer, tick):
    writer.writerow(
        {'time': tick[0],
         'ask': format_float(tick[1]),
         'bid': format_float(tick[2]),
         'ask_volume': tick[3],
         'bid_volume': tick[4]})

def write_candle(writer, candle):
    writer.writerow(
        {'time': stringify(candle.timestamp),
         'open': format_float(candle.open_price),
         'close': format_float(candle.close_price),
         'high': format_float(candle.high),
         'low': format_float(candle.low),
         'ask_volume': format_float(candle.ask_volume),
         'bid_volume': format_float(candle.bid_volume)})

class CSVDumper:
    def __init__(self, symbol, timeframe, start, end, folder, mergin, header=False):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.folder = folder
        self.include_header = header
        self.buffer = {}
        self.mergin = mergin
        db_no = valid_symbol(symbol)
        self.r = redis.Redis(host='localhost', port=6379, db=db_no)
        self.db = self.symbol
        if self.mergin !=0:
            self.db = self.symbol + str(self.mergin)
    def get_header(self):
        if self.timeframe == TimeFrame.TICK:
            return ['time', 'ask', 'bid', 'ask_volume', 'bid_volume']
        return ['time', 'open', 'close', 'high', 'low', 'ask_volume', 'bid_volume']

    def append(self, day, ticks):
        previous_key = None
        current_ticks = []
        ask_volumes = []
        bid_volumes = []
        ask_current_ticks = []
        self.buffer[day] = []
        tmp_buffer = {}
        last_hour = ticks[len(ticks)-1][0].hour +1
        hour_delta = ticks[0][0].hour  
        print("tick1: ", ticks[0])    
        print("ticklength:" , len(ticks))
        print("lasttick:", ticks[len(ticks)-1])
        for tick in ticks:
            if self.timeframe == TimeFrame.TICK:
                self.buffer[day].append(tick)
            else:
                ts = time.mktime(tick[0].timetuple())
                key = int(ts - (ts % self.timeframe))
                if self.mergin != 0: 
                    if (ts % self.timeframe) < self.mergin:
                        key = int(ts - self.timeframe - (ts % self.timeframe) + self.mergin)
                    else:
                        key = int(ts - (ts % self.timeframe) + self.mergin)
                if previous_key != key and previous_key is not None:
                    tmp_buffer[previous_key] = {"current_ticks":current_ticks, "ask_current_ticks":ask_current_ticks,"ask_volumes":ask_volumes, "bid_volumes":bid_volumes}
                    """ 
                    n = int((key - previous_key) / self.timeframe)
                    for i in range(0, n):
                        self.buffer[day].append(
                            Candle(self.symbol, previous_key + i * self.timeframe, self.timeframe, current_ticks))
                    """
                    current_ticks = []
                    ask_current_ticks = []
                    ask_volumes = []
                    bid_volumes = []
                #closeには仲値を入れる
                mid = float((Decimal(str(tick[1])) + Decimal(str(tick[2]))) / Decimal("2"))
                ask_current_ticks.append(tick[1])
                current_ticks.append(mid)
                ask_volumes.append(tick[3])
                bid_volumes.append(tick[4])
                previous_key = key
                
        if self.timeframe != TimeFrame.TICK:
            tmp_buffer[previous_key] = {"current_ticks":current_ticks, "ask_current_ticks":ask_current_ticks, "ask_volumes":ask_volumes, "bid_volumes":bid_volumes}
            #self.buffer[day].append(Candle(self.symbol, previous_key, self.timeframe, current_ticks))
           
            dtime = datetime(day.year, day.month, day.day , hour=hour_delta, minute=0,second=0)        
            dtime = dtime + timedelta(seconds = self.mergin) 
            #day + timedelta(hours=hour_delta)       
            print("day: " + dtime.strftime('%Y-%m-%d  %H:%M:%S'))
            #print("tmp_buffer_len:", len(tmp_buffer))
            first_day_str = datetime.fromtimestamp(time.mktime(dtime.timetuple()))
            prev_dtime = dtime - timedelta(seconds = self.timeframe)
            prev_dtime_int = int(time.mktime(prev_dtime.timetuple()))
            print("first_day: ", first_day_str)
            result = self.r.zrevrange(self.db, 0, 0, withscores=True) 
            result2 = self.r.zrangebyscore(self.db,prev_dtime_int,prev_dtime_int)
            #keys = self.r.keys().sort(reverse=True)
            print("first: ", sorted(tmp_buffer)[0])
            tmp_prev_value = tmp_buffer.get(sorted(tmp_buffer)[0])
            prev_value = {"current_ticks":[tmp_prev_value.get("current_ticks")[0],], "ask_current_ticks":[tmp_prev_value.get("ask_current_ticks")[0],], "ask_volumes":[], "bid_volumes":[]}
            print("last_hour: ", last_hour)
            if len(result2) != 0:
                tmp_v = json.loads(result2[0].decode('utf-8'))
                prev_value = {"current_ticks":[tmp_v.get("close"),], "ask_current_ticks":[tmp_v.get("ask"),], "ask_volumes":[], "bid_volumes":[]}
            elif len(result) !=0 and result[0][1] <= prev_dtime_int:
                tmp_v2 = json.loads(result[0][0].decode('utf-8'))
                prev_value = {"current_ticks":[tmp_v2.get("close"),], "ask_current_ticks":[tmp_v2.get("ask"),], "ask_volumes":[], "bid_volumes":[]}
            """
            if keys:
                for key in keys:
                    if(key < first_day_str):
                        tmp_v = self.r.get(key)
                        prev_value = {"current_ticks":[tmp_v.get("close"),], "ask_volumes":[], "bid_volumes":[]}
                        break
            """
            index_n = 1
            prev_hour = 0
            while True:
                prev_hour = dtime.hour
                tmp_time = int(time.mktime(dtime.timetuple()))
                """
                if index_n == 1 or (index_n % 100) ==0:
                    print("hour: ", dtime.hour, "  ", index_n, last_hour)
                """
                reg_data = tmp_buffer.get(tmp_time, prev_value)        
                tmp_current = reg_data.get("current_ticks")
                tmp_ask = reg_data.get("ask_current_ticks")
                prev_value = {"current_ticks":[tmp_current[len(tmp_current)-1],],"ask_current_ticks":[tmp_ask[len(tmp_ask)-1],], "ask_volumes":[], "bid_volumes":[]}
                self.buffer[day].append(Candle(self.symbol, tmp_time, self.timeframe, \
                                               reg_data.get("current_ticks"), \
                                               reg_data.get("ask_current_ticks"), \
                                               reg_data.get("ask_volumes"), \
                                               reg_data.get("bid_volumes")))
                dtime = dtime + timedelta(seconds = self.timeframe)
                index_n += 1
                if(dtime.hour == last_hour or (dtime.hour == 0  and prev_hour == 23)): 
                    break
            
            with self.r.pipeline() as pipe:
                try:
                    for cand in self.buffer[day]:
                        #parent = stringify(cand.timestamp)
                        score = cand.timestamp
                        child = {'open': cand.open_price,
                                 'close': cand.close_price,
                                 'ask': cand.ask_price,
                                 'high': cand.high,
                                 'low': cand.low,
                                 'ask_volume': cand.ask_volume,
                                 'bid_volume': cand.bid_volume,
                                 'time': stringify(cand.timestamp)}
                        #pipe.hmset(parent, child)
                        tmp_val = self.r.zrangebyscore(self.db,cand.timestamp,cand.timestamp)
                        if len(tmp_val) == 0:
                            pipe.zadd(self.db,json.dumps(child),score) 
                    pipe.execute()
                except:
                    print("db regist  error occured:", traceback.format_exc());
                else:
                    print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"), "OK")
                finally:
                    pipe.reset()

    def dump(self):
        file_name = TEMPLATE_FILE_NAME.format(self.symbol,
                                              self.start.year, self.start.month, self.start.day,
                                              self.end.year, self.end.month, self.end.day)

        Logger.info("Writing {0}".format(file_name))
        """
        with open(join(self.folder, file_name), 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.get_header())
            if self.include_header:
                writer.writeheader()
            for day in sorted(self.buffer.keys()):
                for value in self.buffer[day]:
                    if self.timeframe == TimeFrame.TICK:
                        write_tick(writer, value)
                    else:
                        write_candle(writer, value)
        """
        Logger.info("{0} completed".format(file_name))
