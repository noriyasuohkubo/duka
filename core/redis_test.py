import csv
import time
from datetime import datetime,  timedelta
from os.path import join
import redis
import traceback
import json

r = redis.Redis(host='localhost', port=6379, db=3)
result = r.zrangebyscore("EURUSD",100,100)
if len(result) != 0:
    print("OK")
print("result: ", result)

with r.pipeline() as pipe:
    #pipe.watch("EURUSD")
    tmp_val = r.zrangebyscore("EURUSD",100,100)
    if len(tmp_val) == 0:
        pipe.zadd("EURUSD","bbb",100)
    pipe.execute()
