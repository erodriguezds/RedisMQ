import sys
import time
import string
import random
import asyncio
import redis

def randstr(chars=string.ascii_uppercase + string.digits, minsize=100, maxsize=200):
    size = random.randint(minsize, maxsize)
    return ''.join(random.choice(chars) for _ in range(size))

r = redis.Redis(
    host='127.0.0.1',
    port=6379
)

def produce(keys, total):
    max_k = len(keys) - 1
    for _ in range(0,total):
        k = random.randint(0, max_k)
        key = keys[k]
        r.execute_command("RQ.PUSH", key, randstr())


async def producer(total, bulksize, keys):
    batch = []
    max_k = len(keys) - 1
    
    for i in range(0,len(keys)):
        batch.append([])
    
    while total > 0:
        k = random.randint(0, max_k)
        key = keys[k]
        batch[k].append(randstr())
        if len(batch[k]) >= bulksize:
            print(f"RQ.PUSH {key} {batch[k]}")
            r.execute_command("RQ.PUSH", key, *batch[k])
            batch[k] = []
        total -= 1

    for k in range(len(keys)):
        if len(batch[k]) > 0:
            key = keys[k]
            print(f"RQ.PUSH {key} {batch[k]}")
            r.execute_command("RQ.PUSH", key, *batch[k])
            batch[k] = []

async def consumer(cid, keys, popcount, timeout):
    total = 0
    while True:
        jobs = r.execute_command("RQ.POP", "COUNT", popcount, "BLOCK", timeout, *keys)
        if not jobs or len(jobs) == 0:
            break
        for job in jobs:
            key, id, payload = job
            print(f"Processing job id {id} from queue {key}, payload: {payload}. Total jobs processed: {total}")
            await asyncio.sleep(random.randint(1,10)/100)
            #print(f"Acknowledge job id '{id}', payload: '{payload}'")
            r.execute_command("RQ.ACK", key, id)
            total += 1
            #print(f"Total jobs consumed: {total}")

async def main():
    if(len(sys.argv) < 4):
        print("Usage:\n   py producer.py  <total>  <bulksize>  <key1>  [ <key2> [ ... ] ]\n")
        return
    
    keys = []
    total = int(sys.argv[1])
    bulksize = int(sys.argv[2])
    for i in range(3, len(sys.argv)):
        keys.append(sys.argv[i])
    print(f"Producing {total} jobs in bulks of {bulksize}. Keys: {keys}")
    producer_task = asyncio.create_task(producer(total, bulksize, keys))
    
    print(f"started at {time.strftime('%X')}")

    # Wait until both tasks are completed (should take
    # around 2 seconds.)
    await producer_task

    print(f"finished at {time.strftime('%X')}")
    #print(f"Produce {sys.argv[1]} jobs per second during {sys.argv[2]} seconds")

asyncio.run(main())
