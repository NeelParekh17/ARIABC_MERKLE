#!/usr/bin/python3

import psycopg2
import sys
import time
import itertools
from threading import Thread, Lock
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from psycopg_pool import ConnectionPool

mutex = Lock()
mutex_seqnum = Lock()
procSeqNum = 0
inputSeqNum = 0
message_dict = {}
totalWaitTime = 0.0

DB_USER = "postgres"
DB_PORT = "5438"
DB_NAME = "postgres"
DB_PASS = ""
#DB_HOST = "10.129.148.129"
DB_HOST = "localhost"
RATE = 800
NUM = 4
LOG_SKIP = 1000
 
#print("arglen== " , len(sys.argv))
if(len(sys.argv) < 4):
  print("Err: missing arguments... ")
  print("Usage: python3 <scriptname> <dbName> <queryDataFile> <pg0Safe1Aria2> <num-thread> [<<Qrate>]");
  exit(-1)

NUM = int(sys.argv[4])
DB_TYPE = int(sys.argv[3])
if(len(sys.argv) > 5):
  RATE = int(sys.argv[5])
reqPause = 1.0 / RATE
print(" arg1 a2 == ", sys.argv[1], sys.argv[2]);
print(" arg3 a4 pause == ", sys.argv[3], sys.argv[4], reqPause);
DB_NAME = sys.argv[1]
connStr = "postgresql://"+DB_USER + ":" + DB_PASS + "@" + DB_HOST+":"+ DB_PORT + "/" + DB_NAME

pool = ConnectionPool(connStr, min_size = NUM, max_size = NUM * 2)

def getSeqHash(seqNum):

    num = seqNum
    nd = 0
    for len in range(0, 8):
      num =  (int) (num/10)
      nd = nd + 1
      if(num == 0):
          break
    numstr = ""
    for len in range(0, 8-nd):
      numstr = numstr + '0'
    return numstr+str(seqNum)

def getSeqNum(dummy):
    global procSeqNum
    global inputSeqNum

    mutex_seqnum.acquire()
    currNum = procSeqNum
    if(procSeqNum == inputSeqNum):
        mutex_seqnum.release()
        return -1, ''
    procSeqNum = procSeqNum + 1
    mutex_seqnum.release()
    if((currNum % LOG_SKIP) < NUM):
      print(f"Thread: {threading.current_thread().name} | ID: {threading.get_ident()} | seq: {procSeqNum}")

    return currNum, message_dict[currNum]

def execWrapper(dummy):
    qCount, q  = getSeqNum(0)
    while (qCount == -1):
        time.sleep(0.00050)
        qCount,q  = getSeqNum(0)
    if (q == ''):
        return
    execTx(qCount, q)

def execTx(qCount, line):
 global totalWaitTime
 
 ts1 = time.time()
 serialErr = 1
 pfx = 's '
 mHash = getSeqHash(qCount)
 
 if(line != message_dict[qCount]):
    print(" qCount = " + str(qCount) + ' ' + message_dict[qCount] + ' whereas line= ' + line)
 if((qCount % LOG_SKIP) < NUM):
    print("timestamp1 = " + str(ts1) + " line= " + line)
 
 try:
  # IMPROVED: Better connection and cursor management
  conn = None
  cur1 = None
  
  try:
    conn = pool.getconn()
    cur1 = conn.cursor()
    
    # Execute query
    if (DB_TYPE == 1):
        cur1.execute(pfx + mHash + ' ' + line)
    else:
        cur1.execute(pfx + line)
    
    # Fetch results BEFORE commit for SELECT queries
    result_message = cur1.statusmessage if cur1.statusmessage else "NO STATUS"
    result_data = None
    
    if((qCount % LOG_SKIP) < NUM):
      if (line.strip().upper().startswith('SELECT') or 
          line.strip().upper().startswith('S SELECT')):
        try:
          result_data = cur1.fetchall()
        except Exception as e:
          print(f"  Warning: Could not fetch results: {e}")
    
    # Now commit
    conn.commit()
    
    if((qCount % LOG_SKIP) < NUM):
      print("  2timestamp1 = " + str(ts1) + " tid= " + str(threading.get_ident()))
      print(result_message)
      if result_data is not None:
        print(str(result_data))
    
    serialErr = 0
    
  finally:
    # IMPROVED: Always return connection to pool
    if conn is not None:
      pool.putconn(conn)
  
  sys.stdout.flush()
  
  ts2 = time.time()
  if((qCount % LOG_SKIP) < NUM):
    print("    time diff (ms) = " + str((ts2 - ts1)*1000) + " tid= " + str(threading.get_ident()))
  
  waitTime = reqPause - (ts2-ts1)
  
  if(waitTime > 0):
       time.sleep(waitTime)
       mutex.acquire()
       totalWaitTime += waitTime
       mutex.release()
 
 except psycopg2.DatabaseError as err:
      print("Database error... " + " tid= " + str(threading.get_ident()))
      print("   chk rerun stoPro= " + line)
      excpStr = traceback.format_exc()
      print("    str==  "+ excpStr)
      found = excpStr.find("SerializationFailure")
      if (found != -1):
        print("   rerun - found it to recurse...")
        # Could implement retry logic here
 except psycopg2.Error as err:
      print("psycopg2 error: " + str(err) + " tid= " + str(threading.get_ident()))
      print("   chk rerun stoPro= " + line)
      traceback.print_exc()
 except Exception as e:
      print("Unexpected error: " + str(e) + " tid= " + str(threading.get_ident()))
      print("   chk rerun stoPro= " + line)
      traceback.print_exc()
 
 sys.stdout.flush()

    
allQueries = []
k = 0

try:
    with open(sys.argv[2], 'r') as fptr:
         count = 0
         for line in fptr:
            name = line.strip()
            count += 1
            if((count % LOG_SKIP) < NUM):
               print("next Tx = " + name + '  ')
            message_dict[inputSeqNum] = name
            inputSeqNum += 1
            k = k + 1
except FileNotFoundError:
    print(f"Error: Query data file '{sys.argv[2]}' not found")
    exit(-1)

message_dict[inputSeqNum] = ''
inputSeqNum += 1
tStart = time.time()
poolFutures = []

try:
    with ThreadPoolExecutor(max_workers= NUM) as execPool: 
      mapAll = 1
      if mapAll == 1:
        results = execPool.map(execWrapper, itertools.repeat(None, inputSeqNum))
        count = 0
        for result in results:
            count += 1
finally:
    pool.close()

tEnd = time.time()
sys.stdout.flush()
print("overall time taken (millisec) = " + str((tEnd - tStart)*1000))
print(" total wait time (ms) " + str(totalWaitTime * 1000))