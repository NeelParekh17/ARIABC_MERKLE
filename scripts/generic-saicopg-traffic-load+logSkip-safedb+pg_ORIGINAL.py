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
reqPause = 1.0 #/RATE;
print(" arg1 a2 == ", sys.argv[1], sys.argv[2]);
print(" arg3 a4 pause == ", sys.argv[3], sys.argv[4], reqPause);
DB_NAME = sys.argv[1]
connStr = "postgresql://"+DB_USER + ":" + DB_PASS + "@" + DB_HOST+":"+ DB_PORT + "/" + DB_NAME

totalWaitTime = 0.0
pool = ConnectionPool(connStr, min_size = NUM)
#, connection_class= <class 'psycopg.Connection'>) 

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
    #print("    thread-id=" +str(threading.get_ident()) + " seq#==  " + numstr+str(currNum))
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
    #print(arg)
    #time.sleep(0.0012) # this fixes most dup-key errors - now only 1 in 1k !!
    qCount, q  = getSeqNum(0)
    while (qCount == -1):
        time.sleep(0.00050)
        qCount,q  = getSeqNum(0)
    if (q == ''):
        return
    execTx(qCount, q)

def execTx(qCount, line):

 #print("0timestamp0 = " + str(ts1) + " line= " + line)
 #sys.stdout.flush()
 try:
  ts1 = time.time()
  serialErr = 1
  pfx = 's '
  mHash = getSeqHash(qCount)
  if(line != message_dict[qCount]):
    print(" qCount = " + str(qCount) + ' ' + message_dict[qCount] + ' whereas line= ' + line)
  if((qCount % LOG_SKIP) < NUM):
    print("timestamp1 = " + str(ts1) + " line= " + line)
  with pool.connection() as conn:
    #    conn.execute(line)
    cur1 = conn.cursor()
    if (DB_TYPE == 1):
        cur1.execute(pfx + mHash + ' ' + line  )
    else:
        cur1.execute(pfx + line  )
  conn.commit()
  if((qCount % LOG_SKIP) < NUM):
    print("  2timestamp1 = " + str(ts1) + " tid= " + str(threading.get_ident()))
  sys.stdout.flush()
  #while (serialErr == true):
      #print("Database connected successfully")
  #print("3timestamp1 = " + str(ts1) )
  serialErr = 0
      #break
  if((qCount % LOG_SKIP) < NUM):
    print(cur1.statusmessage)
    if(line[0] =='s') or (line[0] =='S'):
      print(str(cur1.fetchall()))
  ts2 = time.time()
  #print("    timestamp2 = " + str(ts2) )
  if((qCount % LOG_SKIP) < NUM):
    print("    time diff (ms) = " + str((ts2 - ts1)*1000) + " tid= " + str(threading.get_ident()) )
  waitTime = reqPause - (ts2-ts1)
  waitTime = 0
  #global totalWaitTime
  if(waitTime > 0) :
       time.sleep(waitTime);
       mutex.acquire()
       totalWaitTime += waitTime
       mutex.release()
       #print("     timeWait = " + str(waitTime) )
  #else:
       #print("     NO...timeWait = " + str(waitTime) )
 #except psycopg.errors.SerializationFailure:
 #except (Exception, psycopg2.DatabaseError) as error:
  #session.rollback()
 # print("pgcode: " + str(error.pgcode))

 # if isinstance(error, psycopg2.errors.SerializationFailure):
 #   print("got instance of the SerializationFailure subclass")

 # else:
 #     print("got instance of the other subclass")
 #     raise error
 #except psycopg2.Error as err:
 #     print("Database psycopg.errors.SerializationFailure... ")
 #     excpStr = traceback.format_exc()
 #     if excpStr.find("SerializationFailure"):
 #       print(" found it to recurse...")
 #     print(excpStr)
 except:
      print("Database not connected successfully... chk rerun" + " tid= " + str(threading.get_ident()))
      #pool.putconn(conn)
      print("   chk rerun stoPro= " + line)
      excpStr = traceback.format_exc()
      print("    str==  "+ excpStr)
      found = excpStr.find("SerializationFailure")
      if (found != -1):
        print("   rerun - found it to recurse...")
        #execStoPro(line)
 sys.stdout.flush()

    
#a = executor.submit(my_function)
allQueries = []
k = 0
with open(sys.argv[2], 'r') as fptr:
     count = 0
     for line in fptr:
        name = line.strip()
        count += 1
        if((count % LOG_SKIP) < NUM):
           print("next Tx = " + name + '  ')
        message_dict[inputSeqNum] = name
        inputSeqNum += 1
        #execStoPro(line)
        k = k + 1
        #rows1 = cur1.fetchall()
        #for data in rows1:
        #  print("result " + str(data) )
        #if(k%10 == 0): # full
        #  break;
            # print(str(k) + " " + cur1.statusmessage)
message_dict[inputSeqNum] = ''
inputSeqNum += 1
tStart = time.time()
poolFutures = []

with ThreadPoolExecutor(max_workers= NUM) as execPool: 
  mapAll = 1
  if  mapAll == 1:
    # allQ change to msg-dict ; fewer refleaks than submit...
    results = execPool.map(execWrapper, itertools.repeat(None, inputSeqNum))
    count = 0
    for result in results:
        count += 1
        #if((count % LOG_SKIP) < NUM):
        #    print(result)

tEnd = time.time()
sys.stdout.flush()
print("overall time taken (millisec) = " + str((tEnd - tStart)*1000))
print(" total wait time (ms) " + str(totalWaitTime))

