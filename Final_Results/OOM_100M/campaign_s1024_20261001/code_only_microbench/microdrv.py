import sys, time, random, multiprocessing as mp
import psycopg
from psycopg import errors
host, port, clients, secs, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), int(sys.argv[5])
def worker(k, q):
    r = random.Random(seed * 1000 + k)
    c = psycopg.connect(host=host, port=port, user='postgres', dbname='postgres', autocommit=False)
    cur = c.cursor()
    cur.execute("SHOW transaction_isolation"); iso = cur.fetchone()[0]; c.commit()
    done = retries = 0
    end = time.time() + secs
    while time.time() < end:
        w = r.randint(1, 100); ids = [r.randint(1, 20000) for _ in range(10)]
        while True:
            try:
                cur.execute("SELECT stock_tx(%s, %s)", (w, ids)); c.commit(); done += 1; break
            except (errors.SerializationFailure, errors.DeadlockDetected):
                c.rollback(); retries += 1
                if time.time() >= end: break
    q.put((done, retries, iso)); c.close()
q = mp.Queue(); ps = [mp.Process(target=worker, args=(k, q)) for k in range(clients)]
t0 = time.time(); [p.start() for p in ps]; res = [q.get() for _ in ps]; [p.join() for p in ps]
el = time.time() - t0
d = sum(x[0] for x in res); rt = sum(x[1] for x in res)
print(f"tps={d/secs:.1f} committed={d} retries={rt} isolation={res[0][2]} elapsed={el:.1f}")
