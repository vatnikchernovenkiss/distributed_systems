import redis
import pandas as pd
import time

r1 = redis.Redis(host='127.0.0.1', port=6379)
r2 = redis.Redis(host='127.0.0.1', port=6376)

def ETL_data(csv):
    time_beg = time.time()
    df = pd.read_csv(csv, sep='\t')
    n_rec = df.shape[0]
    rows = df.values
    for i in range(n_rec // 2):
        row = rows[i]
        key = row[0]
        value =  ' '.join(list(map(lambda x: str(x), row[1:])))
        r1.set(key, value)

    for i in range(n_rec // 2, n_rec):
        row = rows[i]
        key = row[0]
        value =  ' '.join(list(map(lambda x: str(x), row[1:])))
        r2.set(key, value)
    print('Time taken to load data:', round(time.time() - time_beg, 1))

ETL_data('svtl_meteo_20190424-20220101.csv')