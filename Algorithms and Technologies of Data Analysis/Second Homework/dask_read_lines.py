from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import time

def factorizer(row):
    result = []
    d = 2
    while d * d <= row:
        if row % d == 0:
            result.append(d)
            row //= d
        else:
            d += 1
    if row > 1:
        result.append(row)
    return len(result)

def factorizer2(df):
    return df.apply(factorizer)

if __name__ == "__main__":
    start_time = time.time()
    client = Client(n_workers=1, threads_per_worker=20, processes=True, memory_limit='10GB')
    client

    # df = dd.read_csv("C:/Users/user/Downloads/line_numbers")
    # dfc = df['Number'].compute()
    # res_fin = dfc.apply(factorizer)
    # print(res_fin.sum())
    df = pd.read_csv("C:/Users/user/Downloads/line_numbers")
    dfc = dd.from_pandas(df, npartitions=10)
    res_fin = dfc['Number'].map_partitions(factorizer2, meta=(None, 'int32'))
    print(res_fin.sum().compute())
    whole_time = time.time() - start_time
    print("Programm has finished execution in: ", whole_time , " seconds")