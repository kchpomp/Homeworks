import time
import pandas as pd
import numpy as np
from multiprocessing import Process, Pool
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

start_time = time.time()
# Open a file
test = open("C:/Users/user/Downloads/line_numbers", "r")
# Create a DataFrame
daf = pd.read_csv(test, index_col=0, low_memory=False)
test.close()

# Split dataframe on multiple parts
a, b, c, k, e, f, g, h = np.array_split(daf, 8)


# Initialize a function that factorizes number
def factorizer(n):
    result = []
    d = 2
    while d * d <= n:
        if n % d == 0:
            result.append(d)
            n //= d
        else:
            d += 1
    if n > 1:
        result.append(n)
    return result


chk = threading.RLock()


def fact(dst):
    # Fatorize all numbers in data frame and append to empty lists
    chk.acquire()
    # Initialize empty list
    res = []

    for index, row in dst.iterrows():
        res.append(factorizer(row['Number']))

    # print(len(res))
    chk.release()
    return res


def comparison(l1, l2):

    l1.sort()
    l2.sort()

    if l1 == l2:
        return "Equal"
    else:
        return "Non equal"


if __name__ == "__main__":
    start_time1 = time.time()
    with ProcessPoolExecutor(8) as pool:

        fut1 = pool.submit(fact, a)
        fut2 = pool.submit(fact, b)
        fut3 = pool.submit(fact, c)
        fut4 = pool.submit(fact, k)
        fut5 = pool.submit(fact, e)
        fut6 = pool.submit(fact, f)
        fut7 = pool.submit(fact, g)
        fut8 = pool.submit(fact, h)

    fin_list = fut1.result() + fut2.result() + fut3.result() + fut4.result() + fut5.result() + fut6.result() + fut7.result() + fut8.result()
    # print(fin_list)
    # print("Fin list len is: ", len(fin_list))
    res_list = [item for sublist in fin_list for item in sublist]
    print("Final length is: ", len(res_list))
    whole_time1 = time.time() - start_time
    print("The process took ", whole_time1, " seconds")

    start_time2 = time.time()
    # Initialize empty list
    res_full1 = []

    # Fatorize all numbers in data frame and append to empty lists
    for index, row in daf.iterrows():
        res_full1.append(factorizer(row['Number']))

    # Convert list of lists to the list
    res_lst1 = [item for sublist in res_full1 for item in sublist]

    # Print number of simple digits
    print("Len if list without multiprocessing with 1 list is: ", len(res_lst1))

    whole_time2 = time.time() - start_time2
    print("The process took ", whole_time2, " seconds")

    print("Performance difference between 1 and 2 is: ", whole_time2 - whole_time1, " seconds")

    start_time3 = time.time()
    # Initialize empty list
    res_full2 = []

    for i in [a, b, c, k, e,  f, g, h]:
        # Fatorize all numbers in data frame and append to empty lists
        for index, row in i.iterrows():
            res_full2.append(factorizer(row['Number']))

    # Convert list of lists to the list
    res_lst2 = [item for sublist in res_full2 for item in sublist]

    # Print number of simple digits
    print("Len if list without multiprocessing with 8 lists is: ", len(res_lst2))

    whole_time3 = time.time() - start_time3
    print("The process took ", whole_time3, " seconds")

    print("Performance difference between method 1 and 3 is: ", whole_time3 - whole_time1, " seconds")

    res_list.sort()
    res_lst1.sort()
    res_lst2.sort()

    print("Methods 1 and 2 result is: ", comparison(res_list, res_lst1))
    print("Methods 1 and 3 result is: ", comparison(res_list, res_lst2))
    print("Methods 2 and 3 result is: ", comparison(res_lst1, res_lst2))
