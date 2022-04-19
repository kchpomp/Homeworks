#!/usr/bin/env python
# coding: utf-8

# In[22]:


import numpy as np
import time
import os
import mmap
import multiprocessing
import threading
import tqdm
import concurrent.futures as pool


# In[23]:


#Variant without threading
start_time = time.time()
file=open("test_huge_file","r+b")
number_array = np.fromfile(file, dtype=np.dtype('uint32').newbyteorder('B'))
max_value = np.ndarray.max(number_array)
print("Find max element takes %s seconds" % (time.time() - start_time))
min_value = np.ndarray.min(number_array)
print("Find min element takes %s seconds " % (time.time() - start_time))
array_sum = 0
for i in range(len(number_array)):
    array_sum += int(number_array[i])
print("Min value is: ", min_value)
print("Max value is: ", max_value)
print("Sum of all elements is: ", array_sum)
file.close()
whole_time = time.time() - start_time
print("Whole process takes %s seconds" % (whole_time))


# In[24]:


#Variant that uses threading
start_time = time.time()
time_find_max = 0
time_find_min= 0
whole_time_threading = 0


def sum_of_all_elements(arr):
    global whole_time_threading
    arr_sum = 0
    n = len(arr)
    for i in range(n):
        arr_sum += arr[i]
    whole_time_threading = time.time() - start_time
    print("Find sum of all elements takes %s seconds" % (whole_time_threading))
    print("Sum array elements is: ", arr_sum)
    return arr_sum, whole_time_threading

def array_min(arr):
    global time_find_min
    arr_min = np.ndarray.min(arr)
    time_find_min = time.time() - start_time
    print("--- Find min element takes %s seconds ---" % (time_find_min))
    print("Min value is: ", arr_min)
    return arr_min,time_find_min 

def array_max(arr):
    global time_find_max
    arr_max = np.ndarray.max(arr)
    time_find_max = time.time() - start_time
    print("Find max element takes %s seconds" % (time_find_max))
    print("Max value is: ", arr_max)
    return  arr_max, time_find_max


mm = np.memmap(open_file, dtype = "int32", mode='r+', offset = 0)
# print(type(mm))

nparray = np.frombuffer(mm, dtype=np.dtype('uint32').newbyteorder('B'))
# print(type(nparray))
t2 = threading.Thread(target=array_min, args=(nparray,))
t3 = threading.Thread(target=array_max, args=(nparray,))
# t1 = threading.Thread(target=sum_of_all_elements, args=(nparray,))
executor = pool.ThreadPoolExecutor()
executor.submit(sum_of_all_elements, nparray)
# # t1.is_alive()
t2.start()
# # t2.is_alive()
t3.start()
# # t3.is_alive()
# t1.start()
# t1.close()
# t2.close()
# t3.close()

# p1 = multiprocessing.Process(target=sum_of_all_elements, args=(nparray,))
# p2 = multiprocessing.Process(target=array_min, args=(nparray,))
# p2 = multiprocessing.Process(target=array_max, args=(nparray,))

# p1.start()
# p1.join()

# p2.start()
# p2.join()

# p3.start()
# p3.join()


# p1.close()
# p2.close() 
# p3.close()

# first_sum = first_sum_of_elements(arr)
# second_sum = second_sum_of_elements(arr)

# total_sum = first_sum + second_sum

# print("Sum of all elements is: ", total_sum)
# open_file.close()
    

# print("--- %s seconds ---" % (time.time() - start_time))


# In[27]:


print("Performance difference is %s seconds" % (whole_time - whole_time_threading))
print("Threading shows an increase in %s percent" %((whole_time/whole_time_threading - 1) * 100))


# In[ ]:




