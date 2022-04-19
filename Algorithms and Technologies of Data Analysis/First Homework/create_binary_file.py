#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#this task allows to generate binary file min 3 GB consisting of random big endian numbers 


# In[4]:


import random
import tqdm
import os

filename = "test_huge_file"
gb_factor = 3 #This variable shows how many GigaBytes will be in the file

bit_size = 4 #This variable allows to specify whether we use int32 or int64
cycles_num = int(gb_factor*(1000**3)/bit_size) #How many cycles will be made
max_number = 2**(bit_size*8) #The highest number in the row

    
with open(filename, 'wb') as f:
    for i in tqdm.tqdm(range(cycles_num)):
        f.write(random.randint(0, max_number).to_bytes(bit_size, byteorder='big', signed=False))


# In[ ]:




