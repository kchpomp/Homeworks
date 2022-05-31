from random import randint
import pandas as pd

filename = "line_numbers"
filename1 = "line_numbers_golang"
f = open(filename, 'a')
f1 = open(filename, 'a')
bit_size = 4
max_number = 2 ** (bit_size * 8)  # The highest number

res1 = randint(0, max_number)
df1 = pd.DataFrame({"Number": [res1]})

lines = 1
while lines < 2000:
    res2 = randint(0, max_number)

    df2 = pd.DataFrame({"Number": [res2]})
    df1 = df1.append(df2, ignore_index=True)

    lines += 1

df_csv = df1.to_csv(f)
df_csv1 = df1.to_csv(f1, header=False, index=False)
f.close()
f1.close()