import sys
from itertools import imap
import math

count=[]
temp=[]
with open("hypo1.csv") as f:
        for line in f:
                line= line.strip()
                line= line.split(",")

                count.append(int(line[1]))
                temp.append(int(line[2]))

def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)

def pearson(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff

    return diffprod / math.sqrt(xdiff2 * ydiff2)
    
print pearson(count,temp)
