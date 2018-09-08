import os
import sys
import numpy as np

"""python dataset_getnerator.py [size] [type_of_dataset]

type_of_dataset:
1 - band-join with three streams
2 - equi-join with duplicate keys
"""

size = int(sys.argv[1])
scenario = int(sys.argv[2]) 
sep = ' '
range_min, range_max = 1, 10000
path = str(size) + '/'

# Make the dataset directory if it doesn't exist yet
if not os.path.exists(path):
    os.makedirs(path)

if scenario == 1:
    """For three way band-join scenario
    R [x,y] [int,float]
    S [a,b,c,d] [int,float,double,boolean]
    T [e,f] [double,boolean]

    R.x >= S.a - 10
    R.x <= S.a + 10
    R.y >= S.b - 10
    R.y <= S.b + 10
    S.c >= T.e - 10
    S.c <= T.e + 10
    S.d = T.f
    """

    # Generate uniform data in range of range_min-range_max,
    # (similar with binary join in CellJoin, HandshakeJoin, and ScaleJoin)

    # Stream R
    x = np.random.randint(range_min,range_max, size)
    y = np.random.uniform(range_min,range_max, size)

    # Stream S
    a = np.random.randint(range_min,range_max, size)
    b = np.random.uniform(range_min,range_max, size)
    c = np.random.uniform(range_min,range_max, size)
    d = np.random.choice(['true','false'], size) # Because true and false in Java is lowercase

    # Stream T
    e = np.random.uniform(range_min,range_max, size)
    f = np.random.choice(['true','false'], size)

    # Write data to [size]/ path
    R = open(path + 'R','w+')
    S = open(path + 'S','w+')
    T = open(path + 'T','w+')

    for i in range(size):
        R.write(sep.join([str(x[i]),str(y[i])]) + '\n')
        S.write(sep.join([str(a[i]),str(b[i]),str(c[i]),str(d[i])]) + '\n')
        T.write(sep.join([str(e[i]),str(f[i])]) + '\n')

elif scenario == 2:
    """For four way join with duplicates key for each stream with common key attributes
    R [x,y] [int,int]
    S [a,b] [int,int]
    T [c,d] [int,int]
    U [e,f] [int,int]
    
    Common attribute:
    R.x = S.a = T.c = U.e
    
    Distinct attribute:
    R.x = S.a AND
    S.b = T.c AND
    T.d = U.e
    """

    # Stream R
    x = np.random.randint(range_min,range_max, size)
    y = np.random.randint(range_min,range_max, size)

    # Stream S
    a = np.random.randint(range_min,range_max, size)
    b = np.random.randint(range_min,range_max, size)

    # Stream T
    c = np.random.randint(range_min,range_max, size)
    d = np.random.randint(range_min,range_max, size)

    # Stream U
    e = np.random.randint(range_min,range_max, size)
    f = np.random.randint(range_min,range_max, size)

    # Write data to [size]/ path
    R = open(path + 'R','w+')
    S = open(path + 'S','w+')
    T = open(path + 'T','w+')
    U = open(path + 'U','w+')

    for i in range(size):
        R.write(sep.join([str(x[i]),str(y[i])]) + '\n')
        S.write(sep.join([str(a[i]),str(b[i])]) + '\n')
        T.write(sep.join([str(c[i]),str(d[i])]) + '\n')
        U.write(sep.join([str(e[i]),str(f[i])]) + '\n')
