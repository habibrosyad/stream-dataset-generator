import os
import numpy as np
from random import shuffle

"""This script was made by @dchengz for Python 2.7.
It creates four stream (R,S,T,U) datasets with distinct keys.

Note: To produce 6000000 tuples per stream might take days
"""

# Number of streams
n = 4
# Number of tuples per stream
N = 6000000
selectivity = 0.1
# Number of join results
ns = int(N*selectivity)

sel_1 = 600000 
percent = 20
sel_3 = N*percent//100 
sel_2 = int(N*(1-selectivity) - sel_1 - 3*sel_3)//3

unq_keys = int(ns+4*sel_1+6*sel_2+4*sel_3)
uniqueness = round(1.0*unq_keys/(N*n),2)

arr_r = range(ns+1,ns+sel_1+1)
arr_s = range(ns+sel_1+1,ns+2*sel_1+1)
arr_t = range(ns+2*sel_1+1,ns+3*sel_1+1)
arr_u = range(ns+3*sel_1+1,ns+4*sel_1+1)
arr_rs = range(ns+4*sel_1+1,ns+4*sel_1+sel_2+1)
arr_rt = range(ns+4*sel_1+sel_2+1,ns+4*sel_1+2*sel_2+1)
arr_ru = range(ns+4*sel_1+2*sel_2+1,ns+4*sel_1+3*sel_2+1)
arr_st = range(ns+4*sel_1+3*sel_2+1,ns+4*sel_1+4*sel_2+1)
arr_su = range(ns+4*sel_1+4*sel_2+1,ns+4*sel_1+5*sel_2+1)
arr_tu = range(ns+4*sel_1+5*sel_2+1,ns+4*sel_1+6*sel_2+1)
arr_rst = range(ns+4*sel_1+6*sel_2+1,ns+4*sel_1+6*sel_2+sel_3+1)
arr_rsu = range(ns+4*sel_1+6*sel_2+sel_3+1,ns+4*sel_1+6*sel_2+2*sel_3+1)
arr_rtu = range(ns+4*sel_1+6*sel_2+2*sel_3+1,ns+4*sel_1+6*sel_2+3*sel_3+1)
arr_stu = range(ns+4*sel_1+6*sel_2+3*sel_3+1,ns+4*sel_1+6*sel_2+4*sel_3+1)

DIR_PATH = str(N)
if not os.path.exists(DIR_PATH):
    os.makedirs(DIR_PATH)

######################### CREATE DATA FOR STREAMS WITHOUT JOIN RESULTS & RANDOMISE
arr_r.extend(arr_rs); arr_r.extend(arr_rt); arr_r.extend(arr_ru);
arr_r.extend(arr_rst); arr_r.extend(arr_rsu); arr_r.extend(arr_rtu);
shuffle(arr_r)

arr_s.extend(arr_rs); arr_s.extend(arr_st); arr_s.extend(arr_su);
arr_s.extend(arr_rst); arr_s.extend(arr_rsu); arr_s.extend(arr_stu);
shuffle(arr_s)

arr_t.extend(arr_rt); arr_t.extend(arr_st); arr_t.extend(arr_tu);
arr_t.extend(arr_rst); arr_t.extend(arr_rtu); arr_t.extend(arr_stu);
shuffle(arr_t)

arr_u.extend(arr_ru); arr_u.extend(arr_su); arr_u.extend(arr_tu);
arr_u.extend(arr_rsu); arr_u.extend(arr_rtu); arr_u.extend(arr_stu);
shuffle(arr_u)

### CREATE R STREAM
arr_R = []
arr_rstu = range(1,ns+1)
for n in range(1,N+1):
    if n % (N/ns) == 0:
        num_res = arr_rstu.pop(0)
        arr_R.append(str(num_res)+' '+str(n))
    else:
        num_nonres = arr_r.pop(0)
        arr_R.append(str(num_nonres)+' '+str(n))
namefile = 'R'
file = open(DIR_PATH+'/'+namefile, 'w')
file.write("\n".join(arr_R))
file.close()

### CREATE S STREAM
arr_S = []
arr_rstu = range(1,ns+1)
for n in range(1,N+1):
    if n % (N/ns) == 0:
        num_res = arr_rstu.pop(0)
        arr_S.append(str(num_res)+' '+str(n))
    else:
        num_nonres = arr_s.pop(0)
        arr_S.append(str(num_nonres)+' '+str(n))
namefile = 'S'
file = open(DIR_PATH+'/'+namefile, 'w')
file.write("\n".join(arr_S))
file.close()

### CREATE T STREAM
arr_T = []
arr_rstu = range(1,ns+1)
for n in range(1,N+1):
    if n % (N/ns) == 0:
        num_res = arr_rstu.pop(0)
        arr_T.append(str(num_res)+' '+str(n))
    else:
        num_nonres = arr_t.pop(0)
        arr_T.append(str(num_nonres)+' '+str(n))
namefile = 'T'
file = open(DIR_PATH+'/'+namefile, 'w')
file.write("\n".join(arr_T))
file.close()

### CREATE U STREAM
arr_U = []
arr_rstu = range(1,ns+1)
for n in range(1,N+1):
    if n % (N/ns) == 0:
        num_res = arr_rstu.pop(0)
        arr_U.append(str(num_res)+' '+str(n))
    else:
        num_nonres = arr_u.pop(0)
        arr_U.append(str(num_nonres)+' '+str(n))
namefile = 'U'
file = open(DIR_PATH+'/'+namefile, 'w')
file.write("\n".join(arr_U))
file.close()