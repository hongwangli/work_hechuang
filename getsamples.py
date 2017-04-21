#! /usr/bin/env python
# -*- cocoding: utf-8 -*-

import sys
import os
import time
import datetime
import numpy as np
import math
import json
from scipy import stats
from sklearn.utils.multiclass import type_of_target
from sklearn.decomposition import PCA
def pro_fea_split(k):
    """
    蓄水池算法, k为百分比
    """
    # if k > 100: exit(0)
    pro_file = '/home/xuyonglong/feature/working/experiment/20170320/sample/s_30'
    output_dir = '/home/xuyonglong/feature/working/experiment/20170320/sample/s_30_new'
    # s_1 = output_dir + '/test'
    # s_2 = output_dir + '/train'
    # s = []
    # re = []
    zeros=[]
    ones=[]
    with open(pro_file, 'r') as rfp:
        for line in rfp:
            linelist = line.strip().split('\t')
            # print linelist
            if linelist[1]=='0':
                zeros.append(linelist[0])
            else:ones.append(linelist[0])
    print len(zeros)
    re = zeros[:k]
    for i, e in enumerate(zeros[k:]):
        r = np.random.randint(0, k + i + 1)
        if r <= k - 1:
            re[r] = e
    with open(output_dir, 'w') as wfp1:
        for l in re:
            wfp1.write(l+"\t"+"0")
            wfp1.write("\n")
        for o in ones:
            wfp1.write(o+"\t"+"1")
            wfp1.write("\n")
if __name__ == '__main__':
    pro_fea_split(10000)