"""
这个文件的作用是将一个每个文件的一个特征值扩展成为多个特征值
"""

import os
from typing import Dict

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction

savepath2 = "tmp\\single\\2\\"
savepath3= "tmp\\single\\3\\"

if __name__ == "__main__":
    print("数据提取".center(40, "*"))

    if not os.path.exists(savepath3):
        os.makedirs(savepath3)

    # == 读取步骤2中产生的所有列表 产生一个dictPds
    lfiles = os.listdir(savepath2)
    dictPds : Dict = {}
    for i in lfiles:
        readfile = os.path.join(savepath2, i)
        tPd = pd.read_csv(readfile)
        if isEmptyInDataFrame(tPd):
            print("{} have NAN".format(readfile))
            exit(1)
        # 获得文件名字对应的数字
        i = int(i.split(sep=".")[0])
        dictPds[i] = tPd

    for i in dictPds.keys():
        print("错误码{} 提取中".format(i))
        tpath = os.path.join(savepath3, "{}.csv".format(i))
        tpd, err = featureExtraction(dictPds[i], windowSize=WINDOWS_SIZE)
        if err:
            print("{}_more.csv 特征提取失败".format(i))
        tpd.to_csv(tpath, index=False)

    print("数据提取结束".center(40, "*"))
