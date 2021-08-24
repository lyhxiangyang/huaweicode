"""
-   特征合并
"""

import pandas as pd
import numpy as np
import os

from utils.DataFrameOperation import mergeDataFrames, isEmptyInDataFrame

AllCSVFiles = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_single\\wrfrst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_server.csv"
]

savepath = "tmp\\1\\"

if __name__  == "__main__":
    print("数据合并中".center(40, "*"))
    allPds = [pd.read_csv(ipath) for ipath in AllCSVFiles]

    # == 判断是否存在空
    for ipds in allPds:
        if isEmptyInDataFrame(ipds):
            print("打开csv文件中 有空值")
            exit(1)
    # == 将多个文件进行合并
    mergedPd, err = mergeDataFrames(allPds)
    print("合并之后文件大小：", mergedPd.shape)
    print("文件中没有空值")

    # == 将文件进行保存
    if not os.path.exists(savepath):
        os.mkdir(savepath)

    # == 将文件进行保存到文件下
    savefile = os.path.join(savepath, "1.mergedpd.csv")
    pd.DataFrame(mergedPd).to_csv(savefile)




