"""
-   特征合并
"""

import pandas as pd
import numpy as np
import os

from utils.DataFrameOperation import mergeDataFrames, isEmptyInDataFrame

AllCSVFiles = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_multi_e5_43\\wrfrst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_multi_e5_104\\wrfrst_e5-104_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst多机e5\\result\\wrf-e5-43-multi\\wrfrst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst多机e5\\result\\wrf-e5-104-multi\\wrfrst_e5-104_server.csv"
]
savepath = "tmp\\multi\\1\\"
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
        os.makedirs(savepath)

    # == 将文件进行保存到文件下
    savefile = os.path.join(savepath, "1.mergedpd.csv")
    mergedPd.to_csv(savefile, index=False)




