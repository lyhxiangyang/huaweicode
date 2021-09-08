"""
此文件的作用是将server数据进行合并
"""
import os

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame, subtractFirstLineFromDataFrame, mergeDataFrames
from utils.DefineData import FAULT_FLAG, TIME_COLUMN_NAME

AllCSVFiles = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_single\\wrfrst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_server.csv",
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_server.csv"
]
savepath = "tmp\\wrf_single_process\\1.2\\"

if __name__ == "__main__":
    # 先将路径自动生成
    if not os.path.exists(savepath):
        os.makedirs(savepath)

    ####################################################################################################################
    print("server数据合并中".center(40, "*"))
    tallPds = [pd.read_csv(ipath) for ipath in AllCSVFiles]
    # ==判断是否存在空
    for ipds in tallPds:
        print("shape: {}".format(ipds.shape))
        if isEmptyInDataFrame(ipds):
            print("打开的csv文件中， 有空值")
            exit(1)

    # == 将每一个Pd中的数据都减去第一行
    allcolumns = list(tallPds[0].columns)
    allcolumns.remove(FAULT_FLAG)
    allcolumns.remove(TIME_COLUMN_NAME)
    allPds = []
    for ipd in allPds:
        tpd, err = subtractFirstLineFromDataFrame(df=ipd, columns=allcolumns)
        if err:
            print("在处理server数据中，减去第一行出现问题")
            exit(1)
        allPds.append(tpd)

    # == 将多个文件进行合并
    meregdPd, err = mergeDataFrames(allPds)
    if err:
        print("数据合并失败")
        exit(1)
    print("合并之后文件大小：", meregdPd.shape)
    if isEmptyInDataFrame(meregdPd):
        print("合并之后的文件存在空值")
        exit(1)
    print("文件中没有空值")

    ####################################################################################################################
    # 将文件进行保存
    tpath = os.path.join(savepath, "1.2_serverData_mergePd.csv")
    meregdPd.to_csv(tpath, index=False)
