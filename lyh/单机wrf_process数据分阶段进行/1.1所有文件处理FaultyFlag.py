"""
1. 这个文件的作用是将所有的process数据进行读取，然后修改为对应的FlagFaulty
"""
import os
from typing import Tuple

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame, judgeSameFrames, mergeDataFrames
from utils.DefineData import FAULT_FLAG

savepath = "tmp\\wrf_single_process\\1\\"

abnormalPathes = {
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-1.csv": 91,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-2.csv": 92,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-3.csv": 93,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-5.csv": 94,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-10.csv": 95,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-10.csv": 12,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-20.csv": 13,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-50.csv": 14,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-100.csv": 15,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-1.csv": 81,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-2.csv": 82,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-3.csv": 83,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-5.csv": 84,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-10.csv": 85,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpusingle-5.csv": 21,
    #####
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpuall-5.csv": 11,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusingle-10.csv": 22,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusingle-20.csv": 23,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusingle-50.csv": 24,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusingle-100.csv": 25,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusmulti-5.csv": 31,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusmulti-10.csv": 32,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusmulti-20.csv": 33,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusmulti-50.csv": 34,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_cpusmulti-100.csv": 35,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_mbw_process-1.csv": 51,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_mbw_process-2.csv": 52,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_mbw_process-3.csv": 53,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_mbw_process-5.csv": 54,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_mbw_process-10.csv": 55,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-30.csv": 61,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-60.csv": 62,
    # "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-90.csv": 63,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-120.csv": 64,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-600.csv": 65,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.17-2\\wrf_rst_e5-43_process_memleak-1200.csv": 65,
}
process_features = [
    "time",
    # "pid",
    # "status",
    # "create_time",
    # "puids_real",
    # "puids_effective",
    # "puids_saved",
    # "pgids_real",
    # "pgids_effective",
    # "pgids_saved",
    "user",
    "system",
    "children_user",
    "children_system",
    "iowait",
    # "cpu_affinity",
    "memory_percent",
    "rss",
    "vms",
    "shared",
    "text",
    "lib",
    "data",
    "dirty",
    "read_count",
    "write_count",
    "read_bytes",
    "write_bytes",
    "read_chars",
    "write_chars",
    "num_threads",
    "voluntary",
    "involuntary",
    "faultFlag",
]

# 将一个DataFrame的FAULT_FLAG重值为ff
def setPDfaultFlag(df: pd.DataFrame, ff: int) -> pd.DataFrame:
    if FAULT_FLAG in df.columns.array:
        df = df.drop(FAULT_FLAG, axis=1)
    lengthpd = len(df)
    ffdict = {FAULT_FLAG: [ff] * lengthpd}
    tpd = pd.DataFrame(data=ffdict)
    tpd = pd.concat([df, tpd], axis=1)
    return tpd


if __name__ == "__main__":
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    ####################################################################################################################
    print("1. 将所有文件的处理成对应faultFlag".center(40, "*"))
    allpds = []
    for ipath, iflaut in abnormalPathes.items():
        tpd = pd.read_csv(ipath)
        # 判断这个tpd是否满足不存在空值的条件
        if isEmptyInDataFrame(tpd):
            print("path: {} 存在空的情形".format(ipath))
            exit(1)
        # 将tpd中的flag设置为对应的flag
        tpd = setPDfaultFlag(tpd, iflaut)
        allpds.append(tpd)
        print("{}: {}".format(os.path.basename(ipath), tpd.shape))

    if not judgeSameFrames(allpds):
        print("不是所有的特征都是相同的")
        exit(1)
    ####################################################################################################################
    print("2. 将所有文件合并成一个文件".center(40, "*"))
    mergePD, flag = mergeDataFrames(allpds)
    if flag:
        print("合并文件失败")
        exit(1)
    # 将文件进行保存
    mergePD: pd.DataFrame
    tpath = os.path.join(savepath, "mergePD_Before.csv")
    mergePD.to_csv(tpath, index=False)

    # 判断我选择特征是否是mergePD的真子集
    allcolumns = set(mergePD.columns.array)
    if set(process_features) <= allcolumns:
        print("是一个子集")
    else:
        print("不是一个子集")
        print(allcolumns ^ set(process_features))

    ####################################################################################################################
    print("3. 去掉不必要的特征".center(40, "*"))
    print("去掉之前大小：{}".format(mergePD.shape))
    mergePD = mergePD[process_features]
    print("去掉之后大小：{}".format(mergePD.shape))
    tpath = os.path.join(savepath, "mergePD_After.csv")
    mergePD.to_csv(tpath, index=False)







