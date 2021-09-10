"""
1. 将错误码合并并替换
"""

"""
1. 这个文件的作用是将所有的process数据进行读取，然后修改为对应的FlagFaulty
2. 将各个提取之后的错误码进行保存
"""
import os
from typing import Tuple, List

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame, judgeSameFrames, mergeDataFrames
from utils.DefineData import FAULT_FLAG

savepath = "tmp\\wrf_single_process_1\\1.1\\"

abnormalPathes = {
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_single\\wrfrst_e5-43_process-2.csv": 0,
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
    # 下面这三个文件需要先运行步骤5中的第一步
    # "tmp\\wrf_process_otherplatform\\1km\\1.错误码分割\\0.csv": 0,
    # "tmp\\wrf_process_otherplatform\\3km\\1.错误码分割\\0.csv": 0,
    # "tmp\\wrf_process_otherplatform\\9km\\1.错误码分割\\0.csv": 0,
}

dataPathes = [
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-1km-单机数据整合版\\wrf_1km_160_process.csv",
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-3km-单机数据整合版\\wrf_3km_160_process.csv",
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-9km-单机数据整合版\\wrf_9km_160_process.csv",
]


def getDataByFaultFlag(FaultFlag: int, pathes: List[str]) -> pd.DataFrame:
    dfpds = [pd.read_csv(i) for i in pathes]
    mergepd, err = mergeDataFrames(dfpds)
    if err:
        print("在函数 getDataByFaultFlag中 读取失败")
        exit(1)
    mergepd: pd.DataFrame
    respd = mergepd.loc[mergepd[FAULT_FLAG] == FaultFlag].reset_index(drop=True)
    return respd


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
    "cpu_affinity",  # 依照这个来为数据进行分类
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
# 如果只是用某个错误里面的若干核心，就修改下面的includecores变量，比如下面错误2就只是用核心1中的数据
includecores = {
    2: [1],
    3: [0, 1, 2, 3, 4, 5, 6]
}
# 排除一些错误码的使用，也可以将abnormalPathes中的数据进行注释到达同样的效果
excludefaulty = [81, 82, 83, 84, 85]

CPU_FEATURE = "cpu_affinity"


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
    allpdlists = {}
    allpdlists[0] = [getDataByFaultFlag(0, dataPathes)]
    for ipath, iflaut in abnormalPathes.items():
        if iflaut in excludefaulty:
            continue
        iflaut = iflaut // 10
        if iflaut not in allpdlists:
            allpdlists[iflaut] = []
        # 将这个文件读取出来，那么接下来我要判断去掉不必要的核心数
        tpd = pd.read_csv(ipath)
        # 如果在这个字典里面, 按照核心数提取数据
        if iflaut in includecores.keys():
            tpd = tpd[[True if i in includecores[iflaut] else False for i in tpd[CPU_FEATURE]]]
            tpd.reset_index(drop=True, inplace=True)
        # 判断这个tpd是否满足不存在空值的条件
        if isEmptyInDataFrame(tpd):
            print("path: {} 存在空的情形".format(ipath))
            exit(1)
        # 将tpd中的flag设置为对应的flag
        tpd = setPDfaultFlag(tpd, iflaut)
        allpdlists[iflaut].append(tpd)
        print("{}: {}".format(os.path.basename(ipath), tpd.shape))

    ####################################################################################################################
    print("2. 将allpdlists 每个列表进行合并")
    allpds = {}
    for iflauty, ipdlist in allpdlists.items():
        tpd, err = mergeDataFrames(ipdlist)
        if err:
            print("步骤2中合并列表失败")
            exit(1)
        allpds[iflauty] = tpd
        print("{}: {}".format(iflauty, tpd.shape))

    ####################################################################################################################

    if not judgeSameFrames(list(allpds.values())):
        print("不是所有的特征都是相同的")
        exit(1)
    ####################################################################################################################
    # 将错误码进行保存
    for iflaut, ipd in allpds.items():
        tpd = ipd[process_features]
        tpath = os.path.join(savepath, str(iflaut) + ".csv")
        tpd.to_csv(tpath, index=False)
