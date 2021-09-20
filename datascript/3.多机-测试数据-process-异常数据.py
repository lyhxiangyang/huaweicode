"""
处理process数据的目的：是针对测试数据中的多机数据

1. 这个脚本的目的是用来处理一个文件中有多种数据异常，能够自动找出并且分类
2. 将会抛弃标签为0的数据
3. 最终会按照 错误类型-次数-核心数的目录顺序来进行数据的分类
4. 假设连续的错误类型都是按照时间顺序的
"""
import os
from typing import Tuple, Union, List

import pandas as pd

from utils.DefineData import FAULT_FLAG, CPU_FEATURE

prefixtime = "测试数据_多机_process"
faultprefix = "fault_"

# 是否剔除0数据
isexcludeNormal = True
savedatapath = "tmp/Data"
datapath = [
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cacheGrabNum-1.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cacheGrabNum-2.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cacheGrabNum-3.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cacheGrabNum-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cacheGrabNum-10.csv",
    # cpuall
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpuall-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpuall-10.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpuall-20.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpuall-50.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpuall-100.csv",
    # cpugrab
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpugrab-1.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpugrab-2.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpugrab-3.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpugrab-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpugrab-10.csv",
    # cpusingle
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusingle-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusingle-10.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusingle-20.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusingle-50.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusingle-100.csv",
    # cpumulti
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusmulti-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusmulti-10.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusmulti-20.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusmulti-50.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_cpusmulti-100.csv",
    # mbw_process
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_mbw_process-1.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_mbw_process-2.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_mbw_process-3.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_mbw_process-5.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_mbw_process-10.csv",
    # memleak
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_memleak-30.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_memleak-60.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_memleak-120.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_memleak-600.csv",
    "D:/HuaweiMachine/测试数据/wrfrst多机e5/result/wrf-e5-43-multi/wrfrst_e5-43_process_memleak-1200.csv",

]

"""
函数功能： 得到指定路径下以prefix为前缀的下一个目录名
savepath是包含错误码的
"""


def getTimeFileName(prefix: str, savepath: str) -> Union[Tuple[None, bool], Tuple[str, bool]]:
    if not os.path.exists(savepath):
        return None, True
    listdirs = os.listdir(savepath)
    lenprefix = len([idir for idir in listdirs if idir.startswith(prefix)])
    return prefix + str(lenprefix + 1), False


# 获得与当前位置beginpos相同的内容且连续的最后一个位置的下一个位置
# 如果为返回值为-1，则代表这个初始位置不可用
def getListNextNotSame(l1: list, beginpos: int) -> int:
    if beginpos >= len(l1):
        return -1
    i = beginpos + 1
    for i in range(beginpos + 1, len(l1) + 1):
        if i == len(l1):
            return len(l1)
        if l1[i] != l1[beginpos]:
            return i


"""
df在保存前会调用这个函数，用来对数据进行处理
"""

featureSub = ["user", "system"]
def DataProcess(df: pd.DataFrame) -> pd.DataFrame:
    df, err = subtractLastLineFromDataFrame(df, featureSub)
    return df


"""
-   按照错误码进行划分, 得到一系列的list
"""


def SplitFaultFlag(df: pd.DataFrame) -> List[Tuple[int, pd.DataFrame]]:
    respdList = []
    nowpos = 0
    while True:
        nextpos = getListNextNotSame(df[FAULT_FLAG], nowpos)
        if nextpos == -1:
            break
        nowflag = df.iloc[nowpos][FAULT_FLAG]
        respdList.append((nowflag, df.iloc[nowpos:nextpos]))
        nowpos = nextpos
    return respdList


"""
-   按照核心数进行划分， 得到一系列的list
"""


def SplitCores(df: pd.DataFrame) -> List[Tuple[int, pd.DataFrame]]:
    if CPU_FEATURE not in df.columns.array:
        print("函数SplitCores错误")
        print("{} 这一列在表格中不存在".format(CPU_FEATURE))
        exit(1)
    corelist = list(set(df[CPU_FEATURE]))
    coreList = []
    for icore in corelist:
        tpd = df.loc[df[CPU_FEATURE] == icore]
        tpd.reset_index(drop=True)
        # 将CPU_FEATURE去掉
        # coreDict[icore] = tpd.drop(CPU_FEATURE, axis=1)
        tpd = DataProcess(tpd)
        coreList.append((icore, tpd))
    return coreList


"""
-   将错误码和DataFrame进行文件的保存
    此时假设所有的
"""


def SaveDataFrame(faulty: int, faultyDataFrame: pd.DataFrame):
    faultypath = os.path.join(savedatapath, faultprefix + str(faulty))
    if not os.path.exists(faultypath):
        os.makedirs(faultypath)
    timefilename, err = getTimeFileName(prefix=prefixtime, savepath=faultypath)
    corefilepath = os.path.join(faultypath, timefilename)
    if not os.path.exists(corefilepath):
        os.makedirs(corefilepath)
    if err:
        print("SaveDataFrame函数错误")
        print("{} 路径不存在".format(faultypath))
        exit(1)
    corepds = SplitCores(faultyDataFrame)
    for corenum, corepd in corepds:
        corefilename = os.path.join(corefilepath, str(corenum) + ".csv")
        corepd.to_csv(corefilename, index=False)


"""
- 只用来分割和保存一个文件中的数据
"""


def DealOneFile(df: pd.DataFrame) -> bool:
    # 保证有标签这个选项
    if FAULT_FLAG not in df:
        return False
    # 得到一个错误码 加 DataFrame的结构
    faultList = SplitFaultFlag(df)
    for ifault, faultDataFrame in faultList:
        if isexcludeNormal and ifault == 0:
            continue

        print("进行错误码{}的保存".format(ifault).center(40, "*"))
        SaveDataFrame(ifault, faultDataFrame)


"""
-   将DataFrame中的columns都减去上一行，第一行等于0
"""


def subtractLastLineFromDataFrame(df: pd.DataFrame, columns: List) -> Union[
    Tuple[None, bool], Tuple[pd.DataFrame, bool]]:
    df = df.copy()
    if len(df) <= 1:
        return None, True
    # 先将整个表格往上一隔
    dfcolumns_1 = df.loc[:, columns].shift(periods=-1, axis=0, fill_value=0)
    # 然后相减
    dfcolumns_2 = dfcolumns_1 - df.loc[:, columns]
    # 然后下一一位
    df.loc[:, columns] = dfcolumns_2.shift(periods=1, axis=0, fill_value=0)
    return df, False


if __name__ == "__main__":
    for ipath in datapath:
        df = pd.read_csv(ipath)
        # 数据预处理部分
        # 1. 将对应的每个文件的都减去第一行和前一行
        # 数据分割部分
        DealOneFile(df)
