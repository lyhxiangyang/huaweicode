"""
处理process数据的目的：
1. 处理只包含正常数据的文件

主要行为是按照核心数进行分开
"""
import os
from typing import Tuple, Union, List

import pandas as pd

from utils.DefineData import FAULT_FLAG, CPU_FEATURE

prefixtime = "single"
faultprefix = "fault_"

# 是否剔除0数据
isexcludeNormal = True
savedatapath = "tmp/Data"
datapath = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_single\\wrfrst_e5-43_process-2.csv",
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
    此时假设所有的时间都是连续
    该函数会自动处理user和system等累计值数据
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
        SaveDataFrame(0, df)
