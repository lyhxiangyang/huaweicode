"""
这个文件的本意是包含对DataFrame的各种操作
如：
1. 将多个相同类型的DataFrame合并成一个
2. 将一个大的DataFrame根据某个特征值分割成多个

"""

from typing import List, Dict
from utils.DefineData import *
import pandas as pd

"""
函数功能：合并多个DataFrame 返回值是一个合并之后的DataFrame
函数参数：预期是一个含有DataFrame的列表 
返回值： 包含各个DataFrame的一个大的DataFrame , 第二个参数表示是否运行出错 True 代表出错  False代表没错
"""


def mergeDataFrames(lpds: List[pd.DataFrame]) -> (pd.DataFrame, bool):
    # 如果说传进来的DataFrame头部不一样 就报错
    if not judgeSameFrames(lpds):
        return None, False
    # 将列表中的数据全都合并
    dfres = pd.concat(lpds, ignore_index=True)
    return dfres, False


"""
函数功能： 判断多个DataFrame是否含有
函数参数：预期是一个含有DataFrame的列表 
"""


def judgeSameFrames(lpds: List[pd.DataFrame]) -> bool:
    lcolumns = [list(i.columns.array) for i in lpds]

    # 长度为0的时候可以返回True
    if len(lpds) == 0 or len(lpds) == 1:
        return True
    for i in range(1, len(lcolumns)):
        if lcolumns[1] != lcolumns[0]:
            return False
    return True


"""
函数功能： 将DataFrame中按照FAULT_FLAG进行分类生成
函数参数： 传入一个Server的信息 包含Fault_Flag参数
函数返回值： Dict 第一个是错误码 对应是其DataFrame结构
"""


def divedeDataFrameByFaultFlag(df: pd.DataFrame) -> (Dict[int, pd.DataFrame], bool):
    # 先判断是否存在Fault_FLag参数，不存在就报错
    if FAULT_FLAG not in list(df.columns.array):
        return None, True

    # 对Fault_Flag这一行进行去重, 并将错误
    sFault_Flag_Colums = sorted(list(set(df[FAULT_FLAG])))

    # 重复n个空的DataFrame， 方便使用zip直接生成一个Dict结构的数据结构
    repeatEmptyDataFrames = [pd.DataFrame(columns=df.columns.array) for i in range(0, len(sFault_Flag_Colums))]
    resDict = dict(zip(sFault_Flag_Colums, repeatEmptyDataFrames))

    # 遍历DataFrame根据 Fault_Flag这一行来分开
    for i in range(0, len(df)):
        # 第i行的获取
        df_iline = df.iloc[i]
        # 错误码
        iFault_Flag_Number = df_iline[FAULT_FLAG]
        # 在对应字典中添加一行, 忽略index 逐步增加
        resDict[iFault_Flag_Number] = resDict[iFault_Flag_Number].append(df_iline, ignore_index=True)

    # =================================================
    # 显示DEBUG的信息
    if DEBUG:
        print("divedeDataFrameByFaultFlag".center(40, "*"))
        for k, kpd in resDict.items():
            print(str(k).center(10, "="))
            print(kpd)
        print("end".center(40, "*"))
    # =================================================

    # 返回
    return resDict, False
