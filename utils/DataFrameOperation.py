"""
这个文件的本意是包含对DataFrame的各种操作
"""
from typing import List

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
