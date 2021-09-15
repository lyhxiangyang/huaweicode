from typing import List, Tuple

import pandas as pd
from utils.DataFrameOperation import subtractLastLineFromDataFrame
from utils.DefineData import FAULT_FLAG


tDict1 = {
    "user": [1,2,3,4, 5, 5],
    "user1": [1,2,3,4, 5, 5],
    "system": [22, 22, 33, 33, 33, 44],
    "system1": [22, 22, 33, 33, 33, 44],
    FAULT_FLAG: [1,1,1,2,2,3]
}
tdf1 = pd.DataFrame(data=tDict1)
tdf1

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
-   按照错误码进行划分, 得到一系列的list
"""

def SplitFaultFlag(df :pd.DataFrame) -> List[Tuple[int, pd.DataFrame]]:
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

SplitFaultFlag(tdf1)
