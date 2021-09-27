import time
from typing import List, Union

"""
将时间格式转化为int
"2021/8/29 0:54:08"
"""


def TranslateTimeToInt(stime: str) -> int:
    itime = time.mktime(time.strptime(stime, '%Y-%m-%d %H:%M:%S'))
    return int(itime)


"""
将一个时间的中的秒字段都变为0
"""


def TranslateTimeStrToStr(stime: str) -> str:
    stime = stime[0]
    ttime = time.strptime(stime, '%Y-%m-%d %H:%M:%S')
    strtime = time.strftime('%Y-%m-%d %H:%M:00', ttime)
    return strtime


# 转变一个列表的字符串
def TranslateTimeListStrToStr(stime: List[str]) -> Union[str, list[str]]:
    reslist = []
    for itime in stime:
        ttime = time.strptime(itime, '%Y-%m-%d %H:%M:%S')
        strtime = time.strftime('%Y-%m-%d %H:%M:00', ttime)
        reslist.append(strtime)
    if len(reslist) == 1:
        return reslist[0]
    return reslist
