
import time




"""
将时间格式转化为int
"2021/8/29 0:54:08"
"""
def TranslateTimeToInt( stime: str) -> int:
    itime = time.mktime(time.strptime(stime, '%Y/%m/%d %H:%M:%S'))
    return int(itime)
