"""
这一步的作用就是将process的数据和server的数据结合起来
"""
import pandas as pd

from utils.DefineData import TIME_COLUMN_NAME

savepathfile1_1 = "tmp\\tmp\\wrf_single_process\\1.1\\mergePD_After.csv"
savepathfile1_2 = "tmp\\wrf_single_process\\1.2\\1.2_serverData_mergePd.csv"

if __name__ == "__main__":
    # 验证一波两者时间一列的关系
    time1Pd = pd.read_csv(savepathfile1_1)
    time2Pd = pd.read_csv(savepathfile1_2)
    c1 = set(time1Pd[TIME_COLUMN_NAME])
    c2 = set(time2Pd[TIME_COLUMN_NAME])

    if c1 <= c2:
        print("c1 <= c2")
    if c1 >= c2:
        print("c1 >= c2")


