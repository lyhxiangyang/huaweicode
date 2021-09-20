"""
1. 能够进行特征的选择
"""

import os
from collections import defaultdict
from typing import Dict, Tuple, Union, Any, List

import pandas as pd

from utils.FileSaveRead import readFaultyPD, saveFaultyCoreDict, readCoresPD, saveFaultyDict

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
    # "children_user",
    # "children_system",
    "iowait",
    # "cpu_affinity",  # 依照这个来为数据进行分类
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




if __name__ == "__main__":
    datasavepath = "tmp/Data"
    tmpsavepath = "tmp/11.滑动窗口2多机模型预测"
    tmpsavepath1 = "tmp/11.滑动窗口2多机模型预测/1.训练数据处理部分"
    if not os.path.exists(tmpsavepath1):
        os.makedirs(tmpsavepath1)

    excludeFaulty = [81, 82, 83, 84, 85]
    # 读取所有的数据
    singleexcludeFaulty = [41, 42, 43, 44, 45, 71, 72, 73, 74, 75]
    faulty_core_pd_dict, err = readFaultyPD(datasavepath, readDir="测试数据_多机_process1", excludeFaulty=singleexcludeFaulty)

    # 测试数据
    saveFaultyCoreDict(os.path.join(tmpsavepath1, "测试数据_正常_异常"), faulty_core_pd_dict)

    # 数据修订版1KM正常
    datadir = "数据修订版_多机_1KM_process"

    normal_1km, err = readCoresPD(readpath=os.path.join(datasavepath, "fault_0", datadir), select_feature=process_features)
    if err:
        print("数据修订版 1KM 读取错误")
        exit(1)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常1km"), normal_1km)

    # 数据修订版3KM正常
    datadir = "数据修订版_多机_3KM_process"
    normal_3km, err = readCoresPD(readpath=os.path.join(datasavepath, "fault_0", datadir), select_feature=process_features)
    if err:
        print("数据修订版 3KM 读取错误")
        exit(1)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常3km"), normal_3km)

    # 数据修订版9KM正常
    datadir = "数据修订版_多机_9KM_process"
    normal_9km, err = readCoresPD(readpath=os.path.join(datasavepath, "fault_0", datadir), select_feature=process_features)
    if err:
        print("数据修订版 9KM 读取错误")
        exit(1)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常9km"), normal_9km)



# 手动删除或者脚本删除随机抢占和的核心数




