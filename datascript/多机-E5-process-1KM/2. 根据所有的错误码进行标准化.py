


# 需要标准化文件所在的路径
import os.path
from typing import List

import pandas as pd


# 需要标准化的错误码
from utils.DefineData import TIME_COLUMN_NAME, FAULT_FLAG
from utils.FileSaveRead import saveFaultyDict
from utils.ProcessData import standardPDfromOriginal

standardized_normalflag = 0
standardized_abnormalflag = [15]
# 需要标准化的特征
allFeature = ["time",
               "user_server",
               "nice",
               "system_server",
               "idle",
               "iowait_server",
               "irq",
               "softirq",
               "steal",
               "guest",
               "guest_nice",
               "ctx_switches",
               "interrupts",
               "soft_interrupts",
               "syscalls",
               "freq",
               "load1",
               "load5",
               "load15",
               "total",
               "available",
               # "percent",
               "used",
               "free",
               "active",
               "inactive",
               "buffers",
               "cached",
               "handlesNum",
               "pgpgin",
               "pgpgout",
               "fault",
               "majflt",
               "pgscank",
               "pgsteal",
               "pgfree",
               # "faultFlag_server",
               # "pid",
               # "status",
               # "create_time",
               # "puids_real",
               # "puids_effective",
               # "puids_saved",
               # "pgids_real",
               # "pgids_effective",
               # "pgids_saved",
               "user_process",
               "system_process",
               # "children_user",
               # "children_system",
               "iowait_process",
               # "cpu_affinity",
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
               # "num_threads",
               "voluntary",
               "involuntary",
               "faultFlag"
]






"""
目前对标准化
"""
if __name__ == "__main__":
    # readpath
    standardized_path = "tmp/tData_10_9/多机-E5-process-server-1KM-win3_step1/所有process错误码信息-原始数据"
    spath = "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据"
    # 需要标准化的特征
    standardFeature = ["load1"]

    fault_pd_Dict = {}

    # 将normal的加入进来，并且计算平均值
    filepath = os.path.join(standardized_path, str(standardized_normalflag) + ".csv")
    if os.path.exists(filepath):
        print("正常文件不存在")
        exit(1)
    normalpd = pd.read_csv(filepath)
    standardNormalPd = standardPDfromOriginal(normalpd, standardFeatures=standardFeature)
    fault_pd_Dict[standardized_normalflag] = standardNormalPd

    # 计算平均值
    meanValue = standardNormalPd.loc[:, standardFeature].mean()

    for ifault in standardized_abnormalflag:
        filename = str(ifault) + ".csv"
        filepath = os.path.join(standardized_path, filename)
        if not os.path.exists(filepath):
            print("{}文件不存在".format(filename))
            continue
        filepd = pd.read_csv(filepath)
        standardPD = standardPDfromOriginal(filepd, standardFeatures=standardFeature, meanValue=meanValue)
        fault_pd_Dict[ifault] = standardPD
    saveFaultyDict(spath, fault_pd_Dict)








































































































