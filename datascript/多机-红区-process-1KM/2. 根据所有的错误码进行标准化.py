


# 需要标准化文件所在的路径
import os.path
from typing import List

import pandas as pd


# 需要标准化的错误码
from utils.FileSaveRead import saveFaultyDict

standardized_flag = [0, 15]
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
只返回标准化之后的数据特征， 没有标准化的不返回
"""
def standardPDfromOriginal(df : pd.DataFrame, standardFeatures: List[str]) -> pd.DataFrame:
    nostandardDf = df.loc[:, standardFeatures]
    nostandardDf : pd.DataFrame
    meanValue = nostandardDf.mean()
    # 进行标准化
    standardDf = (nostandardDf / meanValue * 100).astype("int64")
    return standardDf

"""
目前对标准化
"""
if __name__ == "__main__":
    # readpath
    standardized_path = "tmp/tData_10_9/多机-红区-process-server-1KM-win3_step1/所有process错误码信息-原始数据"
    spath = "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据"
    # 需要标准化的特征
    standardFeature = ["load1"]

    fault_pd_Dict = {}
    for ifault in standardized_flag:
        filename = str(ifault) + ".csv"
        filepath = os.path.join(standardized_path, filename)
        if not os.path.exists(filepath):
            print("{}文件不存在".format(filename))
            continue
        filepd = pd.read_csv(filepath)
        standardPD = standardPDfromOriginal(filepd, standardFeatures=standardFeature)
        fault_pd_Dict[ifault] = standardPD
    saveFaultyDict(spath, fault_pd_Dict)








































































































