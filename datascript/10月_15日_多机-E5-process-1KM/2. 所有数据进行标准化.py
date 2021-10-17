
# 需要标准化文件所在的路径
import os.path
from typing import List

import pandas as pd


# 需要标准化的错误码
from utils.DataFrameOperation import SortLabels, PushLabelToFirst, PushLabelToEnd
from utils.DefineData import TIME_COLUMN_NAME, FAULT_FLAG
from utils.FileSaveRead import saveFaultyDict, readFilename_Time_Core_pdDict, readFilename_Time_Core_Faulty_pdDict
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

def standardPDfromOriginal(df: pd.DataFrame, standardFeatures=None, meanValue=None) -> pd.DataFrame:
    if standardFeatures is not None:
        if FAULT_FLAG in standardFeatures:
            standardFeatures.remove(FAULT_FLAG)
        if TIME_COLUMN_NAME in standardFeatures:
            standardFeatures.remove(TIME_COLUMN_NAME)
    if standardFeatures is None:
        standardFeatures = []
    nostandardDf = df.loc[:, standardFeatures]
    nostandardDf: pd.DataFrame
    # 如果为空 代表使用自己的mean
    if meanValue is None:
        meanValue = nostandardDf.mean()
    # 进行标准化
    standardDf = (nostandardDf / meanValue * 100).astype("int64")
    if TIME_COLUMN_NAME in df.columns.array:
        standardDf[TIME_COLUMN_NAME] = df[TIME_COLUMN_NAME]
    if FAULT_FLAG in df.columns.array:
        standardDf[FAULT_FLAG] = df[FAULT_FLAG]

    standardDf = SortLabels(standardDf)
    standardDf = PushLabelToFirst(standardDf, TIME_COLUMN_NAME)
    standardDf = PushLabelToEnd(standardDf, FAULT_FLAG)
    return standardDf

def getDFmean(df: pd.DataFrame, standardFeatures: List[str]) -> pd.Series:
    if FAULT_FLAG in standardFeatures:
        standardFeatures.remove(FAULT_FLAG)
    if TIME_COLUMN_NAME in standardFeatures:
        standardFeatures.remove(TIME_COLUMN_NAME)
    return df[:, standardFeatures].mean()



if __name__ == "__main__":
    normalpath = "tmp/tData-10-18/多机-E5-process-server-1KM/1.所有process错误码信息/0.csv"
    file_time_corePath = "tmp/tData-10-18/多机-E5-process-server-1KM/2.filename-time-core"
    file_time_core_faultyPath: str = "tmp/tData-10-18/多机-E5-process-server-1KM/3.filename-time-core-faulty"

    # 获得所有正常情况下各个特征的平均值
    nomalpd = pd.read_csv(normalpath)
    normalmean = getDFmean(nomalpd, standardFeatures=allFeature)

    #读取所有的文件到字典中
    print("开始读取file_time_core信息".center(40, "*"))
    file_time_corePDDict = readFilename_Time_Core_pdDict(file_time_corePath)
    print("结束读取file_time_core信息".center(40, "*"))
    print("开始读取file_time_core_faulty信息".center(40, "*"))
    file_time_core_faultyPDDict = readFilename_Time_Core_Faulty_pdDict(file_time_core_faultyPath)
    print("结束读取file_time_core_faulty信息".center(40, "*"))



