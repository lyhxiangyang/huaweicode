import errno
import os
from collections import defaultdict
from re import T
from typing import List, Tuple, Union, Dict, Any

import pandas as pd

from utils.DataFrameOperation import SortLabels, PushLabelToFirst, PushLabelToEnd
from utils.DefineData import TIME_COLUMN_NAME, TIME_INTERVAL, CPU_FEATURE, FAULT_FLAG, WINDOWS_SIZE
from utils.FileSaveRead import saveFaultyDict, saveFilename_Time_Core_pdDict, saveFilename_Time_Core_Faulty_pdDict
from utils.ProcessData import TranslateTimeToInt, TranslateTimeListStrToStr

allfeature = ["time", "user_sever", "nice", "system_sever", "idle", "iowait_sever", "irq", "softirq", "steal", "guest",
              "guest_nice", "ctx_switches", "interrupts", "soft_interrupts", "syscalls", "freq", "load1", "load5",
              "load15", "total", "available", "percent", "used", "free", "active", "inactive", "buffers", "cached",
              "handlesNum", "pgpgin", "pgpgout", "fault", "majflt", "pgscank", "pgsteal", "pgfree", "faultFlag_sever",
              "pid", "status", "create_time", "puids_real", "puids_effective", "puids_saved", "pgids_real",
              "pgids_effective", "pgids_saved", "user_process", "system_process", "children_user", "children_system",
              "iowait_process", "cpu_affinity", "memory_percent", "rss", "vms", "shared", "text", "lib", "data",
              "dirty", "read_count", "write_count", "read_bytes", "write_bytes", "read_chars", "write_chars",
              "num_threads", "voluntary", "involuntary", "faultFlag"]

usedFeature = ["time",
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
               "cpu_affinity",
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

accumulationFeatures = ["user_server", "nice", "system_server", "idle", "iowait_server", "irq", "softirq", "steal",
                        "guest", "guest_nice", "ctx_switches", "interrupts", "soft_interrupts", "syscalls",
                        "user_process", "system_process"]

process_features = [
    # "time",
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
datapathsever = "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_server.csv"
datapath = [
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-61.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-62.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-63.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-64.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-65.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-66.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-67.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-68.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-69.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-70.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-71.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-72.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-73.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-74.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-75.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-76.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-77.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-78.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-79.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-80.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E6/1KM/异常数据/wrf_1km_multi_43/wrf_3km_e5-43_process-81.csv",
]

"""
将一个文件的所有时间进行连续时间段放入划分
index必须是0开头的
"""


def splitDataFrameByTime(df: pd.DataFrame) -> List[pd.DataFrame]:
    respd = []
    beginLine = 0
    sbeginLineTime = df.loc[beginLine, TIME_COLUMN_NAME]
    ibeginTime = TranslateTimeToInt(sbeginLineTime)
    iLastLineTime = ibeginTime
    for nowline in range(1, len(df)):
        snowLineTime = df.loc[nowline, TIME_COLUMN_NAME]
        inowLineTime = TranslateTimeToInt(snowLineTime)
        if inowLineTime - iLastLineTime == 0:
            continue
        # 误差在59 - 61s之间 或者等于0
        if not (TIME_INTERVAL - 1 <= inowLineTime - iLastLineTime <= TIME_INTERVAL + 1):
            tpd = df.loc[beginLine: nowline, :].reset_index(drop=True)
            beginLine = nowline
            respd.append(tpd)

        iLastLineTime = inowLineTime
    tpd = df.loc[beginLine: len(df), :].reset_index(drop=True)
    respd.append(tpd)
    return respd


"""
将列表中的df保存为0.csv
"""


def saveDFListToFiles(spath: str, pds: List[pd.DataFrame]):
    if not os.path.exists(spath):
        os.makedirs(spath)
    for i in range(0, len(pds)):
        savefilepath = os.path.join(spath, str(i) + ".csv")
        pds[i].to_csv(savefilepath, index=False)


def readDFListFromFiles(spath: str) -> List[pd.DataFrame]:
    reslist = []
    if not os.path.exists(spath):
        return reslist
    files = os.listdir(spath)
    for i in range(0, len(files)):
        readfilenames = str(i) + ".csv"
        readfilepath = os.path.join(spath, readfilenames)
        tpd = pd.read_csv(readfilepath)
        reslist.append(tpd)
    return reslist


"""
按照核心数进行划分
"""


def SplitDFByCores(df: pd.DataFrame) -> List[Tuple[int, pd.DataFrame]]:
    if CPU_FEATURE not in df.columns.array:
        print("函数SplitCores错误")
        print("{} 这一列在表格中不存在".format(CPU_FEATURE))
        exit(1)
    corelist = list(set(df[CPU_FEATURE]))
    coreList = []
    for icore in corelist:
        tpd = df.loc[df[CPU_FEATURE] == icore]
        tpd.reset_index(drop=True, inplace=True)
        # 将CPU_FEATURE去掉
        # coreDict[icore] = tpd.drop(CPU_FEATURE, axis=1)
        coreList.append((icore, tpd))
    return coreList


# 将DF保存为
def saveCoreDFToFiles(spath: str, coreppds: List[Tuple[int, pd.DataFrame]]):
    if not os.path.exists(spath):
        os.makedirs(spath)
    for icore, ipd in coreppds:
        savefilename = os.path.join(spath, str(icore) + ".csv")
        ipd.to_csv(savefilename)


def readCoreDFFromFiles(spath) -> List[Tuple[int, pd.DataFrame]]:
    if not os.path.exists(spath):
        return []
    dirnames = os.listdir(spath)
    reslist = []
    for ifile in dirnames:
        icore = int(os.path.splitext(ifile)[0])
        readfilename = os.path.join(spath, ifile)
        tpd = pd.read_csv(readfilename)
        reslist.append((icore, tpd))
    return reslist


def subtractLastLineFromDataFrame(df: pd.DataFrame, columns: List) -> Union[None, pd.DataFrame]:
    df = df.copy()
    if len(df) <= 1:
        return None
    # 先将整个表格往上一隔
    dfcolumns_1 = df.loc[:, columns].shift(periods=-1, axis=0, fill_value=0)
    # 然后相减
    dfcolumns_2 = dfcolumns_1 - df.loc[:, columns]
    # 然后下一一位
    df.loc[:, columns] = dfcolumns_2.shift(periods=1, axis=0, fill_value=0)
    return df





"""
将原始数据提取出来
提取各个错误标签出来
"""


def abstractFaultPDDict(df: pd.DataFrame, extraFeature: List[str] = []) -> \
        Union[dict[int, dict], Any]:
    # 获得这个df中所有的错误码的类型
    if FAULT_FLAG not in df.columns.array:
        print("featureExtractionOriginalData 中没有错误标签")
        exit(1)
    # 获得所有的错误码标识
    faults = list(set(list(df.loc[:, FAULT_FLAG])))
    resFaultDF = {}
    for ifault in faults:
        selectLine = df.loc[:]
        fdf = df.loc[df.loc[:, FAULT_FLAG] == ifault, extraFeature]
        resFaultDF[ifault] = fdf
    return resFaultDF







# 合并的两个类型是fault-DataFrame
def mergeTwoDF(dic1: Dict[int, pd.DataFrame], dic2: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
    allfaulty = list(dic1.keys())
    allfaulty.extend(list(dic2.keys()))
    allfauly = list(set(allfaulty))
    resDict = {}
    for ifaulty in allfauly:
        tpd: pd.DataFrame = pd.DataFrame()
        if ifaulty in dic1 and ifaulty in dic2:
            tpd = pd.concat([dic1[ifaulty], dic2[ifaulty]], ignore_index=True)
        elif ifaulty in dic1:
            tpd = dic1[ifaulty]
        elif ifaulty in dic2:
            tpd = dic2[ifaulty]
        resDict[ifaulty] = tpd
    return resDict

# # 得到一个df
#
# def get_time_core_and_faulty(spath : str, filepd: pd.DataFrame):
#     if not os.path.exists(spath):
#         os.makedirs(spath)
#     # 先按照时间段划分
#     pdbytime = splitDataFrameByTime(filepd)
#


# 处理一个文件
# 存储的中间文件都在spath中

def processOneFile(spath: str, filepd: pd.DataFrame, isSlide: bool = True):
    if not os.path.exists(spath):
        os.makedirs(spath)

    # 先按照时间段划分
    pdbytime = splitDataFrameByTime(filepd)

    # 将其保存到 spath/1.时间段划分集合
    print("1.{} 按照时间段划分开始".format(os.path.basename(filename)))
    # tmp/{filename}/1.时间段划分集合
    saveDFListToFiles(spath=os.path.join(spath, "1.时间段划分集合文件"), pds=pdbytime)
    print("{} 按照时间段划分结束".format(os.path.basename(filename)))

    # 对每一个时间段划分
    thisFileFaulty_PD_Dict = {}
    thisTime_core_FileFaulty_PD_Dict = {}
    thisTime_core_PD_Dict = {}
    for i in range(0, len(pdbytime)):
        thisTime_core_PD_Dict[i] = {}
        thisTime_core_FileFaulty_PD_Dict[i] = {}

        print("2.{} 第{}个时间段依照核心划分".format(i, i))
        corepds = SplitDFByCores(pdbytime[i])
        # 将corepds保存出来 以便观察
        # tmp/tData/2.时间段划分集合文件详细信息/第{}时间段分割核心
        tcoresavepath = os.path.join(spath,"2.时间段划分集合文件详细信息", "{}.第{}时间段分割核心".format(i, i))
        saveCoreDFToFiles(tcoresavepath, corepds)
        # 对每个核心特征进行减去前一行
        subcorepds = []
        for icore, ipd in corepds:
            tpd = subtractLastLineFromDataFrame(ipd, accumulationFeatures)
            subcorepds.append((icore, tpd))

        # tmp/{filename}/2.时间段划分集合文件详细信息/
        tcoresavepath = os.path.join(spath, "2.时间段划分集合文件详细信息", "{}.第{}时间段分割核心-减前一行".format(i, i).format(i))
        saveCoreDFToFiles(tcoresavepath, subcorepds)

        # 对每一个核心进行处理
        tcoresavepath = os.path.join(spath, "2.时间段划分集合文件详细信息", "{}.第{}时间段分割核心-减去前一行-分割错误码".format(i, i))
        # 这个文件中错误：DF的字典结构
        for icore, icorepd in subcorepds:
            if icore not in thisTime_core_PD_Dict[i]:
                thisTime_core_PD_Dict[i][icore] = icorepd
            print("3.第{}时间段-{}核心处理中".format(i, icore))
            # 将所有的错误码进行提取
            FaultPDDict = abstractFaultPDDict(icorepd, extraFeature=usedFeature)
            if icore not in thisTime_core_FileFaulty_PD_Dict:
                thisTime_core_FileFaulty_PD_Dict[i][icore] = FaultPDDict
            #  将每个文件中的每个时间段中的每个核心进行错误码划分的结果进行保存
            tcore_fault_savepath = os.path.join(tcoresavepath, str(icore))
            saveFaultyDict(tcore_fault_savepath, FaultPDDict)
            # 合并总的错误
            thisFileFaulty_PD_Dict = mergeTwoDF(thisFileFaulty_PD_Dict, FaultPDDict)
    # 将这个文件中提取到的所有错误码进行保存
    tallsavefaultypath = os.path.join(spath, "3.所有错误码信息")
    saveFaultyDict(tallsavefaultypath, thisFileFaulty_PD_Dict)
    # 返回一个此文件所有错误的的Fault-PD， 返回按照时间段-核心-PD的字典结构， 返回按照时间段-核心-错误码-PD的字典结构
    return thisFileFaulty_PD_Dict, thisTime_core_PD_Dict, thisTime_core_FileFaulty_PD_Dict


"""
将server数据进行合并 
中间数据存储在spath中
"""


def mergeSeverAndProcess(servrtpd: pd.DataFrame, processpd: pd.DataFrame, spath: str = None) -> pd.DataFrame:
    if spath is not None and not os.path.exists(spath):
        os.makedirs(spath)
    suffixes = ("_server", "_process")
    mergedSeverProcessPD = pd.merge(servrtpd, processpd, how='right', on=[TIME_COLUMN_NAME],
                                    suffixes=("_server", "_process"))

    # 先获得所有的含有空行数据, index没有从reset
    haveNullDF = mergedSeverProcessPD.loc[mergedSeverProcessPD.isnull().T.any()]
    # 获得没有空行的数据
    noNullDF = mergedSeverProcessPD.dropna()

    # 将数据进行保存
    haveNullDF: pd.DataFrame
    noNullDF: pd.DataFrame
    # spath/server中没有对应时间的数据.csv
    if len(haveNullDF) != 0:
        tspath = os.path.join(spath, "server中没有对应时间的数据.csv")
        haveNullDF.to_csv(tspath)

    if len(noNullDF) != 0:
        tspath = os.path.join(spath, "server中有对应时间的数据.csv")
        noNullDF.to_csv(tspath)
    # 将index重新整理一下返回
    # 需要将 faultFlag_process转化为faultFlag
    noNullDF = noNullDF.rename(columns={FAULT_FLAG + suffixes[1]: FAULT_FLAG})
    return noNullDF.reset_index(drop=True)


# 将时间序列的秒这一项都变成秒
def changeTimeColumns_process(df: pd.DataFrame) -> pd.DataFrame:
    tpd = df.loc[:, [TIME_COLUMN_NAME]].apply(lambda x: TranslateTimeListStrToStr(x.to_list()), axis=0)
    df.loc[:, TIME_COLUMN_NAME] = tpd.loc[:, TIME_COLUMN_NAME]
    return df


def changeTimeColumns_server(df: pd.DataFrame) -> pd.DataFrame:
    tpd = df.loc[:, [TIME_COLUMN_NAME]].apply(lambda x: TranslateTimeListStrToStr(x.to_list(), '%Y/%m/%d %H:%M'),
                                              axis=0)
    df.loc[:, TIME_COLUMN_NAME] = tpd.loc[:, TIME_COLUMN_NAME]
    return df


"""
1. 合并server和process数据
2. 将每个process的每个核的每个错误码进行输出, 没有特征提取
3. 合并所有的错误码
"""

if __name__ == "__main__":
    spath = "tmp/tData-10-18/多机-E5-process-server-3KM"
    all_faulty_pd_dict = {}
    orginal_all_faulty_pd_dict = {}
    isSlideWin = True  # True代表这个step为win， False代表step为1

    severpd = pd.read_csv(datapathsever)
    changeTimeColumns_server(severpd)

    filename_time_core_pdDict = {}
    filename_time_core_faultDict = {}
    for ipath in datapath:
        filename = os.path.basename(ipath)
        filename = os.path.splitext(filename)[0]
        # tmp/{}/0.合并server和process数据
        mergedpath = os.path.join(spath, "0.所有文件处理过程", filename, "0.合并server和process数据")

        processpd = pd.read_csv(ipath)
        # 改变一个文件的时间， 因为server文件和process文件文件中的时间不对
        changeTimeColumns_process(processpd)
        server_process_pd = mergeSeverAndProcess(severpd, processpd, mergedpath)
        # tmp/{filename}
        onefile_Faulty_PD_Dict, time_core_pdDict, time_core_faultDict = processOneFile(spath=os.path.join(spath, "0.所有文件处理过程", filename), filepd=server_process_pd,
                                                isSlide=isSlideWin)
        all_faulty_pd_dict = mergeTwoDF(onefile_Faulty_PD_Dict, all_faulty_pd_dict)
        filename_time_core_pdDict[filename] = time_core_pdDict
        filename_time_core_faultDict[filename] = time_core_faultDict
    # 将所有的信息进行保存
    tallsavefaultypath = os.path.join(spath, "1.所有process错误码信息")
    saveFaultyDict(tallsavefaultypath, all_faulty_pd_dict)

    #将filename-time-core进行保存
    sspath = os.path.join(spath, "2.filename-time-core")
    saveFilename_Time_Core_pdDict(sspath, filename_time_core_pdDict)

    # 将filename-time-core进行保存
    sspath = os.path.join(spath, "3.filename-time-core-faulty")
    saveFilename_Time_Core_Faulty_pdDict(sspath, filename_time_core_faultDict)
