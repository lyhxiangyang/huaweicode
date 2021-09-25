import os
from re import T
from typing import List, Tuple, Union, Dict

import pandas as pd

from utils.DataFrameOperation import SortLabels, PushLabelToFirst, PushLabelToEnd
from utils.DefineData import TIME_COLUMN_NAME, TIME_INTERVAL, CPU_FEATURE, FAULT_FLAG, WINDOWS_SIZE
from utils.FileSaveRead import saveFaultyDict
from utils.ProcessData import TranslateTimeToInt
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


datapath = [
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-3.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-4.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-5.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-6.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-7.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-8.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-9.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-10.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-11.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-12.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-13.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-14.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-15.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-16.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-17.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-18.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-19.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-20.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-21.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-22.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-23.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-24.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-25.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-26.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-27.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-28.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-29.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-30.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-31.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-32.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-33.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-34.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-35.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-36.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-37.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-38.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-39.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-40.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-41.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-42.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-43.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-44.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-45.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-46.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-47.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-48.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-49.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-50.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-51.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-52.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-53.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-54.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-55.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-56.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-57.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-58.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-59.csv",
    "D:/HuaweiMachine/数据分类/wrf/多机/E5/1KM\异常数据\wrf_1km_multi_43\wrf_1km_e5-43_process-60.csv",
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
保证这个df的时间序列是连续的，并且可能包含多个错误类型
保证带有time 和标签特征
"""
def featureExtraction(df: pd.DataFrame, windowSize: int = 5, silidWindows: bool = True, extraFeature : List[str] = []) -> Dict[int, pd.DataFrame]:
    lendf = len(df)
    resDict = {}
    if windowSize > lendf:
       return resDict

    # suffix_name = ["_min", "_max", "_percentage_5", "_percentage_25", "_percentage_50", "_percentage_75",
    #                "_percentage_95", "_mean", "_var", "_std", "_skewness", "_kurtosis"]
    # # 查分的后缀名 上面suffix_name中的_diff是需要的，用来在字典中生成对应的keys
    # Diff_suffix = "_diff"
    # # 得到所有的特征值
    mycolumnslist = list(df.columns.array)
    # mycolumns = [ic + isuffix for ic in mycolumns for isuffix in suffix_name]
    # mycolumns.extend([i + Diff_suffix for i in mycolumns])


    def getRealLabel(labels: pd.Series) -> int:
        for i in labels:
            if i != 0:
                return i
        return 0
    def getListEnd(list1: List):
        if len(list1) == 0:
            return 0
        return list1[-1]



    beginLineNumber = 0
    endLineNumber = windowSize

    while endLineNumber <= lendf:

        tpd = df.iloc[beginLineNumber:endLineNumber, :]
        nowtime = tpd.loc[beginLineNumber, TIME_COLUMN_NAME]
        realLabel = getRealLabel(tpd.loc[:, FAULT_FLAG])
        if realLabel not in resDict:
           resDict[realLabel] = {}
        # 对每个特征进行选择
        for featurename in mycolumnslist:
            if featurename not in extraFeature:
                continue

            calSerials = tpd.loc[:, featurename]
            if TIME_COLUMN_NAME not in resDict[realLabel]:
                resDict[realLabel][TIME_COLUMN_NAME] = []
            if FAULT_FLAG not in resDict[realLabel]:
                resDict[realLabel][FAULT_FLAG] = []
            resDict[realLabel][TIME_COLUMN_NAME].append(nowtime)
            resDict[realLabel][FAULT_FLAG].append(realLabel)

            #min min_diff
            newfeatureName = featurename + "_min"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.min()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            # max max_diff
            newfeatureName = featurename + "_max"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.max()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # percentage_50
            newfeatureName = featurename + "_percentage_50"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.quantile(0.5)
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # var
            newfeatureName = featurename + "_var"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.var()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # std
            newfeatureName = featurename + "_std"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.std()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # mean
            newfeatureName = featurename + "_mean"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.mean()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # skewness
            newfeatureName = featurename + "_skewness"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.skew()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)

            # kurtosis
            newfeatureName = featurename + "_kurtosis"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resDict[realLabel]:
                resDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resDict[realLabel]:
                resDict[realLabel][newfeatureNameDiff] = []
            featurevalue = calSerials.kurtosis()
            featurevaluediff = featurevalue - getListEnd(resDict[realLabel][newfeatureName])
            resDict[realLabel][newfeatureName].append(featurevalue)
            resDict[realLabel][newfeatureNameDiff].append(featurevaluediff)


        if silidWindows:
           beginLineNumber += windowSize
           endLineNumber += windowSize
        else:
           beginLineNumber += 1
           endLineNumber += 1

    # 将所有resDict中的所有数据diff的第一列中的数据替换为第二个
    for ifaulty, featureDict in resDict.items():
        for ifeaturename, ilist in featureDict.items():
            if not ifeaturename.endswith("_diff"):
                continue
            if len(ilist) >= 2:
                ilist[0] = ilist[1]

    # 将resDict 转化为 resDFDict
    resDFDict = {}
    for ifaulty, featureDict in resDFDict.items():
        resDataFrame = pd.DataFrame(data=featureDict)
        resDataFrame = SortLabels(resDataFrame)
        resDataFrame = PushLabelToFirst(resDataFrame, label=TIME_COLUMN_NAME)
        resDataFrame = PushLabelToEnd(resDataFrame, label=FAULT_FLAG)
        resDataFrame.fillna(0, inplace=True)
        resDFDict[ifaulty] = resDataFrame

    return resDFDict


# 合并的两个类型是fault-DataFrame
def mergeTwoDF(dic1 :Dict[int, pd.DataFrame], dic2: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
    allfaulty = list(dic1.keys())
    allfaulty.extend(list(dic2.keys()))
    allfauly = list(set(allfaulty))
    resDict = {}
    for ifaulty in allfauly:
        tpd = dic1[ifaulty]
        if ifaulty in dic1 and ifaulty in dic2:
            tpd = pd.concat([dic1[ifaulty], dic2[ifaulty]], ignore_index=True)
        elif ifaulty in dic1:
            tpd = dic1[ifaulty]
        elif ifaulty in dic2:
            tpd = dic2[ifaulty]
        resDict[ifaulty] = tpd
    return resDict





# 处理一个文件
# 存储的中间文件都在spath中

def processOneFile(spath: str, filename: str) :
    if not os.path.exists(spath):
        os.makedirs(spath)
    filepd = pd.read_csv(filename)

    resFaulty_PD_Dict = {}

    # 先按照时间段划分
    pdbytime = splitDataFrameByTime(filepd)

    # 将其保存到 spath/1.时间段划分集合
    print("1.{} 按照时间段划分开始".format(os.path.basename(filename)))
    saveDFListToFiles(spath=os.path.join(spath, "1.时间段划分集合"), pds=pdbytime)
    print("{} 按照时间段划分结束".format(os.path.basename(filename)))

    # 对每一个时间段划分
    for i in range(0, len(pdbytime)):
        print("2.{} 第{}个时间段划分".format(i,i))
        corepds = SplitDFByCores(pdbytime[i])
        # 将corepds保存出来 以便观察
        # tmp/tData/2.第{}时间段分割核心
        tcoresavepath = os.path.join(spath, "2.第{}时间段分割核心".format(i))
        saveCoreDFToFiles(tcoresavepath, corepds)
        # 对每个核心特征进行减去前一行
        subcorepds = []
        for icore, ipd in corepds:
            tpd = subtractLastLineFromDataFrame(ipd, ["user", "system"])
            subcorepds.append((icore, tpd))

        # tmp/tData/2.第{}时间段分割核心-减去前一行
        tcoresavepath = os.path.join(spath, "2.第{}时间段分割核心-减去前一行".format(i))
        saveCoreDFToFiles(tcoresavepath, subcorepds)

        # 对每一个核心进行处理
        for icore, icorepd in subcorepds:
            print("3.第{}时间段-{}核心处理中".format(i,icore))
            fefaultDict = featureExtraction(icorepd, windowSize=WINDOWS_SIZE, silidWindows=True)
            # 将第每个核处理之后得到的错误码进行保存
            # tmp/tData/2.第{}时间段分割核心-减去前一行/icore/*
            tcore_fault_savepath = os.path.join(tcoresavepath, str(icore))
            saveFaultyDict(tcore_fault_savepath, fefaultDict)

            # 合并总的错误
            resFaulty_PD_Dict = mergeTwoDF(resFaulty_PD_Dict, fefaultDict)
    # 将这个文件中提取到的所有错误码进行保存
    tallsavefaultypath = os.path.join(spath, "所有错误码信息")
    saveFaultyDict(tallsavefaultypath, resFaulty_PD_Dict)







if __name__ == "__main__":
    spath = "tmp/tData"
    for ipath in datapath:
        filename = os.path.basename(ipath)
        filename = os.path.splitext(filename)[0]
        processOneFile(spath=os.path.join(spath, filename), filename=ipath)
        break
