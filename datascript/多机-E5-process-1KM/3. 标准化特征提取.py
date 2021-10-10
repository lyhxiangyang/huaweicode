


"""
作用将在第二步中生成的标准化文件进行特征提取
"""
import os.path
from typing import List, Union, Any

import pandas as pd

from utils.DataFrameOperation import SortLabels, PushLabelToFirst, PushLabelToEnd
from utils.DefineData import TIME_COLUMN_NAME, FAULT_FLAG
from utils.FileSaveRead import saveFaultyDict

"""
保证这个df的时间序列是连续的，并且可能包含多个错误类型
保证带有time 和标签特征
"""
def featureExtraction(df: pd.DataFrame, windowSize: int = 5, silidWindows: bool = True, extraFeature: List[str] = []) -> \
        Union[dict[int, dict], Any]:
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
        # 添加时间
        if TIME_COLUMN_NAME not in resDict[realLabel]:
            resDict[realLabel][TIME_COLUMN_NAME] = []
        if FAULT_FLAG not in resDict[realLabel]:
            resDict[realLabel][FAULT_FLAG] = []
        resDict[realLabel][TIME_COLUMN_NAME].append(nowtime)
        resDict[realLabel][FAULT_FLAG].append(realLabel)
        # 对每个特征进行选择
        for featurename in mycolumnslist:
            if featurename not in extraFeature:
                continue
            if featurename == TIME_COLUMN_NAME or featurename == FAULT_FLAG:
                continue

            calSerials = tpd.loc[:, featurename]

            # min min_diff
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
                resDict[ifaulty][ifeaturename][0] = ilist[1]

    # 将resDict 转化为 resDFDict
    resDFDict = {}
    for ifaulty, featureDict in resDict.items():
        resDataFrame = pd.DataFrame(data=featureDict)
        resDataFrame = SortLabels(resDataFrame)
        resDataFrame = PushLabelToFirst(resDataFrame, label=TIME_COLUMN_NAME)
        resDataFrame = PushLabelToEnd(resDataFrame, label=FAULT_FLAG)
        resDataFrame.fillna(0, inplace=True)
        resDFDict[ifaulty] = resDataFrame

    return resDFDict







if __name__ == "__main__":
    rpath = "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据"
    spath = "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取"
    extractionFaulty = [0, 15]
    extractionFeatures = ["load1"]
    isSlideWin = True  # True代表这个step为win， False代表step为1
    winsize = 3

    for ifault in extractionFeatures:
        filename = str(ifault) + ".csv"
        faultfilepath = os.path.join(rpath, filename)
        if not os.path.exists(faultfilepath):
            print("{} 文件不存在".format(filename))
            continue
        tpd = pd.read_csv(faultfilepath)
        faultDict = featureExtraction(tpd, windowSize=winsize, silidWindows=isSlideWin, extraFeature=extractionFeatures)
        saveFaultyDict(spath, faultDict)



