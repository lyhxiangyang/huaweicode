import os
from typing import List, Union, Any

import pandas as pd

from utils.DataFrameOperation import PushLabelToFirst, PushLabelToEnd, SortLabels, mergeTwoDF
from utils.DefineData import FAULT_FLAG, TIME_COLUMN_NAME
from utils.FileSaveRead import readFilename_Time_Core_pdDict, saveFilename_Time_Core_pdDict, saveFaultyDict

"""
保证这个df的时间序列是连续的，并且可能包含多个错误类型
保证带有time 和标签特征
"""


def featureExtraction(df: pd.DataFrame, windowSize: int = 5, windowRealSize: int = 1, silidWindows: bool = True,
                      extraFeature=None) -> \
        Union[dict[int, dict], Any]:
    if extraFeature is None:
        extraFeature = []
    lendf = len(df)
    resFaulty_PDDict = {}
    resPD = {}
    if windowSize > lendf:
        return resFaulty_PDDict

    # suffix_name = ["_min", "_max", "_percentage_5", "_percentage_25", "_percentage_50", "_percentage_75",
    #                "_percentage_95", "_mean", "_var", "_std", "_skewness", "_kurtosis"]
    # # 查分的后缀名 上面suffix_name中的_diff是需要的，用来在字典中生成对应的keys
    # Diff_suffix = "_diff"
    # # 得到所有的特征值
    mycolumnslist = list(df.columns.array)

    # mycolumns = [ic + isuffix for ic in mycolumns for isuffix in suffix_name]
    # mycolumns.extend([i + Diff_suffix for i in mycolumns])

    # def getRealLabel(labels: pd.Series) -> int:
    #     for i in labels:
    #         if i != 0:
    #             return i
    #     return 0
    def getRealLabel(labels: pd.Series) -> int:
        inum = 0
        flag = 0
        for i in labels:
            if i != 0:
                flag = i
                inum += 1
        if inum == windowRealSize:
            return flag
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
        if realLabel not in resFaulty_PDDict:
            resFaulty_PDDict[realLabel] = {}
        # 添加时间
        if TIME_COLUMN_NAME not in resFaulty_PDDict[realLabel]:
            resFaulty_PDDict[realLabel][TIME_COLUMN_NAME] = []
        if FAULT_FLAG not in resFaulty_PDDict[realLabel]:
            resFaulty_PDDict[realLabel][FAULT_FLAG] = []
        resFaulty_PDDict[realLabel][TIME_COLUMN_NAME].append(nowtime)
        resFaulty_PDDict[realLabel][FAULT_FLAG].append(realLabel)
        if TIME_COLUMN_NAME not in resPD:
            resPD[TIME_COLUMN_NAME] = []
        if FAULT_FLAG not in resPD:
            resPD[FAULT_FLAG] = []
        resPD[TIME_COLUMN_NAME].append(nowtime)
        resPD[FAULT_FLAG].append(realLabel)

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
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.min()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)
            # max max_diff
            newfeatureName = featurename + "_max"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.max()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # percentage_50
            newfeatureName = featurename + "_percentage_50"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.quantile(0.5)
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # var
            newfeatureName = featurename + "_var"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.var()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # std
            newfeatureName = featurename + "_std"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.std()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # mean
            newfeatureName = featurename + "_mean"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.mean()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # skewness
            newfeatureName = featurename + "_skewness"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.skew()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

            # kurtosis
            newfeatureName = featurename + "_kurtosis"
            newfeatureNameDiff = newfeatureName + "_diff"
            if newfeatureName not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureName] = []
            if newfeatureNameDiff not in resFaulty_PDDict[realLabel]:
                resFaulty_PDDict[realLabel][newfeatureNameDiff] = []
            if newfeatureName not in resPD:
                resPD[newfeatureName] = []
            if newfeatureNameDiff not in resPD:
                resPD[newfeatureNameDiff] = []
            featurevalue = calSerials.kurtosis()
            featurevaluediff = featurevalue - getListEnd(resFaulty_PDDict[realLabel][newfeatureName])
            resFaulty_PDDict[realLabel][newfeatureName].append(featurevalue)
            resFaulty_PDDict[realLabel][newfeatureNameDiff].append(featurevaluediff)
            resPD[newfeatureName].append(featurevalue)
            resPD[newfeatureNameDiff].append(featurevaluediff)

        if silidWindows:
            beginLineNumber += windowSize
            endLineNumber += windowSize
        else:
            beginLineNumber += 1
            endLineNumber += 1

    # # 将所有resDict中的所有数据diff的第一列中的数据替换为第二个
    # for ifaulty, featureDict in resFaulty_PDDict.items():
    #     for ifeaturename, ilist in featureDict.items():
    #         if not ifeaturename.endswith("_diff"):
    #             continue
    #         if len(ilist) >= 2:
    #             resFaulty_PDDict[ifaulty][ifeaturename][0] = ilist[1]

    # 将resDict 转化为 resDFDict
    resDFDict = {}
    for ifaulty, featureDict in resFaulty_PDDict.items():
        resDataFrame = pd.DataFrame(data=featureDict)
        resDataFrame = SortLabels(resDataFrame)
        resDataFrame = PushLabelToFirst(resDataFrame, label=TIME_COLUMN_NAME)
        resDataFrame = PushLabelToEnd(resDataFrame, label=FAULT_FLAG)
        resDataFrame.fillna(0, inplace=True)
        resDFDict[ifaulty] = resDataFrame
    # 原始文件的变化
    originDF = pd.DataFrame(data=resPD)
    originDF = SortLabels(originDF)
    originDF = PushLabelToFirst(originDF, label=TIME_COLUMN_NAME)
    originDF = PushLabelToEnd(originDF, label=FAULT_FLAG)
    originDF.fillna(0, inplace=True)
    # 原始文件的处理， 以及其中的错误码
    return originDF, resDFDict


def FeaExtra_file_time_core(ftcDict, windowSize: int = 5, windowRealSize: int = 1,
                            silidWindows: bool = True,
                            extraFeature=None):
    resDict = {}
    fault_PDDict = {}
    for filename, time_core_pdDict in ftcDict.items():
        resDict[filename] = {}
        for time, core_pdDict in time_core_pdDict.items():
            resDict[filename][time] = {}
            print("filename:{}-time:{}".format(filename, time))
            for icore, tpd in core_pdDict.items():
                fePD, fault_Dict = featureExtraction(tpd, windowSize, windowRealSize, silidWindows, extraFeature)
                resDict[filename][time][icore] = fePD
                fault_PDDict = mergeTwoDF(fault_Dict, fault_PDDict)
    return resDict, fault_PDDict


if __name__ == "__main__":
    spath = "tmp/tData-10-18/多机-E5-process-server-1KM"
    extractedFeaturee = ["load1"]
    # 将所有的标准化数据读取
    file_time_core_standardPath = "tmp/tData-10-18/多机-E5-process-server-1KM/4.filename-time-core-标准化"
    print("读取filename-time-core数据中".center(40, "*"))
    file_time_core_standardDict = readFilename_Time_Core_pdDict(file_time_core_standardPath)
    # 进行特征提取
    print("特征提取中".center(40, "*"))
    file_time_core_standard_FeatureExtractionDict, allFault_PDDict = FeaExtra_file_time_core(file_time_core_standardDict, windowSize=3, windowRealSize=3, silidWindows=True,
                                                                                             extraFeature=extractedFeaturee)
    # 将特征提取之后的文件进行保存
    print("filename-time-core-标准化-特征提取开始".center(40, "*"))
    sspath = os.path.join(spath, "6.filename-time-core-标准化-特征提取")
    saveFilename_Time_Core_pdDict(sspath, file_time_core_standard_FeatureExtractionDict)
    print("filename-time-core-标准化-特征提取结束".center(40, "*"))

    # 将获得的所有特征提取之后的错误进行保存
    sspath = os.path.join(spath, "7.特征提取所有错误")
    saveFaultyDict(sspath, allFault_PDDict)
