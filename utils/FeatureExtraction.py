"""
这个文件的本意是包含特征提取的函数
比如：
1. 将滑动窗口设置，然后提取最小值、最大值等数值
"""
from typing import Tuple, Union, List

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame, SortLabels, PushLabelToFirst, PushLabelToEnd
from utils.DefineData import *

"""
函数功能：将一个DataFrame的结构中的数据按照滑动窗口大小提取如下特征值
# - 最小值
# - 最大值
# - 5%大的值
# - 25% 大的值
# - 50%大的值
# - 75%大的值
# - 95%大的值
# - 平均数
# - 方差
# - 倾斜度
# - 峰度

备注：保证传入进来的DataFrame的FaultFlag只有一类
"""


def featureExtraction(featurePD: pd.DataFrame, windowSize: int = 5) -> Union[
    Tuple[None, bool], Tuple[Union[pd.DataFrame, pd.Series], bool]]:
    # 1个特征会生成很多新的特征, 下面是这个特征需要添加的后缀名
    suffix_name = ["_min", "_max", "_percentage_5", "_percentage_25", "_percentage_50", "_percentage_75",
                   "_percentage_95", "_mean", "_var", "_std", "_skewness", "_kurtosis"]
    # 查分的后缀名 上面suffix_name中的_diff是需要的，用来在字典中生成对应的keys
    Diff_suffix = "_diff"

    # 一个内部函数用来获得列表最后一位
    def getListEnd(list1: List):
        if len(list1) == 0:
            return 0
        return list1[-1]

    # 长度为0 不进行处理
    if len(featurePD) == 0:
        return None, True

    nowFaultFlag = featurePD[FAULT_FLAG][0]

    # 获得下一个窗口大小得函数
    def getnext(beginpos: int) -> Tuple[int, int]:
        endpos = beginpos + windowSize
        if endpos > len(featurePD):
            endpos = len(featurePD)
        return beginpos, endpos

    # 保存结果的返回值
    resDataFrame = pd.DataFrame()

    for featurename in featurePD.columns.array:
        # 先去除掉要排除的特征值
        if featurename in EXCLUDE_FEATURE_NAME:
            continue

        # 特征名字featurename
        # 接下来 创建一个字典，对应每个特征名字，value是一个数组
        # 为saveTable添加列项
        myColumeNamesList = [featurename + suistr for suistr in suffix_name]
        # myColumeNamesValues = [[]] * len(myColumeNames) #这里面存储的都是链表, 比如上面长度为2，那么这个值为[[], []]
        myColumeNamesDict = dict(zip(myColumeNamesList, [[] for _ in range(len(myColumeNamesList))]))

        beginLine = 0
        # 接下来按照滑动窗口大小，将对应的特征值计算一遍
        while beginLine + windowSize <= len(featurePD):
            # 获得特征值中对应滑动窗口大小的数值。

            beginLine, endLine = getnext(beginLine)
            # print(beginLine, endLine)
            # 获得对应一列的数据
            calSerials = featurePD.iloc[beginLine:endLine][featurename]
            # print(list(calSerials))

            newfeatureName = featurename + "_min"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.min()
            # 判断是否有key的存在，不存在就新建
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            # print(newfeatureName, calSerials.min())
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_max"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.max()

            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_5"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.05)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_25"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.25)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_50"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.5)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_75"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.75)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_95"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.95)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_var"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.var()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_std"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.std()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_mean"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.mean()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_skewness"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.skew()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_kurtosis"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.kurtosis()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            # 修改起始行号
            beginLine = endLine

        # 我们差分特征的第一个选项是有问题的默认和第一个值一样，我们将其调整成为和第二一样
        for ikey, ilist in myColumeNamesDict.items():
            if ikey.endswith(Diff_suffix) and len(ilist) > 2:
                ilist[0] = ilist[1]

        # 将搜集到的这个特征的信息保存到新的DataFrame中
        # for newfeatureName in myColumeNamesList:
        #     resDataFrame[newfeatureName] = myColumeNamesDict[newfeatureName]
        tDF = pd.DataFrame(myColumeNamesDict)
        # if isEmptyInDataFrame(tDF):
        #     print("2. DataFrame is None")
        #     print("特征名字：", featurename)
        #     tDF.to_csv("tmp/1.error.csv")
        #     exit(1)

        resDataFrame = pd.concat([resDataFrame, tDF], axis=1)
        # if isEmptyInDataFrame(resDataFrame):
        #     print("3. DataFram is None")
    # 为新的DataFrame添加标签
    td = {FAULT_FLAG: [nowFaultFlag for i in range(0, len(resDataFrame))]}
    tpd = pd.DataFrame(td)
    resDataFrame = pd.concat([resDataFrame, tpd], axis=1)

    # 将结果排一下顺序
    resDataFrame = SortLabels(resDataFrame)
    resDataFrame = PushLabelToFirst(resDataFrame, label=TIME_COLUMN_NAME)
    resDataFrame = PushLabelToEnd(resDataFrame, label=FAULT_FLAG)

    if DEBUG:
        print("featureExtraction".center(40, "*"))
        print(resDataFrame.iloc[:, 0:2])
        print("end".center(40, "*"))

    return resDataFrame, False


"""
函数功能：将一个DataFrame的结构中的数据按照滑动窗口大小提取如下特征值
# - 最小值
# - 最大值
# - 5%大的值
# - 25% 大的值
# - 50%大的值
# - 75%大的值
# - 95%大的值
# - 平均数
# - 方差
# - 倾斜度
# - 峰度

备注：保证传入进来的DataFrame的FaultFlag只有一类
"""


def featureExtraction_excludeAccumulation(featurePD: pd.DataFrame, windowSize: int = 5,
                                          accumulateFeature: List[str] = []) -> Union[
    Tuple[None, bool], Tuple[Union[pd.DataFrame, pd.Series], bool]]:
    # 1个特征会生成很多新的特征, 下面是这个特征需要添加的后缀名
    suffix_name = ["_min", "_max", "_percentage_5", "_percentage_25", "_percentage_50", "_percentage_75",
                   "_percentage_95", "_mean", "_var", "_std", "_skewness", "_kurtosis"]
    # 查分的后缀名 上面suffix_name中的_diff是需要的，用来在字典中生成对应的keys
    Diff_suffix = "_diff"

    # 一个内部函数用来获得列表最后一位
    def getListEnd(list1: List):
        if len(list1) == 0:
            return 0
        return list1[-1]

    # 长度为0 不进行处理
    if len(featurePD) == 0:
        return None, True

    nowFaultFlag = featurePD[FAULT_FLAG][0]

    # 获得下一个窗口大小得函数
    def getnext(beginpos: int) -> Tuple[int, int]:
        endpos = beginpos + windowSize
        if endpos > len(featurePD):
            endpos = len(featurePD)
        return beginpos, endpos

    # 保存结果的返回值
    resDataFrame = pd.DataFrame()

    for featurename in featurePD.columns.array:
        # 先去除掉要排除的特征值
        if featurename in EXCLUDE_FEATURE_NAME:
            continue

        # 特征名字featurename
        # 接下来 创建一个字典，对应每个特征名字，value是一个数组
        # 为saveTable添加列项
        myColumeNamesList = [featurename + suistr for suistr in suffix_name]
        # myColumeNamesValues = [[]] * len(myColumeNames) #这里面存储的都是链表, 比如上面长度为2，那么这个值为[[], []]
        myColumeNamesDict = dict(zip(myColumeNamesList, [[] for _ in range(len(myColumeNamesList))]))

        beginLine = 0
        # 接下来按照滑动窗口大小，将对应的特征值计算一遍
        while beginLine + windowSize <= len(featurePD):
            # 获得特征值中对应滑动窗口大小的数值。

            beginLine, endLine = getnext(beginLine)
            # print(beginLine, endLine)
            # 获得对应一列的数据
            calSerials = featurePD.iloc[beginLine:endLine][featurename]
            # print(list(calSerials))

            newfeatureName = featurename + "_min"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.min()
            # 判断是否有key的存在，不存在就新建
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            # print(newfeatureName, calSerials.min())
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_max"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.max()

            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_5"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.05)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_25"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.25)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_50"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.5)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_75"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.75)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_percentage_95"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.quantile(0.95)
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_var"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.var()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_std"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.std()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_mean"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.mean()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_skewness"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.skew()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            newfeatureName = featurename + "_kurtosis"
            newfeatureNameDiff = newfeatureName + Diff_suffix
            featurevalue = calSerials.kurtosis()
            if newfeatureNameDiff not in myColumeNamesDict.keys():
                myColumeNamesDict[newfeatureNameDiff] = []
            myColumeNamesDict[newfeatureNameDiff].append(
                featurevalue - getListEnd(myColumeNamesDict[newfeatureName]))
            myColumeNamesDict[newfeatureName].append(featurevalue)
            if newfeatureName is None:
                return None, True

            # 修改起始行号
            beginLine = endLine

        # 我们差分特征的第一个选项是有问题的默认和第一个值一样，我们将其调整成为和第二一样
        for ikey, ilist in myColumeNamesDict.items():
            if ikey.endswith(Diff_suffix) and len(ilist) > 2:
                ilist[0] = ilist[1]

        # 将搜集到的这个特征的信息保存到新的DataFrame中
        # for newfeatureName in myColumeNamesList:
        #     resDataFrame[newfeatureName] = myColumeNamesDict[newfeatureName]
        tDF = pd.DataFrame(myColumeNamesDict)
        # if isEmptyInDataFrame(tDF):
        #     print("2. DataFrame is None")
        #     print("特征名字：", featurename)
        #     tDF.to_csv("tmp/1.error.csv")
        #     exit(1)

        resDataFrame = pd.concat([resDataFrame, tDF], axis=1)
        # if isEmptyInDataFrame(resDataFrame):
        #     print("3. DataFram is None")
    # 为新的DataFrame添加标签
    td = {FAULT_FLAG: [nowFaultFlag for i in range(0, len(resDataFrame))]}
    tpd = pd.DataFrame(td)
    resDataFrame = pd.concat([resDataFrame, tpd], axis=1)

    # 将结果排一下顺序
    resDataFrame = SortLabels(resDataFrame)
    resDataFrame = PushLabelToFirst(resDataFrame, label=TIME_COLUMN_NAME)
    resDataFrame = PushLabelToEnd(resDataFrame, label=FAULT_FLAG)

    if DEBUG:
        print("featureExtraction".center(40, "*"))
        print(resDataFrame.iloc[:, 0:2])
        print("end".center(40, "*"))

    return resDataFrame, False
