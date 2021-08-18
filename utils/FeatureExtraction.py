"""
这个文件的本意是包含特征提取的函数
比如：
1. 将滑动窗口设置，然后提取最小值、最大值等数值
"""
from typing import Tuple

import pandas as pd
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


def featureExtraction(featurePD: pd.DataFrame, windowSize: int = 5) -> Tuple[pd.DataFrame, bool]:
    # 1个特征会生成很多新的特征, 下面是这个特征需要添加的后缀名
    suffix_name = ["_min", "_max", "_percentage_5", "_percentage_25", "_percentage_50", "_percentage_75",
                   "_percentage_95", "_mean", "_var", "_std", "_skewness", "_kurtosis"]
    # 长度为0 不进行处理
    if len(featurePD) == 0:
        return None, True

    nowFaultFlag = featurePD[FAULT_FLAG][0]

    # 获得下一个窗口大小得函数
    def getnext(beginpos: int) -> Tuple[int, int]:
        endpos = beginpos + windowSize
        if endpos > len(featurePD):
            endpos = featurePD
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
        while beginLine < len(featurePD):
            # 获得特征值中对应滑动窗口大小的数值。

            beginLine, endLine = getnext(beginLine)
            print(beginLine, endLine)
            # 获得对应一列的数据
            calSerials = featurePD.iloc[beginLine:endLine][featurename]
            print(list(calSerials))

            newfeatureName = featurename + "_min"
            myColumeNamesDict[newfeatureName].append(calSerials.min())
            print(newfeatureName, calSerials.min())

            newfeatureName = featurename + "_max"
            myColumeNamesDict[newfeatureName].append(calSerials.max())

            newfeatureName = featurename + "_percentage_5"
            myColumeNamesDict[newfeatureName].append(calSerials.quantile(0.05))

            newfeatureName = featurename + "_percentage_25"
            myColumeNamesDict[newfeatureName].append(calSerials.quantile(0.25))

            newfeatureName = featurename + "_percentage_50"
            myColumeNamesDict[newfeatureName].append(calSerials.quantile(0.50))

            newfeatureName = featurename + "_percentage_75"
            myColumeNamesDict[newfeatureName].append(calSerials.quantile(0.75))

            newfeatureName = featurename + "_percentage_95"
            myColumeNamesDict[newfeatureName].append(calSerials.quantile(0.95))

            newfeatureName = featurename + "_var"
            myColumeNamesDict[newfeatureName].append(calSerials.var())

            newfeatureName = featurename + "_std"
            myColumeNamesDict[newfeatureName].append(calSerials.std())

            newfeatureName = featurename + "_mean"
            myColumeNamesDict[newfeatureName].append(calSerials.mean())

            newfeatureName = featurename + "_skewness"
            myColumeNamesDict[newfeatureName].append(calSerials.skew())

            newfeatureName = featurename + "_kurtosis"
            myColumeNamesDict[newfeatureName].append(calSerials.kurtosis())

            # 修改起始行号
            beginLine = endLine
        # 将搜集到的这个特征的信息保存到新的DataFrame中
        for newfeatureName in myColumeNamesList:
            resDataFrame[newfeatureName] = myColumeNamesDict[newfeatureName]
    # 为新的DataFrame添加标签
    resDataFrame[FAULT_FLAG] = nowFaultFlag

    if DEBUG:
       print("featureExtraction".center(40, "*"))
       print(resDataFrame.iloc[:, 0:2])
       print("end".center(40, "*"))

    return resDataFrame, False






