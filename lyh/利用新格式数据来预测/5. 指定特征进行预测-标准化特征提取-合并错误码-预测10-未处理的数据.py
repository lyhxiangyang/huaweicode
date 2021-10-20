from typing import Union, Any, List

import pandas as pd
from pandas import DataFrame, Series
from pandas.io.parsers import TextFileReader

from Classifiers.TrainToTest import ModelTrainAndTest
from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import FAULT_FLAG

# 1KM数据预测
# trainNormalDataPath = [
#     "tmp/tData-10-18/多机-E5-process-server-1KM/7.特征提取所有错误/0.csv",
#     "tmp/tData-10-18/多机-红区-process-server-1KM/7.特征提取所有错误/0.csv",
# ]
#
# trainAbnormalDataPath = [
#     "tmp/tData-10-18/多机-E5-process-server-1KM/7.特征提取所有错误/13.csv",
#     "tmp/tData-10-18/多机-E5-process-server-1KM/7.特征提取所有错误/14.csv",
#     "tmp/tData-10-18/多机-E5-process-server-1KM/7.特征提取所有错误/15.csv",
# ]
#
# testNormalDataPath = [
#     "tmp/tData-10-18/多机-红区-process-server-1KM/7.特征提取所有错误/0.csv",
# ]
#
# testAbnormalDataPath = [
#     "tmp/tData-10-18/多机-红区-process-server-1KM/7.特征提取所有错误/13.csv",
#     "tmp/tData-10-18/多机-红区-process-server-1KM/7.特征提取所有错误/14.csv",
#     "tmp/tData-10-18/多机-红区-process-server-1KM/7.特征提取所有错误/15.csv",
# ]

trainNormalDataPath = [
    "tmp/tData-10-18/多机-E5-process-server-3KM/9.特征提取所有错误-处理首尾/0.csv",
    "tmp/tData-10-18/多机-红区-process-server-3KM/9.特征提取所有错误-处理首尾/0.csv",
]

trainAbnormalDataPath = [
    "tmp/tData-10-18/多机-E5-process-server-3KM/9.特征提取所有错误-处理首尾/13.csv",
    "tmp/tData-10-18/多机-E5-process-server-3KM/9.特征提取所有错误-处理首尾/14.csv",
    "tmp/tData-10-18/多机-E5-process-server-3KM/9.特征提取所有错误-处理首尾/15.csv",
]

testNormalDataPath = [
    "tmp/tData-10-18/多机-红区-process-server-3KM/9.特征提取所有错误-处理首尾/0.csv",
]

testAbnormalDataPath = [
    "tmp/tData-10-18/多机-红区-process-server-3KM/9.特征提取所有错误-处理首尾/13.csv",
    "tmp/tData-10-18/多机-红区-process-server-3KM/9.特征提取所有错误-处理首尾/14.csv",
    "tmp/tData-10-18/多机-红区-process-server-3KM/9.特征提取所有错误-处理首尾/15.csv",
]


def get_List_pre_suffix(clist: List[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [i for i in clist if i.startswith(prefix) and i.endswith(suffix)]
def get_List_pre_nosuffix(clist: List[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [i for i in clist if i.startswith(prefix) and not i.endswith(suffix)]
def get_List_nosuffix(clist: List[str], suffix: str="") -> List[str]:
    if suffix == "":
        return clist
    return [i for i in clist if not i.endswith(suffix)]


# 将一个DataFrame的FAULT_FLAG重值为ff
def setPDfaultFlag(df: pd.DataFrame, ff: int) -> pd.DataFrame:
    if FAULT_FLAG in df.columns.array:
        df = df.drop(FAULT_FLAG, axis=1)
    lengthpd = len(df)
    ffdict = {FAULT_FLAG: [ff] * lengthpd}
    tpd = pd.DataFrame(data=ffdict)
    tpd = pd.concat([df, tpd], axis=1)
    return tpd

"""
指定标准化之后并且特征提取的数据， 去除后缀为_diff的特征，对强度为15的数据进行验证
"""
# 预测未处理数据
if __name__ == "__main__":
    spath = "tmp/E5多机预测红区-单特征load1-标准化特征提取-合并错误"
    # trainedPDList: list[Union[Union[TextFileReader, Series, DataFrame, None], Any]] = []
    # for i in trainDataPath:
    #     tpd = pd.read_csv(i)
    #     trainedPDList.append(tpd)
    # allTrainedPD, err = mergeDataFrames(trainedPDList)
    # allTrainedPD: pd.DataFrame
    # if err:
    #     print("train合并出错")
    #     exit(1)
    #
    # testPDList = []
    # for i in testDataPath:
    #     tpd = pd.read_csv(i)
    #     testPDList.append(tpd)
    # allTestPD, err = mergeDataFrames(testPDList)
    # if err:
    #     print("test合并出错")
    #     exit(1)
    #==================================================================读取训练的normal数据
    trainNormalList = []
    for i in trainNormalDataPath:
        tpd = pd.read_csv(i)
        trainNormalList.append(tpd)
    #==================================================================读取训练的abnormal数据
    trainAbnormalList = []
    for i in trainAbnormalDataPath:
        tpd = pd.read_csv(i)
        # 修改list的标签
        tpd = setPDfaultFlag(tpd, 10)
        trainAbnormalList.append(tpd)
    #==================================================================读取测试的normal数据
    testNormalList = []
    for i in testNormalDataPath:
        tpd = pd.read_csv(i)
        testNormalList.append(tpd)
    #==================================================================读取训练的normal数据
    testAbnormalList = []
    for i in testAbnormalDataPath:
        tpd = pd.read_csv(i)
        tpd = setPDfaultFlag(tpd, 10)
        testNormalList.append(tpd)

    #==================================================================将所有的训练数据进行合并
    allTrainedPD, err = mergeDataFrames(trainNormalList + trainAbnormalList)
    if err:
        print("训练数据合并失败")
        exit(1)
    #==================================================================将所有的测试数据进行合并
    allTestPD, err = mergeDataFrames(testNormalList + testAbnormalList)
    if err:
        print("测试数据合并失败")
        exit(1)



    # 获得需要训练的特征
    allfeatureload1_nosuffix = get_List_pre_nosuffix(list(allTrainedPD.columns.array),prefix="load1_", suffix="_diff")
    # 使用平均值 最大值 和最小值
    allfeatureload1_nosuffix = [
        "load1_mean",
        "load1_max",
        "load1_min",
        "load1_percentage_50",
    ]

    print("选择的特征：{}".format(str(allfeatureload1_nosuffix)))
    ModelTrainAndTest(allTrainedPD, allTestPD, spath=spath, selectedFeature=allfeatureload1_nosuffix, modelpath="Classifiers/saved_model/tmp_load1_nosuffix")

