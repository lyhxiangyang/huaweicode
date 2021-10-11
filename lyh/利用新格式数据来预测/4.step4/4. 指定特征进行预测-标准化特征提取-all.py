from typing import Union, Any, List

import pandas as pd
from pandas import DataFrame, Series
from pandas.io.parsers import TextFileReader

from Classifiers.TrainToTest import ModelTrainAndTest
from utils.DataFrameOperation import mergeDataFrames

trainDataPath = [
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/0.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/11.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/12.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/13.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/14.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/15.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/0.csv",
]
testDataPath = [
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/0.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/11.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/12.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/13.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/14.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据-特征提取/15.csv",
]


def get_List_pre_suffix(clist: List[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [i for i in clist if i.startswith(prefix) and i.endswith(suffix)]


def get_List_nosuffix(clist: List[str], suffix: str = "") -> List[str]:
    if suffix == "":
        return clist
    return [i for i in clist if not i.endswith(suffix)]


"""
指定标准化之后并且特征提取的数据， 去除后缀为_diff的特征，对强度为15的数据进行验证
"""
if __name__ == "__main__":
    spath = "tmp/E5多机预测红区-单特征load1-标准化特征提取-强度all"
    trainedPDList: list[Union[Union[TextFileReader, Series, DataFrame, None], Any]] = []
    for i in trainDataPath:
        tpd = pd.read_csv(i)
        trainedPDList.append(tpd)
    allTrainedPD, err = mergeDataFrames(trainedPDList)
    allTrainedPD: pd.DataFrame
    if err:
        print("train合并出错")
        exit(1)

    testPDList = []
    for i in testDataPath:
        tpd = pd.read_csv(i)
        testPDList.append(tpd)
    allTestPD, err = mergeDataFrames(testPDList)
    if err:
        print("test合并出错")
        exit(1)

    # 获得需要训练的特征
    allfeatureload1_nosuffix = get_List_nosuffix(list(allTrainedPD.columns.array), suffix="_diff")
    print("选择的特征：{}".format(str(allfeatureload1_nosuffix)))
    ModelTrainAndTest(allTrainedPD, allTestPD, spath=spath, selectedFeature=allfeatureload1_nosuffix,
                      modelpath="Classifiers/saved_model/tmp_load1_nosuffix")
