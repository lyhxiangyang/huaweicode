"""
这个文件的主要目的是对E5环境下的全CPU抢占场景进行预测结果的分析
"""
import os
from typing import List, Any, Union

import pandas as pd
from pandas import DataFrame, Series
from pandas.io.parsers import TextFileReader

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train, getTestRealLabels, getTestPreLabels
from Classifiers.TrainToTest import ModelTrainAndTest
from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import MODEL_TYPE, FAULT_FLAG
from utils.GetMetrics import get_metrics

trainedFeatures = [

]

trainDataPath = [
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据/0.csv",
    "tmp/tData_10_9/多机-E5-process-server-1KM-win3-step1/原始数据-标准化数据/15.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据/0.csv"
]
testDataPath = [
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据/0.csv",
    "tmp/tData_10_9/多机-红区-process-server-1KM-win3-step1/原始数据-标准化数据/15.csv"
]


def get_List_pre_suffix(clist: List[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [i for i in clist if i.startswith(prefix) and i.endswith(suffix)]



# 指定特征，使用标准化的原始数据来进行模型的预测
if __name__ == "__main__":
    spath = "tmp/E5多机预测红区-单特征load1"
    modelsavepath = "Classifiers/saved_model/tmp_load1_pre"

    trainedPDList: list[Union[Union[TextFileReader, Series, DataFrame, None], Any]] = []
    for i in trainDataPath:
        tpd = pd.read_csv(i)
        trainedPDList.append(tpd)
    allTrainedPD, err = mergeDataFrames(trainedPDList)
    allTrainedPD: pd.DataFrame
    if err:
        print("合并出错")
        exit(1)

    testPDList = []
    for i in testDataPath:
        tpd = pd.read_csv(i)
        testPDList.append(tpd)
    allTestPD, err = mergeDataFrames(testPDList)

    # 获得需要训练的特征
    ModelTrainAndTest(allTrainedPD, allTestPD, spath=spath, selectedFeature=["load1"], modelpath=modelsavepath)
