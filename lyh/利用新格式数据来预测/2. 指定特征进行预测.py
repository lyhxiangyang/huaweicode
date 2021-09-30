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
from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import MODEL_TYPE, FAULT_FLAG
from utils.GetMetrics import get_metrics

trainedFeatures = [

]

trainDataPath = [
    "tmp/tData/多机-E5-process-server-1KM-win5/所有process错误码信息/0.csv",
    "tmp/tData/多机-E5-process-server-1KM-win5/所有process错误码信息/15.csv",
    "tmp/tData/多机-红区-process-server-1KM-win5/所有process错误码信息/0.csv",
]
testDataPath = [
    "tmp/tData/多机-红区-process-server-1KM-win5/所有process错误码信息/0.csv",
    "tmp/tData/多机-红区-process-server-1KM-win5/所有process错误码信息/15.csv",
]


def get_List_pre_suffix(clist: List[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [i for i in clist if i.startswith(prefix) and i.endswith(suffix)]


# 使用三种模型进行预测信息
def TrainThree(trainedpd: pd.DataFrame, spath: str, modelpath: str = "Classifiers/saved_model/tmp",
               selectedFeature: List[str] = None):
    if not os.path.exists(spath):
        os.makedirs(spath)
    if not os.path.exists(modelpath):
        os.makedirs(modelpath)
    tDic = {}
    for itype in MODEL_TYPE:
        accuracy = model_train(trainedpd, itype, saved_model_path=modelpath, trainedFeature=selectedFeature)
        print('Accuracy of %s classifier: %f' % (itype, accuracy))
        tallFault = sorted(list(set(getTestRealLabels())))
        for i in tallFault:
            if i not in tDic.keys():
                tDic[i] = {}
            tmetrics = get_metrics(getTestRealLabels(), getTestPreLabels(), label=i)

            tDic[i]["accuracy_" + itype] = tmetrics["accuracy"]
            tDic[i]["precision_" + itype] = tmetrics["precision"]
            tDic[i]["recall_" + itype] = tmetrics["recall"]

    itpd = pd.DataFrame(data=tDic).T
    savefilename = "1.模型训练过程中数据统计.csv"
    itpd.to_csv(os.path.join(spath, savefilename))


def testThree(testpd: pd.DataFrame, spath: str, modelpath: str = "Classifiers/saved_model/tmp"):
    if not os.path.exists(spath):
        os.makedirs(spath)
    if not os.path.exists(modelpath):
        os.makedirs(modelpath)
    tDic = {}
    reallist = testpd[FAULT_FLAG]
    for itype in MODEL_TYPE:
        prelist = select_and_pred(testpd, model_type=itype, saved_model_path=modelpath)
        anumber = len(prelist)
        rightnumber = len([i for i in range(0, len(prelist)) if prelist[i] == reallist[i]])
        print("{}: 一共预测{}数据，其中预测正确{}数量, 正确率{}".format(itype, anumber, rightnumber, rightnumber / anumber))
        tallFault = sorted(list(set(reallist)))
        for i in tallFault:
            if i not in tDic.keys():
                tDic[i] = {}
            tmetrics = get_metrics(reallist, prelist, i)
            if "num" not in tDic[i].keys():
                tDic[i]["num"] = tmetrics["realnums"][i]
            # 将数据进行保存
            tDic[i]["accuracy_" + itype] = tmetrics["accuracy"]
            tDic[i]["precision_" + itype] = tmetrics["precision"]
            tDic[i]["recall_" + itype] = tmetrics["recall"]
    itpd = pd.DataFrame(data=tDic).T
    savefilename = "1.预测数据信息统计.csv"
    itpd.to_csv(os.path.join(spath, savefilename))
    print("预测信息结束")


"""
中间文件都放在spath中
"""


def ModelTrainAndTest(trainedpd: pd.DataFrame, testpd: pd.DataFrame, spath: str,
                      modelpath: str = "Classifiers/saved_model/tmp", trainAgain: bool = True,
                      selectedFeature: List[str] = None):
    # 先生成模型 得到生成模型的准确率
    if not os.path.exists(spath):
        os.makedirs(spath)
    if not os.path.exists(modelpath):
        os.makedirs(modelpath)
    if trainAgain:
        TrainThree(trainedpd, spath, modelpath, selectedFeature=selectedFeature)

    print("模型训练完成".center(40, "*"))
    print("开始对测试数据进行预测".center(40, "*"))
    testThree(testpd, spath, modelpath)


if __name__ == "__main__":
    spath = "tmp/E5多机预测红区-单特征load1"
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
    allfeatureload1_pre = get_List_pre_suffix(list(allTrainedPD.columns.array), prefix="load1_")
    allfeatureload1_pre_suffix = get_List_pre_suffix(list(allTrainedPD.columns.array), prefix="load1_", suffix="_diff")

    ModelTrainAndTest(allTrainedPD, allTestPD, spath="tmp/E5多机预测红区-单特征load1_pre", selectedFeature=allfeatureload1_pre, modelpath="Classifiers/saved_model/tmp_load1_pre")
    ModelTrainAndTest(allTrainedPD, allTestPD, spath="tmp/E5多机预测红区-单特征load1_pre_suffix",
                      selectedFeature=allfeatureload1_pre_suffix, modelpath="Classifiers/saved_model/tmp_load1_pre_suffix")
