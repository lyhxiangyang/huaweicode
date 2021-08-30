"""
这个文件的主要作用是指定新的文件，然后进程步骤1，2，3，4，5
使用的模型是单机版训练出来的模型，
输入： 指定文件
输出：模型文件
"""
import os
from typing import Dict

from Classifiers.ModelPred import select_and_pred
from utils.DefineData import SaveModelPath, FAULT_FLAG
import pandas as pd
import numpy as np

from Classifiers.ModelTrain import model_train
from utils.DataFrameOperation import mergeDataFrames, divedeDataFrameByFaultFlag, isEmptyInDataFrame
from utils.DefineData import WINDOWS_SIZE, MODEL_TYPE
from utils.FeatureExtraction import featureExtraction
from utils.FeatureSelection import getUsefulFeatureFromAllDataFrames
from utils.GetMetrics import get_metrics

"""
使用测试数据中的单机server来预测《数据修订版》中的单机数据
"""

modelpath = os.path.join(SaveModelPath, "1")

AllCSVFiles = [
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-1km-多机数据整合版\\wrf_1km_160_server.csv",
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-1km-多机数据整合版\\wrf_1km_191_server.csv",
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-3km-多机数据整合版\\wrf_3km_160_server.csv",
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-3km-多机数据整合版\\wrf_3km_191_server.csv",
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-9km-多机数据整合版\\wrf_9km_160_server.csv",
    "D:\\HuaweiMachine\\数据修订版\\多机整合数据\\wrf-9km-多机数据整合版\\wrf_9km_191_server.csv"
]
saverespath = "tmp\\informations"

if __name__ == "__main__":
    ####################################################################################################################
    print("1. 数据合并进行中".center(40, "*"))
    allPds = [pd.read_csv(ipath) for ipath in AllCSVFiles]

    # 进行判空操作
    for ipd in allPds:
        if isEmptyInDataFrame(ipd):
            print("数据有空")
            exit(1)

    # 合并操作
    mergedPd, err = mergeDataFrames(allPds)
    if err:
        print("数据合并失败")
        exit(1)
    if isEmptyInDataFrame(mergedPd):
        print("合并之后的DataFrame有NAN")
        exit(1)

    ####################################################################################################################
    print("2. 数据分割中".center(40, "*"))
    dictPds: Dict
    dictPds, err = divedeDataFrameByFaultFlag(mergedPd)
    if err:
        print("数据分割失败")
        exit(1)
    for i in dictPds.keys():
        print(i, dictPds[i].shape)
        if isEmptyInDataFrame(dictPds[i]):
            print("错误码{}数据有空".format(i))
            exit(1)

    ####################################################################################################################
    print("3. 特征提取中".center(40, "*"))
    normalPD, err = featureExtraction(dictPds[0], windowSize=WINDOWS_SIZE)
    if err:
        print("特征提取失败")
        exit(1)

    abnormalPD = []
    # 为了选择模型中存在的flag
    tpath = "tmp\\single\\4\\alluserful.csv"
    tpd = pd.read_csv(tpath)
    ts = set(tpd[FAULT_FLAG])

    print("错误码  数据条数".center(20, "="))
    for i in dictPds.keys():
        print("{}: {}".format(i, dictPds[i].shape))
        if i == 0 or i not in ts:
            continue
        tpd, err = featureExtraction(dictPds[i], windowSize=WINDOWS_SIZE)
        print("{}: {}".format(i, tpd.shape))
        if err:
            print("abnormal 特征提取失败")
            exit(1)
        if isEmptyInDataFrame(tpd):
            print("错误码{}: 数据有空".format(i))
            exit(1)
        abnormalPD.append(tpd)
    print("错误码  数据条数 结束".center(20, "="))

    ####################################################################################################################
    print("4. 将数据进行合并")
    dfs = [normalPD]
    dfs.extend(abnormalPD)
    mergedPd, err = mergeDataFrames(dfs)
    mergedPd: pd.DataFrame
    if err:
        print("步骤4中数据合并失败")
        exit(1)
    # 将mergedPd进行输出
    mergedPd.to_csv("tmp\\1.input.csv", index=False)

    ####################################################################################################################
    print("调用1中的模型进行预测".center(40, "*"))
    reallist = mergedPd[FAULT_FLAG]
    #  包含准确率、召回率、精确率以及对应模型的的字典
    tDic = {}
    for itype in MODEL_TYPE:
        prelist = select_and_pred(mergedPd, model_type=itype, saved_model_path=modelpath)
        anumber = len(prelist)
        rightnumber = len([i for i in range(0, len(prelist)) if prelist[i] == reallist[i]])
        print("{}: 一共预测{}数据，其中预测正确{}数量, 正确率{}".format(itype, anumber, rightnumber, rightnumber / anumber))
        tallFault = sorted(list(set(reallist)))
        for i in tallFault:
            if i not in tDic.keys():
                tDic[i] = {}
            tmetrics = get_metrics(reallist, prelist, i)
            # 将数据进行保存
            tDic[i]["accuracy_" + itype] = tmetrics["accuracy"]
            tDic[i]["precision_" + itype] = tmetrics["precision"]
            tDic[i]["recall_" + itype] = tmetrics["recall"]

    # 将数据进行输出并保存
    if not os.path.exists(saverespath):
        os.makedirs(saverespath)
    itpd = pd.DataFrame(data=tDic).T
    print(itpd)
    itpd.to_csv(os.path.join(saverespath, "单机模型预测多机数据统计信息.csv"))
    print("=========================")
    print("输出信息->", os.path.join(saverespath, "单机模型预测多机数据统计信息.csv"))
    print("模型预测结束")
