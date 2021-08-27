"""
此文件的主要作用是根据CSV文件来判断每个faultFlag预测的成功度
"""

import os
from typing import List

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from utils.DefineData import FAULT_FLAG, MODEL_TYPE, SaveModelPath

# 包含各种特征的合并文件
from utils.GetMetrics import get_metrics

prefilepath = "tmp\\single\\4\\alluserful.csv"
# 模型文件
modelpath = os.path.join(SaveModelPath, "1")

def getComparePD(reallist: List, prelist: List) -> pd.DataFrame:
    resDict = {}
    allnumber = "allnumber"
    rightnumber = "rightnumber"
    wrongnumber = "wrongnumber"
    for i, realflag in enumerate(reallist):
        if realflag not in resDict.keys():
            resDict[realflag] = {allnumber: 0, rightnumber: 0, wrongnumber: 0}
        resDict[realflag][allnumber] += 1
        if reallist[i] == prelist[i]:
            resDict[realflag][rightnumber] += 1
        else:
            resDict[realflag][wrongnumber] += 1
    return pd.DataFrame(resDict).T

if __name__ == "__main__":
    predpd = pd.read_csv(prefilepath)
    reallist = predpd[FAULT_FLAG]

    for itype in MODEL_TYPE:
        prelist = select_and_pred(predpd, model_type=itype, saved_model_path=modelpath)
        anumber = len(prelist)
        rightnumber = len([i for i in range(0, len(prelist)) if prelist[i] == reallist[i]])
        print("{}: 一共预测{}数据，其中预测正确{}数量, 正确率{}".format(itype, anumber, rightnumber, rightnumber / anumber))
        medic = get_metrics(reallist, prelist, 0)
        print(medic)
        tpd = getComparePD(reallist=reallist, prelist=prelist)
        # 生成准确的模型统计信息
        tpd.to_csv(os.path.join("tmp", itype+".csv"))

    
