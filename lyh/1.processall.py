"""
本意是指定所有的输入 直接得到模型与所有的输出
所谓的输入是指得含有异常和正常错误码的CSV文件
输出是指模型预测的准确率
"""
import pandas as pd
import numpy as np

from Classifiers.ModelTrain import model_train
from utils.DataFrameOperation import mergeDataFrames, divedeDataFrameByFaultFlag
from utils.DefineData import WINDOWS_SIZE, MODEL_TYPE
from utils.FeatureExtraction import featureExtraction
from utils.FeatureSelection import getUsefulFeatureFromAllDataFrames

AllCSVFiles = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst_normal_e5\\result\\normal_single\\wrfrst_e5=43_server.csv"
    ""
    "",
    ""
]


if __name__ == "__main__":
    # 1. 将所有的数据进行合并
    allPds = [pd.read_csv(ipath) for ipath in AllCSVFiles]
    mergedPd, err = mergeDataFrames(allPds)
    if err:
        print("数据合并失败")
        exit(1)

    # 2. 根据错误码进行分割得到一个错误码: dataFrame的结构字典结构
    dictPds, err = divedeDataFrameByFaultFlag(mergedPd)
    if err:
        print("数据分割失败")
        exit(1)
    # 显示一些基本信息
    for i in dictPds:
        print(i, dictPds[i].shape)

    # 3. 得到包含只包含某些特征错误码的表格 并且特征提取
    normalPD, err = featureExtraction(dictPds[0], windowSize=WINDOWS_SIZE)
    if err:
        print("特征提取失败")
        exit(1)
    abnormalPD = []
    for i in dictPds.keys():
        if i == 0:
            continue
        tpd, err = featureExtraction(dictPds[i], windowSize=WINDOWS_SIZE)
        if err:
            print("abnormal 特征提取失败")
            exit(1)
        abnormalPD.append(tpd)

    print("normal: ", normalPD.shape)
    for ipd in abnormalPD:
        print(ipd.shape)


    # 4. 得到一个经过特征选择之后的包含总体的表格
    allUserfulePD, err = getUsefulFeatureFromAllDataFrames(normalpd=normalPD, abnormalpd=abnormalPD)
    if err:
        print("特征选择失败")
        exit(1)
    print("特征选择之后shape：", allUserfulePD.shape)

    # 5. 模型训练
    for itype in MODEL_TYPE:
        accuracy = model_train(allUserfulePD, itype)
        print('Accuracy of %s classifier: %f' % (itype, accuracy))








