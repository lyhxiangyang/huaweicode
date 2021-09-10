"""
这个文件的主要作用是主要检测grape数据
"""
import os
from collections import defaultdict
from typing import Union, Tuple

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train, getTestRealLabels
from utils.DataFrameOperation import isEmptyInDataFrame, mergeDataFrames, judgeSameFrames, divedeDataFrameByFaultFlag
from utils.DefineData import FAULT_FLAG, WINDOWS_SIZE, SaveModelPath, MODEL_TYPE
from utils.FeatureExtraction import featureExtraction
from utils.GetMetrics import get_metrics

abnormalPathes = [
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-1km-单机数据整合版\\wrf_1km_160_process.csv",
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-3km-单机数据整合版\\wrf_3km_160_process.csv",
    "D:\\HuaweiMachine\\数据修订版\\单机整合数据\\wrf-9km-单机数据整合版\\wrf_9km_160_process.csv",
]

process_features = [
    "time",
    # "pid",
    # "status",
    # "create_time",
    # "puids_real",
    # "puids_effective",
    # "puids_saved",
    # "pgids_real",
    # "pgids_effective",
    # "pgids_saved",
    "user",
    "system",
    "children_user",
    "children_system",
    "iowait",
    "cpu_affinity",  # 依照这个来为数据进行分类
    "memory_percent",
    "rss",
    "vms",
    "shared",
    "text",
    "lib",
    "data",
    "dirty",
    "read_count",
    "write_count",
    "read_bytes",
    "write_bytes",
    "read_chars",
    "write_chars",
    "num_threads",
    "voluntary",
    "involuntary",
    "faultFlag",
]

# 如果只是用某个错误里面的若干核心，就修改下面的includecores变量，比如下面错误2就只是用核心1中的数据
includecores = {
    20: [1],
    30: [0, 1, 2, 3, 4, 5, 6]
}
# 排除一些错误码的使用，也可以将abnormalPathes中的数据进行注释到达同样的效果
excludefaulty = [80]
# 使用模型的路径
savemodulepath = os.path.join(SaveModelPath, str(1))

saverespath = "tmp\\informations"
savepath = "tmp\\wrf_process_otherplatform"
isreadfile = False

CPU_FEATURE = "cpu_affinity"

def splitDFbyCore(df: pd.DataFrame) -> Union[Tuple[None, bool], Tuple[dict, bool]]:
    if CPU_FEATURE not in df.columns.array:
        return None, True
    corelist = list(set(df[CPU_FEATURE]))
    coreDict = {}
    for icore in corelist:
        tpd = df.loc[df[CPU_FEATURE] == icore].reset_index(drop=True)
        # 将CPU_FEATURE去掉
        coreDict[icore] = tpd.drop(CPU_FEATURE, axis=1)
    return coreDict, False

if __name__ == "__main__":
    # 将该有的路径都创建

    # 要预测的数据
    mergedPd = pd.read_csv(abnormalPathes[0])
    mergedPd = mergedPd[process_features]
    print("shape: {}".format(mergedPd.shape))
    ## 将错误码进行分开
    ####################################################################################################################
    print("1. 按照错误码将数据进行分割".center(40, "*"))
    tfaultDict, err = divedeDataFrameByFaultFlag1(mergedPd)
    if err:
        print("按照错误码进行数据分割失败")
        exit(1)
    # 将文件
    tpath = os.path.join(savepath, "1.错误码分割")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
    for ifault, ipd in tfaultDict:
        ipd.to_csv(os.path.join(tpath, "{}.csv".format(ifault)), index=False)

    ####################################################################################################################
    faultDict = {}
    print("2. 按照核心数将每一个数据进行分割".center(40, "*"))
    for fault, ipd in tfaultDict:
        tdict, err = splitDFbyCore(ipd)
        if err:
            print("{} 错误码按照核心分离失败".format(fault))
            exit(1)
        faultDict[fault] = tdict

    # 现在得到的是一个字典类型的数组faultDict[i][j] 代表错误码i 核心为j的数据
    # 将数据进行保存
    tpath = os.path.join(savepath, "2.按核分割")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
    for ifault, icoredict in faultDict.items():
        tfaultpath = os.path.join(tpath, str(ifault))
        if not os.path.exists(tfaultpath):
            os.makedirs(tfaultpath)
        for icore, ipd in icoredict.items():
            ipd : pd.DataFrame
            ipd.to_csv(os.path.join(tfaultpath, "{}.csv".format(icore)), index=False)

    ####################################################################################################################
    print("3. 对一些核心数进行处理".center(40, "*"))
    # 舍弃一些错误码的识别
    for i in excludefaulty:
        del faultDict[i]
    # 舍弃一些核的数据
    for fault, icoredict in faultDict:
        if fault in includecores.keys():
            for icore, ipd in icoredict.items():
                if icore not in includecores[fault]:
                    del icoredict[icore]
    # faultDict 存储的是有用的核心数据

    # 将数据进行保存
    tpath = os.path.join(savepath, "3.按核去除无用数据")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
    for ifault, icoredict in faultDict.items():
        tfaultpath = os.path.join(tpath, str(ifault))
        if not os.path.exists(tfaultpath):
            os.makedirs(tfaultpath)
        for icore, ipd in icoredict.items():
            ipd: pd.DataFrame
            ipd.to_csv(os.path.join(tfaultpath, "{}.csv".format(icore)), index=False)

    ####################################################################################################################
    allusefulpds = defaultdict(dict)
    print("4. 特征提取中".format(40, "*"))
    for ifault, idict in faultDict.items():
        for i, ipd in idict.items():
            print("before {}-{}: {}".format(ifault, i, ipd.shape))
            tpd, err = featureExtraction(ipd, windowSize=WINDOWS_SIZE)
            if err:
                print("{}-{}: {} 提取失败".format(ifault, i, ipd.shape))
                exit(1)
            allusefulpds[ifault][i] = tpd
            print("after {}-{}: {}\n".format(ifault, i, tpd.shape))

    # 将特征进行提取
    tpath = os.path.join(savepath, "4. 特征提取")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
    for ifault, icoredict in allusefulpds.items():
        tfaultpath = os.path.join(tpath, str(ifault))
        if not os.path.exists(tfaultpath):
            os.makedirs(tfaultpath)
        for icore, ipd in icoredict.items():
            ipd: pd.DataFrame
            ipd.to_csv(os.path.join(tfaultpath, "{}.csv".format(icore)), index=False)

    ####################################################################################################################
    allmergedDict = {}
    tpath = os.path.join(savepath, "4. 特征提取")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
    print("5. 将所有的数据都进行合并".center(40, "*"))
    for ifault, idict in allusefulpds.items():
        mergeDF, err = mergeDataFrames(list(idict.values()))
        if err:
            print("错误码：{} 合并失败".format(ifault))
            exit(1)
        print("错误码{}: 数据量:{}".format(ifault, mergeDF.shaoe))
        allmergedDict[ifault] = mergeDF
        # 将所有数据都保存到步骤四种
        tfaultpath = os.path.join(tpath, str(ifault))
        if not os.path.exists(tfaultpath):
            os.makedirs(tfaultpath)
        mergeDF : pd.DataFrame
        mergeDF.to_csv("merged.csv", index=False)
    # 将所有的数据合并在一起
    mergedPd, err = mergeDataFrames(list(allmergedDict.values()))
    mergedPd.to_csv("Allmerged.csv", index=False)
    if err:
        print("所有数据合并错误")
        exit(1)
    ####################################################################################################################
    print("6. 模型预测".center(40, "*"))
    reallist = mergedPd[FAULT_FLAG]
    tDic = {}
    for itype in MODEL_TYPE:
        prelist = select_and_pred(mergedPd, model_type=itype, saved_model_path=savemodulepath)
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
    if not os.path.exists(saverespath):
        os.makedirs(saverespath)
    itpd = pd.DataFrame(data=tDic).T
    print(itpd)
    itpd.to_csv(os.path.join(saverespath, "WRF_Process预测另一个平台的WRF单机.csv"))
    print("=========================")
    print("输出信息->", os.path.join(saverespath, "WRF_Process预测另一个平台的WRF单机.csv"))
    print("模型预测结束")
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################






