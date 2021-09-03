"""
这个文件的主要作用是主要检测grape数据
"""
import os
from typing import Union, Tuple

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train, getTestRealLabels
from utils.DataFrameOperation import isEmptyInDataFrame, mergeDataFrames, judgeSameFrames
from utils.DefineData import FAULT_FLAG, WINDOWS_SIZE, SaveModelPath, MODEL_TYPE
from utils.FeatureExtraction import featureExtraction
from utils.GetMetrics import get_metrics

abnormalPathes = {
    "D:\\HuaweiMachine\\测试数据\\grapes-normal-e5\\result\\normal_single\\grapes_e5-43_process-2.csv": 0,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cacheGrabNum-1.csv": 91,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cacheGrabNum-2.csv": 92,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cacheGrabNum-3.csv": 93,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cacheGrabNum-5.csv": 94,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cacheGrabNum-10.csv": 95,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpuall-5.csv": 11,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpuall-10.csv": 12,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpuall-20.csv": 13,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpuall-50.csv": 14,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpuall-100.csv": 15,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpugrab-1.csv": 81,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpugrab-2.csv": 82,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpugrab-3.csv": 83,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpugrab-5.csv": 84,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpugrab-10.csv": 85,
    # "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpusingle-5.csv": 21, # 这个数据存在，但是在wrf模型中不存在
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpusingle-10.csv": 22,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpusingle-20.csv": 23,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpusingle-50.csv": 24,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_cpusingle-100.csv": 25,
    "D:\HuaweiMachine\测试数据\grapes单机版e5\grapes单机版e5\grapes-720\grapes_e5-43_process_cpusmulti-5.csv": 31,
    "D:\HuaweiMachine\测试数据\grapes单机版e5\grapes单机版e5\grapes-720\grapes_e5-43_process_cpusmulti-10.csv": 32,
    "D:\HuaweiMachine\测试数据\grapes单机版e5\grapes单机版e5\grapes-720\grapes_e5-43_process_cpusmulti-20.csv": 33,
    "D:\HuaweiMachine\测试数据\grapes单机版e5\grapes单机版e5\grapes-720\grapes_e5-43_process_cpusmulti-50.csv": 34,
    "D:\HuaweiMachine\测试数据\grapes单机版e5\grapes单机版e5\grapes-720\grapes_e5-43_process_cpusmulti-100.csv": 35,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_mbw_process-1.csv": 51,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_mbw_process-2.csv": 52,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_mbw_process-3.csv": 53,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_mbw_process-5.csv": 54,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_mbw_process-10.csv": 55,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_memleak-30.csv": 61,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_memleak-60.csv": 62,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_memleak-120.csv": 63,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_memleak-600.csv": 64,
    "D:\\HuaweiMachine\\测试数据\\grapes单机版e5\\grapes单机版e5\\grapes-720\\grapes_e5-43_process_memleak-1200.csv": 65,
}
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

savemodulepath = os.path.join(SaveModelPath, str(1))
saverespath = "tmp\\informations"

# 将一个DataFrame的FAULT_FLAG重值为ff
def setPDfaultFlag(df: pd.DataFrame, ff: int) -> pd.DataFrame:
    if FAULT_FLAG in df.columns.array:
        df = df.drop(FAULT_FLAG, axis=1)
    lengthpd = len(df)
    ffdict = {FAULT_FLAG: [ff] * lengthpd}
    tpd = pd.DataFrame(data=ffdict)
    tpd = pd.concat([df, tpd], axis=1)
    return tpd
CPU_FEATURE = "cpu_affinity"
def splitDFbyCore(df: pd.DataFrame) -> Union[Tuple[None, bool], Tuple[dict, bool]]:
    if CPU_FEATURE not in df.columns.array:
        return None, True
    corelist = list(set(df[CPU_FEATURE]))
    coreDict = {}
    for icore in corelist:
        tpd = df.loc[df[CPU_FEATURE] == icore]
        # 将CPU_FEATURE去掉
        coreDict[icore] = tpd.drop(CPU_FEATURE, axis=1)
    return coreDict, False

if __name__ == "__main__":
    if os.path.exists(savepath):
        os.makedirs(savepath)
    ####################################################################################################################
    print("1. 将grape数据的错误码进行处理")
    allpdlists = {}
    for ipath, iflauty in abnormalPathes.items():
        iflauty //= 10
        if iflauty not in allpdlists:
            allpdlists[iflauty] = []
        tpd = pd.read_csv(ipath)
        # 判断这个tpd是否满足不存在空值的条件
        if isEmptyInDataFrame(tpd):
            print("path: {} 存在空的情形".format(ipath))
            exit(1)
        # 将tpd中的flag设置为对应的flag
        tpd = setPDfaultFlag(tpd, iflauty)
        allpdlists[iflauty].append(tpd)
        print("文件:{}, 大小:{}".format(os.path.basename(ipath), tpd.shape))

    ####################################################################################################################
    print("2. 将相同类型的异常,强度不同的数据进行合并".center(40, "*"))
    fault_df_dict = {} # 错误码对应DataFrame
    for iflauty, ipdlist in allpdlists.items():
        tpd, err = mergeDataFrames(ipdlist)
        if err:
            print("步骤2中合并列表失败")
            exit(1)
        fault_df_dict[iflauty] = tpd
    #  合法性判断一下
    if not judgeSameFrames(list(fault_df_dict.values())):
        print("不是所有的特征都是相同的")
        exit(1)
    # 将错误码进行保存
    spath = os.path.join(savepath, "2")
    print("合并后的数据保存在-{}".format(spath))
    for iflaut, ipd in fault_df_dict.items():
        tpd = ipd[process_features]
        tpath = os.path.join(spath, str(iflaut) + ".csv")
        tpd.to_csv(tpath, index=False)
    ####################################################################################################################
    print("3. 按照核心数进行分开".format(40, "*"))
    allpds = {}
    for iflauty, tpd in fault_df_dict.items():
        if iflauty not in allpds.keys():
            allpds[iflauty] = {}
        tdict, err = splitDFbyCore(tpd)
        if err:
            print("将核心提取的过程失败")
            exit(1)
        allpds[iflauty] = tdict
        print("错误码{}: 核心数{}".format(iflauty, len(tdict)))
    # == 将数据保存到3
    spath = os.path.join(savepath, "3")
    for ifaults, idict in allpds.items():
        print("save {}: len {}".format(ifaults, len(idict)))
        tpath = os.path.join(spath, str(ifaults))
        if not os.path.exists(tpath):
            os.makedirs(tpath)
        for i, ipd in idict.items():
            ipd: pd.DataFrame
            tfile = os.path.join(tpath, str(i) + ".csv")
            ipd.to_csv(tfile, index=False)
    ####################################################################################################################
    print("4. 将每个核心都进行滑动窗口处理".center(40, "*"))
    allusefulpds = {}
    spath = os.path.join(savepath, "4")
    print("2. 将每个核心提取中".center(40, "*"))
    for ifault, idict in allpds.items():
        tpath = os.path.join(spath, str(ifault))
        if not os.path.exists(tpath):
            os.makedirs(tpath)
        if ifault not in allusefulpds.keys():
            allusefulpds[ifault] = {}
        for i, ipd in idict.items():
            print("提取前：{}-{}: {}".format(ifault, i, ipd.shape))
            tpd, err = featureExtraction(ipd, windowSize=WINDOWS_SIZE)
            if err:
                print("特征提取过程中失败")
                exit(1)
            allusefulpds[ifault][i] = tpd
            print("提取后：{}-{}: {}".format(ifault, i, tpd.shape))
            tfilepath = os.path.join(tpath, str(i) + ".csv")
            print("save to: {}".format(tpath))
            tpd.to_csv(tfilepath, index=False)
    # 将数据合并得到
    allmergedDict = {}
    userfulFeatureName = "userfulfeature.csv"
    print("4. 将每个错误码中的数据核心数都合成起来".center(40, "*"))
    for ifault, idict in allusefulpds.items():
        mergeDF, err = mergeDataFrames(list(idict.values()))
        if err:
            print("步骤4中合并数据操作错误")
            exit(1)
        allmergedDict[ifault] = mergeDF
        # 将错误码数据合成起来之后保存
        tpath = os.path.join(spath, str(ifault))
        tfilepath = os.path.join(tpath, userfulFeatureName)
        mergeDF.to_csv(tfilepath, index=False)
    # 再将所有的数据合并在一起
    allmergedpd = mergeDataFrames(list(allmergedDict.values()))

    ####################################################################################################################
    mergedPd = allmergedpd
    # 进行预测
    print("利用wrf的模型对grape进行预测".center(40, "*"))
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
            # 将数据进行保存
            tDic[i]["accuracy_" + itype] = tmetrics["accuracy"]
            tDic[i]["precision_" + itype] = tmetrics["precision"]
            tDic[i]["recall_" + itype] = tmetrics["recall"]
    if not os.path.exists(saverespath):
        os.makedirs(saverespath)
    itpd = pd.DataFrame(data=tDic).T
    print(itpd)
    itpd.to_csv(os.path.join(saverespath, "WRF_Process预测grape.csv"))
    print("=========================")
    print("输出信息->", os.path.join(saverespath, "WRF_Process预测grape.csv"))
    print("模型预测结束")
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
