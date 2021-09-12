"""
这个文件的主要作用是预测另一个平台所有的wrf数据
另一个平台有三个输入数据1km、3km、9km
将会输出预测1km、3km、9km已经将其合并之后的输出数据
"""
import os
from collections import defaultdict
from typing import Union, Tuple

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train, getTestRealLabels
from utils.DataFrameOperation import isEmptyInDataFrame, mergeDataFrames, judgeSameFrames, divedeDataFrameByFaultFlag, \
    divedeDataFrameByFaultFlag1
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
    2: [1],
    3: [0, 1, 2, 3, 4, 5, 6]
}
# 排除一些错误码的使用，也可以将abnormalPathes中的数据进行注释到达同样的效果
excludefaulty = [8]
# 使用模型的路径
savemodulepath = os.path.join(SaveModelPath, str(1))

saverespathinfo = "tmp\\informations"
savepath1 = "tmp\\wrf_process_otherplatform"
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
    filenames = [
        "1km_input.csv",
        "3km_input.csv",
        "9km_input.csv",
        "1_3_9km_imput.csv"
    ]
    filedirs = [
        "1km",
        "3km",
        "9km",
        "allkm"
    ]
    for iii in range(0, 4):
        savepath = os.path.join(savepath1, filedirs[iii])
        # 要预测的数
        mergePds = [pd.read_csv(ipath) for ipath in abnormalPathes]

        if iii == 3:
            mergedPd, err = mergeDataFrames(mergePds)
        else:
            mergedPd = mergePds[iii]

        mergedPd = mergedPd[process_features]
        print("shape: {}".format(mergedPd.shape))
        ## 将错误码进行分开
        ####################################################################################################################
        allmergedDict = {}
        tpath = os.path.join(savepath, "4. 特征提取")
        tpahtfile = os.path.join(tpath, "Allmerged.csv")
        if not os.path.exists(tpahtfile):
            print("{} - {}: file not exist".format(filedirs[iii], "Allmerged.csv"))

        mergedPd = pd.read_csv()

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
        if not os.path.exists(saverespathinfo):
            os.makedirs(saverespathinfo)
        itpd = pd.DataFrame(data=tDic).T
        print(itpd)
        itpd.to_csv(os.path.join(saverespathinfo, filenames[iii]))
        print("=========================")
        print("输出信息->", os.path.join(saverespathinfo, filenames[iii]))
        print("模型预测结束")
        ####################################################################################################################
        ####################################################################################################################
        ####################################################################################################################
        ####################################################################################################################
        ####################################################################################################################






