"""
1. 能够进行特征的选择
"""

import os
from collections import defaultdict
from typing import Dict, Tuple, Union, Any, List

import pandas as pd

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
    # "children_user",
    # "children_system",
    "iowait",
    # "cpu_affinity",  # 依照这个来为数据进行分类
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

"""
-   文件目的：从路径下读取对应的核信息，
readpath： 读取的路径
excludecore: 要排除读取信息的核
"""


def readCoresPD(readpath: str, excludecore=None, select_feature: List[str] = None) -> Union[
    Tuple[None, bool], Tuple[dict[int, Any], bool]]:
    if excludecore is None:
        excludecore = []
    if not os.path.exists(readpath):
        return None, True
    core_pd_Dict = {}
    for score in os.listdir(readpath):
        icore = int(os.path.splitext(score)[0])
        if icore in excludecore:
            continue
        tpathfile = os.path.join(readpath, score)
        tpd = pd.read_csv(tpathfile)
        if select_feature is not None:
            tpd = tpd.loc[select_feature]
        core_pd_Dict[icore] = tpd
    return core_pd_Dict, False

# 将readpath路径下的所有错误码进行读取，排除excludeFaulty，readDir是每个错误码下的读取目录
# 返回一个字典 faulty-core-DataFrame
def readFaultyPD(readpath: str, readDir : str, excludeFaulty=None) -> Union[
    Tuple[None, bool], Tuple[defaultdict[Any, Dict], bool]]:
    if excludeFaulty is None:
        excludeFaulty = []
    if not os.path.exists(readpath):
        return None, True
    faultDict = defaultdict(dict)
    for strFaulty in os.listdir(readpath):
        ifaulty = int(str.split(strFaulty, "_")[1])
        if ifaulty in excludeFaulty:
            continue
        tpath = os.path.join(readpath, strFaulty, readDir)
        if not os.path.exists(tpath):
            print("{} 不存在".format(tpath))
            continue
        tdict, err = readCoresPD(tpath)
        if err:
            print("{}核心读取失败".format(ifaulty))
            exit(1)
        faultDict[ifaulty] = tdict
    return faultDict, False

# 将int - DataFrame这种格式的字典保存
# 将会在savepath目录下生成一系列的 0.csv的文件
def saveFaultyDict(savepath: str, faultydict: Dict[int, pd.DataFrame]):
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    for ifaulty, ifaultypd in  faultydict.items():
        tfilepath = os.path.join(savepath, str(ifaulty) + ".csv")
        ifaultypd.to_csv(tfilepath, index=False)
def readFaultyDict(savepath: str):
    faultydict = {}
    files = os.listdir(savepath)
    for ifile in files:
        tfile = int(os.path.splitext(ifile)[0])
        tpathfile = os.path.join(savepath, ifile)
        tpd = pd.read_csv(tpathfile)
        faultydict[tfile] = tpd
    return faultydict

# 将int-int-DataFrame这种格式的字典进行保存
def saveFaultyCoreDict(savepath: str, faulty_core_dict: Dict[int, Dict[int, pd.DataFrame]]):
    if not os.path.exists(savepath):
        os.makedirs(savepath)
    for ifault, icoredict in faulty_core_dict.items():
        tfaultpath = os.path.join(savepath, str(ifault))
        if not os.path.exists(tfaultpath):
            os.makedirs(tfaultpath)
        for icore, ipd in icoredict.items():
            tfaultcorefile = os.path.join(tfaultpath, str(icore) + ".csv")
            ipd.to_csv(tfaultcorefile, index=False)

def readFaultyCoreDict(savepath: str) -> Dict[int, Dict[int, pd.DataFrame]]:
    faultydirs = os.listdir(savepath)
    faulty_core_dict = defaultdict(dict)
    for sfault in faultydirs:
        ifault = int(sfault)
        tfaultpath = os.path.join(savepath, sfault)
        corefiles = os.listdir(tfaultpath)
        for scorefile in corefiles:
            icorename = int(os.path.splitext(scorefile)[0])
            tfaultcorefile = os.path.join(tfaultpath, scorefile)
            faulty_core_dict[ifault][icorename] = pd.read_csv(tfaultcorefile)
    return faulty_core_dict







if __name__ == "__main__":
    datasavepath = "tmp/Data"
    tmpsavepath = "tmp/11.滑动窗口2多机模型预测"
    tmpsavepath1 = "tmp/11.滑动窗口2多机模型预测/1.训练数据处理部分"
    if not os.path.exists(tmpsavepath1):
        os.makedirs(tmpsavepath1)


    # 读取所有的数据
    faulty_core_pd_dict, err = readFaultyPD(datasavepath, readDir="测试数据_多机_process1")

    # 测试数据
    saveFaultyCoreDict(os.path.join(tmpsavepath1, "测试数据_正常_异常"), faulty_core_pd_dict)

    # 数据修订版1KM正常
    datadir = "数据修订版_多机_1KM_process"
    normal_1km, err = readCoresPD(readpath=os.path.join(datasavepath, datadir), select_feature=process_features)
    if err:
        print("数据修订版 1KM 读取错误")
        exit(1)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常1km"), normal_1km)

    # 数据修订版3KM正常
    datadir = "数据修订版_多机_3KM_process"
    normal_3km, err = readCoresPD(readpath=os.path.join(datasavepath, datadir), select_feature=process_features)
    if err:
        print("数据修订版 3KM 读取错误")
        exit(1)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常3km"), normal_1km)

    # 数据修订版9KM正常
    datadir = "数据修订版_多机_9KM_process"
    if err:
        print("数据修订版 9KM 读取错误")
        exit(1)
    normal_9km, err = readCoresPD(readpath=os.path.join(datasavepath, datadir), select_feature=process_features)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常9km"), normal_1km)








