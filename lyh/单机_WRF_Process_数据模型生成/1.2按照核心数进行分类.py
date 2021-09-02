"""
1. 将步骤1.1中的数据进行读取之后，按照核心数进行划分
"""
import os
from typing import Dict, Tuple, Union

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction

CPU_FEATURE = "cpu_affinity"

savepath1_1 = "tmp\\wrf_single_process\\1.1\\"
savepath1_2 = "tmp\\wrf_single_process\\1.2\\"
savepath1_3 = "tmp\\wrf_single_process\\1.3\\" # 特征提取
savepath1_4 = "tmp\\wrf_single_process\\1.4\\" # 特征提取
savepath1_5 = "tmp\\wrf_single_process\\1.5\\" # 特征提取


def splitDFbyCore(df: pd.DataFrame) -> Union[Tuple[None, bool], Tuple[dict, bool]]:
    if CPU_FEATURE not in df.columns.array:
        return None, True
    corelist = list(set(df[CPU_FEATURE]))
    coreDict = {}
    for icore in corelist:
        tpd = df.loc[df[CPU_FEATURE] == icore]
        # 将CPU_FEATURE去掉
        coreDict[icore] = tpd.drop(CPU_FEATURE)
    return coreDict, False



if __name__ == "__main__":
    if not os.path.exists(savepath1_2):
        os.makedirs(savepath1_2)
    if not os.path.exists(savepath1_3):
        os.makedirs(savepath1_3)
    if not os.path.exists(savepath1_4):
        os.makedirs(savepath1_4)
    if not os.path.exists(savepath1_5):
        os.makedirs(savepath1_5)
    ####################################################################################################################
    # 先将一中的数据进行读取 并且按照核数据提取
    print("1. 按照核心进行数据提取".center(40, "*"))
    lfiles = os.listdir(savepath1_1)
    allpds = {}
    for ifile in lfiles:
        ifault = int(os.path.splitext(ifile)[0])
        tpath = os.path.join(savepath1_1, ifile)
        if ifault not in allpds.keys():
            allpds[ifault] = {}
        tpd = pd.read_csv(tpath)
        tdict, err = splitDFbyCore(tpd)
        if err:
            print("将核心提取的过程失败")
            exit(1)
        allpds[ifault] = tdict
        print("错误码{}  核心数{}".format(ifault, len(tdict)))

    ####################################################################################################################
    # 将allpds数据进行保存
    print("2. 将所有的数据都进行保存".center(40, "*"))
    for ifaults, idict in allpds.items():
        print("save {}: len {}".format(ifaults, len(idict)))
        tpath = os.path.join(savepath1_2, str(ifaults))
        if not os.path.exists(tpath):
            os.makedirs(tpath)
        for i, ipd in idict.items():
            ipd: pd.DataFrame
            tfile = os.path.join(tpath, str(i) + ".csv")
            ipd.to_csv(tfile, index=False)

    ####################################################################################################################
    # 将每个核心树求滑动窗口
    print("3. 将每个核心都进行特征提取".center(40, "*"))
    for ifaults, idict in allpds.items():
        tpath = os.path.join(savepath1_3, str(ifaults))
        if not os.path.exists(tpath):
            os.makedirs(tpath)

        for i, ipd in idict.items():
            tpd , err = featureExtraction(ipd, windowSize=WINDOWS_SIZE)
            if err:
                print("特征提取过程中失败")
                exit(1)
            allpds[ifaults][i] = tpd
            print("{}-{}: {}".format(ifaults, i, tpd.shape))
            tfilepath = os.path.join(tpath, str(i) + ".csv")
            tpd.to_csv(tfilepath, index=False)

    ####################################################################################################################
    print("4. 将每个错误码中的数据核心数都合成起来".center(40, "*"))
    allmergedDict = {}
    for ifaults, idict in allpds.items():
        mergeDF, err = mergeDataFrames(list(idict.values()))
        if err:
            print("步骤4中合并数据操作错误")
        allmergedDict[ifaults] = mergeDF
        #  将错误码数据合成起来之后保存
        tfilepath = os.path.join(savepath1_4, str(ifaults) + ".csv")
        mergeDF.to_csv(tfilepath, index=False)

    ####################################################################################################################
    print("5: 将全部错误码进行合并之后进行保存")
    allUserfulPd, err = mergeDataFrames(list(allmergedDict.values()))
    if err:
        print("第5步骤中全部错误码合并之后出错")
        exit(1)
    tfilepath = os.path.join(savepath1_5, "userFeature.csv")
    allUserfulPd.to_csv(tfilepath, index=False)
    ####################################################################################################################
