"""
2. 将步骤1.1中的数据进行读取之后，按照核心数进行划分
3. 并且求出滑动窗口 特征扩展以及特征条数减少
4. 将每个错误码对应的数据合并在一起
"""
import os
from typing import Dict, Tuple, Union

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames, subtractFirstLineFromDataFrame
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction

CPU_FEATURE = "cpu_affinity"

savepath1_1 = "tmp\\wrf_single_process_step10\\1.1\\"
savepath1_2 = "tmp\\wrf_single_process_step10\\1.2\\"  # 每个核的原始数据
savepath1_21 = "tmp\\wrf_single_process_step10\\1.2_特征处理\\"  # 特征提取

subtrctFeature = ["user", "system"]


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
    if not os.path.exists(savepath1_2):
        os.makedirs(savepath1_2)
    if not os.path.exists(savepath1_21):
        os.makedirs(savepath1_21)

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
    # 将allpds数据进行保存 格式为错误码: 核心数
    print("2. 将所有的数据都进行保存".center(40, "*"))
    for ifaults, idict in allpds.items():
        print("save {}: len {}".format(ifaults, len(idict)))
        tpath = os.path.join(savepath1_2, str(ifaults))
        ttpath = os.path.join(savepath1_21, str(ifaults))

        if not os.path.exists(tpath):
            os.makedirs(tpath)
        if not os.path.exists(ttpath):
            os.makedirs(ttpath)
        for i, ipd in idict.items():
            ipd: pd.DataFrame
            tfile = os.path.join(tpath, str(i) + ".csv")
            ipd.to_csv(tfile, index=False)
            # 进行特征处理
            ipd.reset_index(drop=True, inplace=True)
            ttfile = os.path.join(ttpath, str(i) + ".csv")
            tpd, err = subtractFirstLineFromDataFrame(ipd, subtrctFeature)
            if err:
                print("数据处理失败, subtractFirstLineFromDataFrame")
                exit(1)
            tpd.to_csv(ttfile, index=False)

    print("划分结束")
