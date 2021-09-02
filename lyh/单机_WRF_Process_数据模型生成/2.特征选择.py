"""
要进行特征选择
如：将错误码1和错误码0中的特征进行选择
做法：将1.3中目录都是各个错误， 各个错误下面有的是userfulfeature.csv文件
"""

import os
from typing import Dict, Tuple, Union

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames, judgeSameFrames
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction
from utils.FeatureSelection import getUsefulFeatureFromAllDataFrames

savepath1_1 = "tmp\\wrf_single_process\\1.1\\"
savepath1_2 = "tmp\\wrf_single_process\\1.2\\" # 每个核的原始数据
savepath1_3 = "tmp\\wrf_single_process\\1.3\\" # 特征提取
savepath2 = "tmp\\wrf_single_process\\2\\" # 特征提取
userfulFeatureName = "userfulfeature.csv"


if __name__ == "__main__":
    if not os.path.exists(savepath2):
        os.makedirs(savepath2)
    ####################################################################################################################
    # 先读取每个目录下的文件
    dictPds = {}
    for ifaulty in os.listdir(savepath1_3):
        tpath = os.path.join(savepath1_3, ifaulty)
        ifaultynumber = int(os.path.splitext(ifaulty)[0])
        tusefulfeature = os.path.join(tpath, userfulFeatureName)
        dictPds[ifaultynumber] = pd.read_csv(tusefulfeature)

    if not judgeSameFrames(list(dictPds.values())):
        print("读取的文件中特征值不一样")
        exit(1)

    ####################################################################################################################
    if 0 not in dictPds.keys():
        print("错误码0 没有存在")
        exit(1)

# ==进行特征选择
    normalPD = dictPds[0]
    abnormalPD = [dictPds[i] for i in dictPds.keys() if i != 0]

    print("特征选择前一共{}个特征".format(normalPD.shape[1]))
    allUserfulePD, err = getUsefulFeatureFromAllDataFrames(normalpd=normalPD, abnormalpd=abnormalPD)

    if err:
        print("特征选择失败")
        exit(1)
    print("特征选择后一共{}个特征".format(allUserfulePD.shape[1]))

    if err:
        print("特征选择失败")
        exit(1)
    print("特征选择后一共{}个特征".format(allUserfulePD.shape[1]))
    tpath = os.path.join(savepath2, "alluserful.csv")
    allUserfulePD.to_csv(tpath, index=False)

    print("特征选择结束".center(40, "*"))


































































































