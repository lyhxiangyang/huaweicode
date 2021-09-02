"""
这个文件的主要作用是用来将阶段3生成的所有文件 选择有用的特征 生成一个总的文件
"""

import os
from typing import Dict

import pandas as pd

from utils.DataFrameOperation import isEmptyInDataFrame
from utils.FeatureSelection import getUsefulFeatureFromAllDataFrames

savepath3 = "tmp\\single\\3\\"
savepath4 = "tmp\\single\\4\\"

if __name__ == "__main__":

    print("特征选择".center(40, "*"))

    if not os.path.exists(savepath4):
        os.makedirs(savepath4)

# == 读取步骤2中产生的所有列表 产生一个dictPds
    lfiles = os.listdir(savepath3)
    dictPds: Dict = {}
    for i in lfiles:
        readfile = os.path.join(savepath3, i)
        tPd = pd.read_csv(readfile)
        # 判断是否有空值
        if isEmptyInDataFrame(tPd):
            print("{} have NAN".format(readfile))
            exit(1)
        # 获得文件名字对应的数字
        i = int(i.split(sep=".")[0])
        dictPds[i] = tPd

    if 0 not in dictPds.keys():
        print("错误码0 没有存在")
        exit(1)

# == 进行特征的选择 生成一个新的包含总的特征的DataFrame
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
    tpath = os.path.join(savepath4, "alluserful.csv")
    allUserfulePD.to_csv(tpath, index=False)

    print("特征选择结束".center(40, "*"))



















