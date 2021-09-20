"""
将每个核心都进行读取之后
"""
import os.path
from collections import defaultdict
from typing import Dict

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames
from utils.FeatureExtraction import featureExtraction
from utils.FileSaveRead import readFaultyCoreDict, readFaultyDict, saveFaultyCoreDict, saveFaultyDict

datasavepath = "tmp/Data"
tmpsavepath = "tmp/11.滑动窗口2多机模型预测"
tmpsavepath1 = "tmp/11.滑动窗口2多机模型预测/1.训练数据处理部分"
tmpsavepath2 = "tmp/11.滑动窗口2多机模型预测/2.特征提取"
WINSIZE = 3

if not os.path.exists(tmpsavepath2):
    os.makedirs(tmpsavepath2)

"""
将int-int-pd的数据格式进行特征提取
返回值为int-int-pd格式
"""
def make_dict_featureextraction(faulty_core_pd_dict: Dict[int, Dict[int, pd.DataFrame]]):
    resDict = defaultdict(dict)
    for ifault,icoredict in faulty_core_pd_dict.items():
        for icore, ipd in icoredict.items():
            tpd, err = featureExtraction(ipd, windowSize=WINSIZE)
            if err:
                print("特征提取失败")
                exit(1)
            resDict[ifault][icore] = tpd
    return resDict

# 提取的是int-pd的格式的数据
def make_dict_int_featureextraction(fault_pd_dict: Dict[int, pd.DataFrame]):
    resDict = {}
    for icore, ipd in fault_pd_dict.items():
        tpd, err = featureExtraction(ipd, windowSize=WINSIZE)
        if err:
            print("特征提取失败")
            exit(1)
        resDict[icore] = tpd
    return resDict

# 将字典int-int-DataFrame格式中的DataFrame合并
# 返回值为int-DataFrame类型
# 主要是将所有的核合并
def merge_int_int_DataFrame(faulty_core_pd_dict: Dict[int, Dict[int, pd.DataFrame]]):
    resDict = defaultdict(dict)
    for ifault, icoredict in faulty_core_pd_dict.items():
        ipd = mergeDataFrames(list(icoredict.values()))
        resDict[ifault] = ipd
    return resDict





if __name__ == "__main__":
    # 读取测试数据的数据
    faulty_core_pd_dict = readFaultyCoreDict(os.path.join(tmpsavepath1, "测试数据_正常_异常"))
    faulty_core_pd_dict = make_dict_featureextraction(faulty_core_pd_dict)
    # 测试数据
    saveFaultyCoreDict(os.path.join(tmpsavepath2, "测试数据_正常_异常"), faulty_core_pd_dict)

    dirname = "数据修订版正常1km"
    normal_1km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_1km_pd = make_dict_int_featureextraction(normal_1km_pd)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常1km"), normal_1km_pd)

    dirname = "数据修订版正常3km"
    normal_3km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_3km_pd = make_dict_int_featureextraction(normal_1km_pd)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常3km"), normal_3km_pd)

    dirname = "数据修订版正常9km"
    normal_9km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_9km_pd = make_dict_int_featureextraction(normal_1km_pd)
    saveFaultyDict(os.path.join(tmpsavepath1, "数据修订版正常9km"), normal_9km_pd)




