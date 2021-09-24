"""
将每个核心都进行读取之后
对每个数据进行特征提取，然后将1km 3km 9km的数据加到正常数据中
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
            print("{}-{}".format(ifault, icore))
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
def merge_int_int_DataFrame(faulty_core_pd_dict: Dict[int, Dict[int, pd.DataFrame]]) -> Dict[int, pd.DataFrame]:
    resDict = defaultdict(dict)
    for ifault, icoredict in faulty_core_pd_dict.items():
        ipd = mergeDataFrames(list(icoredict.values()))
        resDict[ifault] = ipd
    return resDict





if __name__ == "__main__":
    # 读取测试数据的数据
    print("测试数据_正常_异常-特征提取".center(40, "*"))
    faulty_core_pd_dict = readFaultyCoreDict(os.path.join(tmpsavepath1, "测试数据_正常_异常"))
    faulty_core_pd_dict = make_dict_featureextraction(faulty_core_pd_dict)
    # 测试数据
    saveFaultyCoreDict(os.path.join(tmpsavepath2, "测试数据_正常_异常_win3"), faulty_core_pd_dict)

    print("数据修订版正常1km-特征提取".center(40, "*"))
    dirname = "数据修订版正常1km"
    normal_1km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_1km_pd = make_dict_int_featureextraction(normal_1km_pd)
    saveFaultyDict(os.path.join(tmpsavepath2, "数据修订版正常1km_win3"), normal_1km_pd)

    dirname = "数据修订版正常3km"
    print("数据修订版正常3km-特征提取".center(40, "*"))
    normal_3km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_3km_pd = make_dict_int_featureextraction(normal_3km_pd)
    saveFaultyDict(os.path.join(tmpsavepath2, "数据修订版正常3km_win3"), normal_3km_pd)

    dirname = "数据修订版正常9km"
    print("数据修订版正常9km-特征提取".center(40, "*"))
    normal_9km_pd = readFaultyDict(os.path.join(tmpsavepath1, dirname))
    normal_9km_pd = make_dict_int_featureextraction(normal_9km_pd)
    saveFaultyDict(os.path.join(tmpsavepath2, "数据修订版正常9km_win3"), normal_9km_pd)

    # 将每个核心数据进行合并
    merge_normal_1km, err = mergeDataFrames(list(normal_1km_pd.values()))
    if err:
        print("1KM合并失败")
        exit(1)
    merge_normal_1km.to_csv(os.path.join(tmpsavepath2, "数据修订版正常1KM_win3_核数据合并.csv"))

    merge_normal_3km, err = mergeDataFrames(list(normal_3km_pd.values()))
    if err:
        print("3KM合并失败")
        exit(1)
    merge_normal_3km.to_csv(os.path.join(tmpsavepath2, "数据修订版正常3KM_win3_核数据合并.csv"))


    merge_normal_9km, err = mergeDataFrames(list(normal_9km_pd.values()))
    if err:
        print("9KM合并失败")
        exit(1)
    merge_normal_9km.to_csv(os.path.join(tmpsavepath2, "数据修订版正常9KM_win3_核数据合并.csv"))

    # 将测试数据的核心数据合并
    faulty_dict = merge_int_int_DataFrame(faulty_core_pd_dict)
    saveFaultyDict(os.path.join(tmpsavepath2, "测试数据正常_异常_核数据合并_win3"), faulty_dict)







