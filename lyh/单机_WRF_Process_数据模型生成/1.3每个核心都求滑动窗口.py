import os

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction

savepath1_1 = "tmp\\wrf_single_process\\1.1\\"
savepath1_2 = "tmp\\wrf_single_process\\1.2\\" # 每个核的原始数据
savepath1_3 = "tmp\\wrf_single_process\\1.3\\" # 特征提取



if __name__ == "__main__":
    if not os.path.exists(savepath1_3):
        os.makedirs(savepath1_3)

    ####################################################################################################################
    # 先读取1.2中的生成的文件
    print("1.读取文件中...".center(40, "*"))
    allpds = {}
    for ifault in os.listdir(savepath1_2):
        tpath = os.path.join(savepath1_2, ifault)
        if ifault not in allpds.keys():
            allpds[ifault] = {}
        for icore in os.listdir(tpath):
            icorenumber = int(os.path.basename(icore)[0])
            tfilepath = os.path.join(tpath, icore)
            allpds[ifault][icorenumber] = pd.read_csv(tfilepath)

    ####################################################################################################################
    # 将特征进行提取
    allusefulpds = {}
    print("2. 将每个核心提取中".center(40, "*"))
    for ifault, idict in allpds.items():
        tpath = os.path.join(savepath1_3, str(ifault))
        if not os.path.exists(tpath):
            os.makedirs(tpath)
        if ifault not in allusefulpds.keys():
            allusefulpds[ifault] = {}
        for i, ipd in idict.items():
            print("{}-{}: {}".format(ifault, i, ipd.shape))
            tpd, err = featureExtraction(ipd, windowSize=WINDOWS_SIZE)
            if err:
                print("特征提取过程中失败")
                exit(1)
            allusefulpds[ifault][i] = tpd
            print("{}-{}: {}".format(ifault, i, tpd.shape))
            tfilepath = os.path.join(tpath, str(i) + ".csv")
            tpd.to_csv(tfilepath, index=False)

    ####################################################################################################################
    # 将数据合并得到
    allmergedDict = {}
    print("4. 将每个错误码中的数据核心数都合成起来".center(40, "*"))
    for ifault, idict in allusefulpds.items():
        mergeDF, err = mergeDataFrames(list(idict.values()))
        if err:
            print("步骤4中合并数据操作错误")
            exit(1)
        allmergedDict[ifault] = mergeDF
        # 将错误码数据合成起来之后保存
        tpath = os.path.join(savepath1_3, str(ifault))
        tfilepath = os.path.join(tpath, "userfulfeature.csv")
        mergeDF.to_csv(tfilepath, index=False)

    # 生成
    print("滑动窗口结束")









    ####################################################################################################################
    ####################################################################################################################



