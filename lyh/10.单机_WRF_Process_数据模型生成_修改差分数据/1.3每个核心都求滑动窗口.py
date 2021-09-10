import os
from collections import defaultdict

import pandas as pd

from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import WINDOWS_SIZE
from utils.FeatureExtraction import featureExtraction

savepath1_1 = "tmp\\wrf_single_process_step10\\1.1\\"
savepath1_2 = "tmp\\wrf_single_process_step10\\1.2_特征处理_减去前一行\\" # 每个核的原始数据
savepath1_31 = "tmp\\wrf_single_process_step10\\1.3_合并\\" # 特征提取
savepath1_3 = "tmp\\wrf_single_process_step10\\1.3\\" # 特征提取



if __name__ == "__main__":
    if not os.path.exists(savepath1_3):
        os.makedirs(savepath1_3)
    if not os.path.exists(savepath1_31):
        os.makedirs(savepath1_31)

    ####################################################################################################################
    #  先读取1.2中生成的文件，将相同错误的的相同核心数的 并合并
    allpdslist = {}
    for ifault in os.listdir(savepath1_2):
        tfault = int(ifault) // 10
        tpath = os.path.join(savepath1_2, ifault)
        if tfault not in allpdslist.keys():
            allpdslist[tfault] = defaultdict(list)
        for icore in os.listdir(tpath):
            icorenumber = int(os.path.splitext(icore)[0])
            tfilepath = os.path.join(tpath, icore)
            tpd = pd.read_csv(tfilepath)
            allpdslist[tfault][icorenumber].append(tpd)

    # 进行合并
    allpds = {}
    for ifault, ifaultdict in allpdslist.items():
        if ifault not in allpds.keys():
            allpds[ifault] = {}
        for icore, corepdlist in ifaultdict.items():
            allpds[ifault][icore], err = mergeDataFrames(allpdslist[ifault][icore])
            if err:
                print("{}-{}合并错误".format(ifault, icore))
                exit(1)

    ####################################################################################################################
    ## 将生成的文件进行保存
    for ifault, ifaultdict in allpds.items():
        tpath = os.path.join(savepath1_31, str(ifault))
        if not os.path.exists(tpath):
            os.makedirs(tpath)
        for icore, corepd in ifaultdict.items():
            corepd : pd.DataFrame
            tpathfile = os.path.join(tpath, str(icore) + ".csv")
            corepd.to_csv(tpathfile, index=False)

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



