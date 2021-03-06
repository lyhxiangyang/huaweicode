"""
这个文件的作用主要是用于模型的训练
"""

import os

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train, getTestRealLabels, getTestPreLabels
from utils.DefineData import MODEL_TYPE, SaveModelPath
from utils.GetMetrics import get_metrics

savepath2 = "tmp\\wrf_single_process_step10\\2\\" # 特征提
filename2 = "alluserful.csv"
savepath3 = "tmp\\wrf_single_process_step10\\5\\"

saverespath = "tmp\\informations"

if __name__ == "__main__":
    if not os.path.exists(SaveModelPath):
        os.makedirs(SaveModelPath)
    print("模型训练".center(40, "*"))

    # == 读取步骤四种生成的文件
    tfile = os.path.join(savepath2, filename2)
    allUserfulPD = pd.read_csv(tfile)

    # == 进行模型的训练
    # 获得模型保存的路径
    inum = len(os.listdir(SaveModelPath)) + 1
    apath = os.path.join(SaveModelPath, str(inum))

    tDic = {}
    for itype in MODEL_TYPE:
        accuracy = model_train(allUserfulPD, itype, saved_model_path=os.path.join(SaveModelPath, str(inum)))
        print('Accuracy of %s classifier: %f' % (itype, accuracy))
        # select_and_pred(allUserfulPD, model_type=itype, saved_model_path=apath)

        tallFault = sorted(list(set(getTestRealLabels())))
        for i in tallFault:
            if i not in tDic.keys():
                tDic[i] = {}
            tmetrics = get_metrics(getTestRealLabels(), getTestPreLabels(), label=i)
            if "num" not in tDic[i].keys():
                tDic[i]["num"] = tmetrics["realnums"][i]
            # 将数据进行输出并保存
            tDic[i]["accuracy_" + itype] = tmetrics["accuracy"]
            tDic[i]["precision_" + itype] = tmetrics["precision"]
            tDic[i]["recall_" + itype] = tmetrics["recall"]

    if not os.path.exists(saverespath):
        os.makedirs(saverespath)
    itpd = pd.DataFrame(data=tDic).T
    print(itpd)
    itpd.to_csv(os.path.join(saverespath, "单机WRF_Process_预测数据统计信息.csv"))
    print("=========================")
    print("输出信息->", os.path.join(saverespath, "单机WRF_Process_预测数据统计信息.csv"))
    print("模型预测结束")



