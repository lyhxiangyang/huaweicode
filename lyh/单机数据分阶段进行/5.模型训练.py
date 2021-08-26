"""
这个文件的作用主要是用于模型的训练
"""

import os

import pandas as pd

from Classifiers.ModelPred import select_and_pred
from Classifiers.ModelTrain import model_train
from utils.DefineData import MODEL_TYPE, SaveModelPath

savepath4 = "tmp\\single\\4\\"
filename4 = "alluserful.csv"
savepath5 = "tmp\\single\\5\\"

if __name__ == "__main__":
    print("模型训练".center(40, "*"))

    # == 读取步骤四种生成的文件
    tfile = os.path.join(savepath4, filename4)
    allUserfulPD = pd.read_csv(tfile)

    # == 进行模型的训练
    # 获得模型保存的路径
    inum = len(os.listdir(SaveModelPath)) + 1
    apath = os.path.join(SaveModelPath, str(inum))
    for itype in MODEL_TYPE:
        accuracy = model_train(allUserfulPD, itype, saved_model_path=os.path.join(SaveModelPath, str(inum)))
        print('Accuracy of %s classifier: %f' % (itype, accuracy))
        select_and_pred(allUserfulPD, model_type=itype, saved_model_path=apath)

