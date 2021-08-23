"""
这个文件的作用主要是用于模型的训练
"""

import os

import pandas as pd

from Classifiers.ModelTrain import model_train
from utils.DefineData import MODEL_TYPE

savepath4 = "tmp\\4\\"
filename4 = "alluserful.csv"
savepath5 = "tmp\\5\\"

if __name__ == "__main__":
    print("模型训练".center(40, "*"))

# == 读取步骤四种生成的文件
    tfile = os.path.join(savepath4, filename4)
    allUserfulPD = pd.read_csv(tfile)

# == 进行模型的训练
    for itype in MODEL_TYPE:
        accuracy = model_train(allUserfulPD, itype)
        print('Accuracy of %s classifier: %f' % (itype, accuracy))







