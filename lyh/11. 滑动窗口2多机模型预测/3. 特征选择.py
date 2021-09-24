"""
目的：使用测试数据中的正常数据和异常数据来选择有用的特征
最终生成一个
1. 包含正常数据和异常数据的所有数据
2. 将1km 3km正常的提取之后的特征值的文件也显示出来
"""

from utils.DataFrameOperation import mergeDataFrames
from utils.FeatureExtraction import featureExtraction
from utils.FileSaveRead import readFaultyCoreDict, readFaultyDict, saveFaultyCoreDict, saveFaultyDict

datasavepath = "tmp/Data"
tmpsavepath = "tmp/11.滑动窗口2多机模型预测"
tmpsavepath1 = "tmp/11.滑动窗口2多机模型预测/1.训练数据处理部分"
tmpsavepath2 = "tmp/11.滑动窗口2多机模型预测/2.特征提取"
tmpsavepath3 = "tmp/11.滑动窗口2多机模型预测/3.特征选择"
WINSIZE = 3


if __name__ == "__main__":
    pass
