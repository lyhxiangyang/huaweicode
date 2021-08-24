"""
将1中的内容进行分割成   错误码-DataFrame
"""

import os

import pandas as pd

from utils.DataFrameOperation import divedeDataFrameByFaultFlag, isEmptyInDataFrame

savepathfile1 = "tmp\\1\\1.mergedpd.csv"
savepath2 = "tmp\\2\\"

if __name__ == "__main__":

    if not os.path.exists(savepath2):
        os.mkdir(savepath2)


    # == 将步骤一种的数据进行读取
    mergedPd = pd.read_csv(savepathfile1)

    # == 数据分割
    dictPds, err = divedeDataFrameByFaultFlag(mergedPd)
    if err:
        print("数据分割失败")
        exit(1)
    # == 接下来输出并保存数据
    print("错误码 shape")
    for i in dictPds:
        print("{} {}".format(i, dictPds[i].shape))
        savefilepath = os.path.join(savepath2, str(i)+".csv")
        pd.DataFrame(dictPds[i]).to_csv(savefilepath)
        if isEmptyInDataFrame(dictPds[i]):
            print("错误码{} 存在NAN值".format(i))
            exit(1)



