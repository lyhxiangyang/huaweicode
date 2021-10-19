
"""
单独指定datascript中步骤3中生成的每个文件每个核心的文件使用现有模型进行预测，预测结果存储在
"""
import os.path

import pandas as pd

from Classifiers.TrainToTest import testThree

# 将一个DataFrame的FAULT_FLAG重值为ff
from utils.DefineData import FAULT_FLAG


def setPDfaultFlag(df: pd.DataFrame) -> pd.DataFrame:
    realflag = list(df[FAULT_FLAG])
    if FAULT_FLAG in df.columns.array:
        df = df.drop(FAULT_FLAG, axis=1)
    dealflag = [(i // 10) * 10 for i in realflag]
    ffdict = {FAULT_FLAG: dealflag}
    tpd = pd.DataFrame(data=ffdict)
    tpd = pd.concat([df, tpd], axis=1)
    return tpd

if __name__ == "__main__":
    spath = "tmp/allpreinformation"
    rpath = "tmp/tData-10-18/多机-红区-process-server-3KM/6.filename-time-core-标准化-特征提取"
    prefilename = "wrf_3km_160_process"
    timeequantum = 0
    prefilenamecore = 0
    usemodelpath = "Classifiers/saved_model/tmp_load1_nosuffix"

    # 要预测文件的路径
    filepath = os.path.join(rpath, prefilename, str(timeequantum), str(prefilenamecore) + ".csv")
    fileinfosavepath = os.path.join(spath, prefilename, str(timeequantum), str(prefilenamecore))
    if not os.path.exists(filepath):
        print("文件不存在")
        exit(1)
    prepd = pd.read_csv(filepath)
    prepd = setPDfaultFlag(prepd)
    testThree(prepd, spath=fileinfosavepath, modelpath=usemodelpath)

