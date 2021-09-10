import pandas as pd

from utils.DataFrameOperation import mergeDataFrames
from utils.DefineData import TIME_COLUMN_NAME

processpathes1 = {
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-1.csv": 91,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-2.csv": 92,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-3.csv": 93,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-5.csv": 94,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cacheGrabNum-10.csv": 95,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-10.csv": 12,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-20.csv": 13,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-50.csv": 14,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpuall-100.csv": 15,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-1.csv": 81,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-2.csv": 82,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-3.csv": 83,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-5.csv": 84,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpugrab-10.csv": 85,
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_process_cpusingle-5.csv": 21,
}
serverpathes1 = [
    "D:\\HuaweiMachine\\测试数据\\wrfrst单机版e5\\wrfrst单机版e5\\wrfrst-7.16-1\\wrf_rst_e5-43_server.csv"
]

if __name__ == "__main__":
    processpds = [pd.read_csv(ipdpath) for ipdpath in processpathes1.keys()]
    allprocesspds, err = mergeDataFrames(processpds)
    if err:
        print("process合并错误")
        exit(1)

    serverpds = [pd.read_csv(ipdpath) for ipdpath in serverpathes1]
    allserverpds, err = mergeDataFrames(serverpds)
    if err:
        print("server合并错误")
        exit(1)

    processTime = set(list(allprocesspds[TIME_COLUMN_NAME]))
    serverTime = set(list(allserverpds[TIME_COLUMN_NAME]))

    if processTime in serverTime:
        print("Server时间大，包含了process时间")
    elif serverTime in processTime:
        print("Process时间大，包含了server时间")

    print("判断结束".center(40, "*"))




