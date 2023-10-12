# Strided sum_by
# ==============

from deephaven import empty_table
from deephaven.parquet import write, read
import time
import os
import statistics

NUM_ROWS = 5 * 10**8
NUM_RUNS = 5
STRIDE = 10**3
PAGE_SIZES = [8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576]

dict = {}
dict["dh_table"] = empty_table(NUM_ROWS).update_view(formulas = ["A=(long)ii", "B=(long)(ii*2)"])
filterStr = "A%" + str(STRIDE) + " == 0"
for PAGE_SIZE in PAGE_SIZES:
    destFilename = "data_from_dh_" + str(PAGE_SIZE) + "," + str(NUM_ROWS) + ".parquet"
    if not os.path.isfile(destFilename):
        write(dict["dh_table"], destFilename, compression_codec_name="UNCOMPRESSED", target_page_size=PAGE_SIZE)
    totalTime = 0
    time_list = []
    for i in range(NUM_RUNS):
        dict["from_disk"] = read(destFilename).where(filters=[filterStr])

        start_sum_by = time.time_ns()
        dict["sparse_sum_by"] = dict["from_disk"].sum_by().select()
        end_sum_by = time.time_ns()
        totalTime += (end_sum_by-start_sum_by)
        time_list.append(end_sum_by-start_sum_by)

        print(str(i+1) + ". Page size = " + str(PAGE_SIZE) + ", NUM_ROWS = " + str(NUM_ROWS) + ", sum_by_time = " + str((end_sum_by - start_sum_by)/10**9) + " sec")
    print("=> For page size = " + str(PAGE_SIZE) + ", avg time = " + str(totalTime/(10**9 * NUM_RUNS)) + " sec, median time = " + str(statistics.median(time_list)/10**9) + " sec")


# sum_by
# =======

from deephaven import empty_table
from deephaven.parquet import write, read
import time
import os
import statistics

NUM_ROWS = 5 * 10**8
NUM_RUNS = 5
PAGE_SIZES = [8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576]

dict = {}
dict["dh_table"] = empty_table(NUM_ROWS).update_view(formulas = ["A=(long)ii", "B=(long)(ii*2)"])
for PAGE_SIZE in PAGE_SIZES:
    destFilename = "data_from_dh_" + str(PAGE_SIZE) + "," + str(NUM_ROWS) + ".parquet"
    if not os.path.isfile(destFilename):
        write(dict["dh_table"], destFilename, compression_codec_name="UNCOMPRESSED", target_page_size=PAGE_SIZE)
    totalTime = 0
    time_list = []
    for i in range(NUM_RUNS):
        dict["from_disk"] = read(destFilename)

        start_sum_by = time.time_ns()
        dict["sparse_sum_by"] = dict["from_disk"].sum_by().select()
        end_sum_by = time.time_ns()
        totalTime += (end_sum_by-start_sum_by)
        time_list.append(end_sum_by-start_sum_by)

        print(str(i+1) + ". Page size = " + str(PAGE_SIZE) + ", NUM_ROWS = " + str(NUM_ROWS) + ", sum_by_time = " + str((end_sum_by - start_sum_by)/10**9) + " sec")
    print("=> For page size = " + str(PAGE_SIZE) + ", avg time = " + str(totalTime/(10**9 * NUM_RUNS)) + " sec, median time = " + str(statistics.median(time_list)/10**9) + " sec")
