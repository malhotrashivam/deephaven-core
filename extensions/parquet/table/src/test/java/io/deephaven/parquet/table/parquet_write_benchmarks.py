import pandas
import pyarrow.parquet
import time
from deephaven.parquet import write, read

NUM_RUNS = 5
FILE_PATH = "/Users/shivammalhotra/Documents/byteArrays512FourMillionRows.parquet"
PAGE_SIZES = [8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576]

# Pyarrow benchmarks
pa_table = pyarrow.parquet.read_table(FILE_PATH)
for PAGE_SIZE in PAGE_SIZES:
    destFilename = "data_from_pa_" + str(PAGE_SIZE) + ".parquet"
    totalTime = 0
    for i in range(NUM_RUNS):
        start = time.time_ns()
        pyarrow.parquet.write_table(pa_table, destFilename, compression="NONE", use_dictionary=False, data_page_size = PAGE_SIZE)
        end = time.time_ns()
        print(str(i+1) + ". Run time for Pyarrow for page size " + str(PAGE_SIZE) + " is " + str((end - start)/10**9) + " sec")
        totalTime += (end - start)
    print("=> Average run time for Pyarrow for page size " + str(PAGE_SIZE) + " is " + str(totalTime/(10**9 * NUM_RUNS)) + " sec")


# DH benchmarks
dh_table = read(FILE_PATH).select()
for PAGE_SIZE in PAGE_SIZES:
    destFilename = "data_from_dh_" + str(PAGE_SIZE) + ".parquet"
    totalTime = 0
    for i in range(NUM_RUNS):
        start = time.time_ns()
        write(dh_table, destFilename, compression_codec_name="UNCOMPRESSED", target_page_size=PAGE_SIZE)
        end = time.time_ns()
        totalTime += (end-start)
        print(str(i+1) + ". Run time for DH for page size " + str(PAGE_SIZE) + " is " + str((end - start)/10**9) + " sec")
    print("=> Average run time for DH for page size " + str(PAGE_SIZE) + " is " + str(totalTime/(10**9 * NUM_RUNS)) + " sec")