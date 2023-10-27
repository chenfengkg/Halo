# -*- coding: UTF-8 -*-
import re
import numpy as np

def print_array(arr):
    # 将数组分割成每30个元素一组
    chunks = [arr[i:i + 12] for i in range(0, len(arr), 12)]
    
    for chunk in chunks:
        # 将每个块重塑为6行5列的数组
        reshaped_chunk = np.reshape(chunk, (6, 2))
        # 行列倒置
        transposed_chunk = np.transpose(reshaped_chunk)
        # 输出数组
        for row in transposed_chunk:
            print(','.join(map(str, row)))
        print('\n')

# def print_array(arr):
#     # 将数组分割成每6个元素一组
#     chunks = [arr[i:i + 6] for i in range(0, len(arr), 6)]
    
#     for chunk in chunks:
#         # 输出数组，每个元素之间用逗号隔开
#         print(','.join(map(str, chunk)))

def extract_throughput(filename):
    with open(filename, 'r') as file:
        data = file.read()
    # load_pattern = r"Throughput: load, (\d+\.\d+) Mops/s"
    # throughput_values = re.findall(load_pattern, data)
    # load_values = [float(value) for value in throughput_values] 
    # print("Load Values: ",load_values)
    run_pattern = r"Throughput: run, (\d+\.\d+) Mops/s"
    throughput_values = re.findall(run_pattern, data)
    run_values = [float(value) for value in throughput_values] 
    print_array(run_values)
    # print("Run Values: ", run_values)
    



# Use the function
filename = "/data/hujing/Halo/.txt_hlsh_halo_varthreads_varworkloads"
extract_throughput(filename)
