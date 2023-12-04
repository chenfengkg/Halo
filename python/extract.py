# -*- coding: UTF-8 -*-
import re
import numpy as np

# def print_array(arr):
#     # 将数组分割成每30个元素一组
#     chunks = [arr[i:i + 24] for i in range(0, len(arr), 24)]
    
#     for chunk in chunks:
#         # 将每个块重塑为6行5列的数组
#         reshaped_chunk = np.reshape(chunk, (6, 4))
#         # 行列倒置
#         transposed_chunk = np.transpose(reshaped_chunk)
#         # 输出数组
#         for row in transposed_chunk:
#             print(','.join(map(str, row)))
#         print('\n')

def extract_dram_index(fileaname):
    # 从文件中读取数据
    with open(filename, 'r') as f:
        lines = f.readlines()

    # 提取run后面的数字
    data = [float(line.split(',')[1].split()[0]) for line in lines if 'Throughput: run,' in line]

    # 每24个数据为一组，共有6组
    data = np.array(data).reshape(-1, 24)

    # 这24个数据先变为6行4列的数组，然后倒置为4行6列的数组
    data = data.reshape(-1, 6, 4).transpose(0, 2, 1)

    # 输出数组
    for i in range(data.shape[0]):
        print('Group {}:'.format(i+1))
        for row in data[i]:
            print(', '.join(map(str, row)))

def print_array(arr):
    # 将数组分割成每6个元素一组
    chunks = [arr[i:i + 6] for i in range(0, len(arr), 6)]
    
    for chunk in chunks:
        # 输出数组，每个元素之间用逗号隔开
        print(','.join(map(str, chunk)))

def extract_recoverytime(filename):
    with open(filename, 'r') as file:
        data = file.read()
    # load_pattern = r"hash: Halo (\d+\.\d+) ms."
    load_pattern = r"SOFT recovery time: (\d+\.\d+)"
    throughput_values = re.findall(load_pattern, data)
    load_values = [float(value) for value in throughput_values] 
    print("Load Values: ",load_values)

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
    # print_array(run_values)
    print("Run Values: ", run_values)

def print_latency(arr):
    # 二维列表转为一维数组
    arr = [item for sublist in arr for item in sublist]
    # 每54个元素一组，变成6行9列的数组
    chunks = [arr[i:i+36] for i in range(0,len(arr),36)]

    # 输出每个chunk，组与组之间隔开
    for chunk in chunks:
        reshaped_chunk = np.reshape(chunk,(4,9))
        for row in reshaped_chunk:
            print(','.join(map(str,row)))
        print('\n')
    
def extract_latency(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()

    data = []
    for i in range(len(lines)):
        if "Latency" in lines[i]:
            latency_data = []
            # 提取"Latency"行的最后一个值
            value = lines[i].split()[-2]
            latency_data.append(int(value))
            # 提取接下来的8行的最后一个值
            for j in range(i+1, i+9):
                value = lines[j].split()[-1]
                latency_data.append(int(value))
            data.append(latency_data)

    print_latency(data)
    return data


# Use the function
filename = "/data/hujing/Halo/.txt_dram_index_var"
# extract_latency(filename)
# extract_recoverytime(filename)
# extract_throughput(filename)
extract_dram_index(filename)
