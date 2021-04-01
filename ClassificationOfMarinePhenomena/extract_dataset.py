import pandas as pd
from shutil import copyfile
import os
import sys
import re
import datetime
import argparse
import ProgressBar

def count_sample(label_list, df_data):
    print("----"*8)
    for key in label_list.keys():
        print(label_list[key]+":", (df_data['labeling'] == key).sum())
    print("----"*8)


def create_data_file(df_data, label_list, complete_data='./complete_data.txt', sampled_data='./sampled_data.txt'):
    
    if not os.path.exists(sampled_data):
        # 观察当前数据集样本分布
        count_sample(label_list, df_data)

        # 修改文件格式
        df_data['file_name'] = df_data['file_name'].map(lambda s: re.sub(r'[^\.]\w*$', 'tiff', s))
        # 生成新的格式的完整标签文件
        df_data.to_csv(complete_data, sep=" ", index=False)

        # 从每一个category随机抽取1000个样本
        df = pd.DataFrame()
        for key in label_list.keys():
            df = df.append( df_data[df_data['labeling']==key].sample(1000, axis=0) )
        df = df.reset_index(drop=True)

        # 观察随机采样后数据集的样本分布
        count_sample(label_list, df)
        # 生成抽样后的标签文件
        df.to_csv(sampled_data, sep=" ", index=False)
        print("create complete_data sampled_data...")
    else:
        print(sampled_data + " is existed")


def  batch_copy(label_list, sampled_data='./sampled_data.txt', dataset_path='./drive/MyDrive/Dataset/GeoTIFF/', source_path='./GeoTIFF/'):
    # 读取 sampled_data
    df = pd.read_table(sampled_data, sep=" ")
    
    # 判断复制路径是否存在
    if not os.path.exists(dataset_path):
        for key in label_list.keys():
            os.makedirs(dataset_path+key)
        print('create copy path...')
    else:
        print('target path ' + dataset_path + " is existed")
    
    # 判断源数据集是否存在
    if not os.path.exists(source_path):
        print('can not find source dataset')
        return
    
    # 创建日志
    log_path = './log.txt'
    line_num = 1
    if not os.path.exists(log_path):
        with open(log_path, 'w') as f:
            nowtime = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
            f.write('create time: ' + nowtime + '\n')
        print('create log...')
    else:
        with open(log_path, 'r') as f:
            line = f.readlines()
            line_num = len(line)
        print(line_num)
    
    # 绘制progeress bar
    pb = ProgressBar.ProgressBar(len(df))
    # 复制文件到Google drive中
    start_loc = line_num - 1
    with open(log_path,'a') as f:
        for i in range(start_loc, len(df)):
            file_name = df['file_name'][i]
            source = "./GeoTIFF/" + file_name
            target = dataset_path + file_name
            pb.update(i)
            try:
                copyfile(source, target)
                nowtime = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
                f.write(str(i) + ' ' + source + ' ' + nowtime + '\n')
            except IOError as e:
                print("Unable to copy file. %s" %e)
                exit(1)
            except:
                print("Unexpected error:", sys.exc_info())
                exit(1)


def main():
    # 设置命令行参数
    parser = argparse.ArgumentParser(description="extract small dataset from a larg one")
    parser.add_argument('-s', '--source', default='/home/qingren/下载/Dataset/TenGeoP-SARwv/58683.txt')
    args = parser.parse_args()

    source_file = args.source
    df_data = pd.read_table(source_file, sep=" ", header=None,
                    names=["file_name", "labeling", "swatch", "capture_time", "latitude", "longitude"])
    label_list = {"F": "Pure Ocean Waves", "G": "Wind Streaks", "H": "Micro Convective Cells",
                "I": "Rain Cells", "J": "Biological Slicks", "K": "Sea Ice",
                "L": "Iceberg", "M": "Low Wind Area", "N": "Atmospheric Front", "O": "Oceanic Front"}
    create_data_file(df_data, label_list)
    batch_copy(label_list)
    

if __name__ == "__main__":
    main()



