import csv
import glob
import io
import os
import sys
import zipfile
from collections import Counter, defaultdict

import tqdm

from utility import save_dict_data


# 获取表头初次出现的位置
def get_index_between(data_list, key_str):
    index_start = data_list.index(key_str)
    return index_start


# 获取当前参数的采样率
def get_sample_rate(data_list, key_str):
    # Counter非常耗时，不要放在较多的循环里
    sample_rate = Counter(data_list).get(key_str)

    return sample_rate


def gen_vars_dict(data_header, real_data, need_vars):
    dict_data = defaultdict(lambda: defaultdict(list))

    for search_header in need_vars:
        sample_rate = get_sample_rate(data_header, search_header)
        dict_data[f"{search_header}"]["rate"] = sample_rate

    for line in real_data:
        for search_header in need_vars:
            index_start = get_index_between(data_header, search_header)
            index_end = index_start + dict_data[f"{search_header}"]["rate"]
            list_data = line[index_start:index_end]
            dict_data[f"{search_header}"]["value"].append(list_data)

    return dict_data


def direct_read_zip(file_name):
    zip_file = zipfile.ZipFile(file_name)
    file_list = zip_file.namelist()

    with zip_file.open(file_list[0], "r") as f:
        content = f.read().decode("unicode-escape")

    return content


def get_csv_header_content(file_name):
    content = direct_read_zip(file_name)
    header = csv.DictReader(io.StringIO(content)).fieldnames
    real_data = csv.reader(io.StringIO(content))
    for i in range(3):
        next(real_data, None)

    return header, real_data


def get_need_vars_from_csv(file_name: str) -> list:
    with open(file_name, "r") as f:
        data = list(csv.reader(f, delimiter=","))[0]

    return data


def get_pure_name_list(file_path_list):
    pure_list = []
    for file_path in file_path_list:
        # 获取zipfile的名字
        _, file_name = os.path.split(file_path)
        prefix_file_name = os.path.splitext(file_name)[0]
        pure_list.append(prefix_file_name)

    return set(pure_list)


def process_zip_to_txt(zip_file, txt_file_s, need_vars, output_folder):
    # 获取zipfile的名字
    _, zip_file_header = os.path.split(zip_file)
    zip_file_header = os.path.splitext(zip_file_header)[0]

    if zip_file_header not in txt_file_s:
        header, real_data = get_csv_header_content(zip_file)
        try:
            dict_data = gen_vars_dict(header, real_data, need_vars)

            # 保存处理dict_data到txt文件中
            txt_file_name = os.path.join(output_folder, f"{zip_file_header}.txt")
            save_dict_data(dict_data, txt_file_name)
        except ValueError as e:
            print(f"{zip_file_header}文件表头不全，具体报错为{e}")
    else:
        pass
        # print(f'{zip_file_header}已经处理过，无需重复处理')

    return zip_file_header


if __name__ == "__main__":
    pass
    # 单线程处理代码
    # py_dir = os.path.dirname(sys.argv[0])
    # os.chdir(py_dir)

    # zip_file_s = glob.glob("E:/python_process/Preprocess/*.zip")
    # txt_file_s = glob.glob("E:/python_process/TXT/*.txt")
    # txt_folder = "E:/python_process/TXT"
    # # 生成已经前处理完成的列表，方便接下来进行对比
    # txt_file_s = get_pure_name_list(txt_file_s)
    # need_vars = get_need_vars_from_csv("./configs/need_vars.csv")

    # downloaded_list = []
    # # tqdm进度条设置
    # total = len(zip_file_s)
    # tqdm_params = {
    #     "desc": "数据处理进度",
    #     "total": total,
    #     "miniters": 1,
    # }
    # with tqdm.tqdm(**tqdm_params) as pb:
    #     for zip_file in zip_file_s:
    #         # 获取zipfile的名字
    #         _, zip_file_path = os.path.split(zip_file)
    #         zip_file_path = os.path.splitext(zip_file_path)[0]
    #         downloaded_list.append(zip_file_path)

    #         if zip_file_path not in txt_file_s:
    #             header, real_data = get_csv_header_content(zip_file)
    #             dict_data = gen_vars_dict(header, real_data, need_vars)

    #             # 保存处理dict_data到txt文件中
    #             txt_folder = "E:/python_process/TXT"
    #             txt_file_name = os.path.join(txt_folder, f"{zip_file_path}.txt")
    #             try:
    #                 pb.set_description(f"正在处理{zip_file_path}")
    #                 save_dict_data(dict_data, txt_file_name)
    #             except:
    #                 with open(
    #                     "./logs/processed_zip_list.csv", "w", newline=""
    #                 ) as csvwt:
    #                     csv_writer = csv.writer(csvwt)
    #                     csv_writer.writerow(downloaded_list)

    #         else:
    #             pb.write(f"{zip_file_path}已经处理过，无需重复处理")

    #         pb.update(1)

    #     with open("./logs/processed_zip_list.csv", "w", newline="") as csvwt:
    #         csv_writer = csv.writer(csvwt)
    #         csv_writer.writerow(downloaded_list)
