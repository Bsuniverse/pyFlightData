import zipfile
from selenium.webdriver.support import expected_conditions as EC
import os,time
from scipy import interpolate 
import numpy as np
import pandas as pd
import time
import datetime
import re

def interp_low_rate_data(data_list, ini_rate, desti_rate):
    sub_list_numbers = len(data_list)
    whole_data_numbers = sub_list_numbers * ini_rate
    x_step = 100 / ini_rate
    x_stop = x_step * (whole_data_numbers - 1)
    resample_rate = int(desti_rate / ini_rate)

    var_y = data_list.flatten()
    var_x = np.linspace(0, x_stop, num=whole_data_numbers)
    new_x = np.linspace(0, x_stop + x_step * (1 - ini_rate / desti_rate), num=whole_data_numbers * resample_rate)
    inter_func = interpolate.interp1d(var_x, var_y, kind='linear', fill_value='extrapolate')
    new_y = inter_func(new_x)

    return new_y

def trans_json_to_array(json_data):
    if "" in (str_list := json_data.values()):
        temp_list = []
        for str_value in str_list:
            if str_value == "":
                float_value = np.nan
            else:
                float_value = float(str_value)

            temp_list.append(float_value)

        temp_pd = pd.Series(temp_list) 
        temp_np = temp_pd.interpolate().bfill().to_numpy()
        return temp_np
    else:
        try:
            return np.asarray(list(json_data.values())).astype(float)
        except ValueError as e:
            return np.asarray(list(json_data.values()))

def trans_date_to_timestamp(date_data):
    try:
        timstamp = time.mktime(time.strptime(date_data, "%Y-%m-%d %H:%M:%S"))
    except:
        timstamp = time.mktime(time.strptime(date_data, "%Y/%m/%d %H:%M:%S"))

    return timstamp

def trans_timestamp_to_date(timestamp):
    datetime_type = datetime.datetime.fromtimestamp(timestamp)
    return datetime_type.strftime("%H:%M:%S:%f")[:-3]

def lbs_to_kg(gross_weight):
    return 0.4536 * gross_weight

def add_cog_percent(dataframe: pd.DataFrame):
    # CG1_0_01_R,CG1_0_1_R,CG1_100_R,CG1_10_R,CG1_1_R
    cog_0_01 = dataframe.pop('CG1_0_01_R')
    cog_0_1 = dataframe.pop('CG1_0_1_R')
    cog_1 = dataframe.pop('CG1_1_R')
    cog_10 = dataframe.pop('CG1_10_R')
    cog_100 = dataframe.pop('CG1_100_R')

    cog = 0.01 * cog_0_01 + 0.1 * cog_0_1 + cog_1 + 10 * cog_10 + 100 * cog_100

    dataframe.insert(loc=0, column='COG', value=cog)

    return dataframe

def extract_zip(file_name, dir_name, delete_origin=True):
    with zipfile.ZipFile(file_name, 'r') as zip_file:
        zip_file.extractall(dir_name)
    if delete_origin:
        os.remove(file_name)

def save_json_date(json_data, file_path):
    json_dataframe = pd.DataFrame()

    for key, value in json_data.items():
        if key == 'DATE':
            date_rate = value['rate']
            date_data = trans_json_to_array(value['value'])

            timestamp_func = np.vectorize(trans_date_to_timestamp)
            date_func = np.vectorize(trans_timestamp_to_date)

            timestamp_list = timestamp_func(date_data)
            date_list = interp_low_rate_data(timestamp_list, date_rate, 16)
            date_list = date_func(date_list)
        else:
            value_rate = value['rate']
            if value_rate < 16:
                value_list = trans_json_to_array(value['value']) 
                value_list = interp_low_rate_data(value_list, value_rate, 16)
            elif value_rate == 16:
                value_list = trans_json_to_array(json_data[key]['value']).flatten() 

            try:
                json_dataframe[key] = value_list
            except ValueError as e:
                print(f"可能出现罕见的部分时间段数据缺失错误，程序报错为{e}\n将默认为后段数据丢失直接用NaN进行补齐")
                index_result = re.findall(r"\((\d*?)\)", repr(e))
                larger_range = int(index_result[1])
                little_range = int(index_result[0])
                value_list = np.pad(value_list, (0, larger_range - little_range), 'constant', constant_values=(np.nan, np.nan))
                json_dataframe[key] = value_list

    json_dataframe = add_cog_percent(json_dataframe)
    json_dataframe['GrossWt_R'] = lbs_to_kg(json_dataframe['GrossWt_R'])

    mid_json_dataframe = json_dataframe.sort_index(axis=1)
    mid_json_dataframe.insert(loc=0, column='DATE', value=date_list)

    mid_json_dataframe.to_csv(file_path, sep='\t', index=False)

def trans_list_to_array(list_data):
    if [""] in list_data:
        temp_list = []
        for str_value in list_data:
            if str_value == [""]:
                float_value = np.nan
            else:
                float_value = float(str_value[0])

            temp_list.append(float_value)

        temp_pd = pd.Series(temp_list) 
        temp_np = temp_pd.interpolate().bfill().to_numpy()
        return temp_np
    else:
        try:
            return np.asarray(list_data).astype(float)
        except ValueError as e:
            return np.asarray(list_data)

def save_dict_data(dict_data, file_path):
    json_dataframe = pd.DataFrame()

    for key, value in dict_data.items():
        if key == 'DATE':
            date_rate = value['rate']
            date_data = trans_list_to_array(value['value'])

            timestamp_func = np.vectorize(trans_date_to_timestamp)
            date_func = np.vectorize(trans_timestamp_to_date)

            timestamp_list = timestamp_func(date_data)
            date_list = interp_low_rate_data(timestamp_list, date_rate, 16)
            date_list = date_func(date_list)
        else:
            value_rate = value['rate']
            if value_rate < 16:
                value_list = trans_list_to_array(value['value']) 
                value_list = interp_low_rate_data(value_list, value_rate, 16)
            elif value_rate == 16:
                value_list = trans_list_to_array(dict_data[key]['value']).flatten() 

            try:
                json_dataframe[key] = value_list
            except ValueError as e:
                print(f"可能出现罕见的部分时间段数据缺失错误，程序报错为{e}\n将默认为后段数据丢失直接用NaN进行补齐")
                index_result = re.findall(r"\((\d*?)\)", repr(e))
                larger_range = int(index_result[1])
                little_range = int(index_result[0])
                value_list = np.pad(value_list, (0, larger_range - little_range), 'constant', constant_values=(np.nan, np.nan))
                json_dataframe[key] = value_list

    json_dataframe = add_cog_percent(json_dataframe)
    json_dataframe['GrossWt_R'] = lbs_to_kg(json_dataframe['GrossWt_R'])

    mid_json_dataframe = json_dataframe.sort_index(axis=1)
    mid_json_dataframe.insert(loc=0, column='DATE', value=date_list)

    mid_json_dataframe.to_csv(file_path, sep='\t', index=False)