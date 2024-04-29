import shutil
import time
import json
import glob
import os
import requests
import sys
import tqdm
import datetime
import subprocess
import logging
import click
from flightScrawl import chrome_runner, FlightSpider, get_ul_list_number
from preprocess import process_zip_to_txt, get_pure_name_list, get_need_vars_from_csv
from pathos.multiprocessing import ProcessingPool as newpool
from pathos import multiprocessing
from utility import extract_zip
from itertools import repeat


logger = logging.getLogger(__name__)


def folder_creator(folder_name: str):
    if not os.path.exists(folder_name):
        print(f"不存在{folder_name},尝试创建目录")
        os.mkdir(folder_name)


def json_compare(
    chrome, flight_spider: FlightSpider, old_json_file: str, token, cookie
):
    # 获取最新、完整的json_list
    li_number = get_ul_list_number(chrome)
    # tqdm has many interesting parameters. Feel free to experiment!
    # 设置下载进度条，抓取所有航班列表
    print("获取全部航班列表")
    whole_json_list = []
    tqdm_params = {
        "total": li_number,
    }
    with tqdm.tqdm(**tqdm_params) as pb:
        for i in range(1, li_number + 1):
            # 每下载一页数据，进度条＋1
            pb.update(1)
            res_json = flight_spider.get_uids(str(i), token, cookie)
            whole_json_list = whole_json_list + json.loads(res_json)["result"]["list"]

    # 读取旧json_list
    with open(old_json_file, "r") as f:
        old_json_list = json.load(f)

    # 获取增量信息
    old_uid8_set = set([flight_info["uid8"] for flight_info in old_json_list])
    whole_uid8_set = set([flight_info["uid8"] for flight_info in whole_json_list])
    added_uid8_set = whole_uid8_set - old_uid8_set

    return chrome, added_uid8_set, whole_json_list


@click.group(chain=True)
def cli():
    pass


@cli.command("download")
@click.option("-u", "--user", required=True, type=str, help="运营数据平台用户名")
@click.option("-p", "--password", required=True, type=str, help="用户密码")
@click.option(
    "-d",
    "--config-dir",
    show_default=True,
    default="./configs",
    help="配置文件夹路径，里面含有list.json(包含所有已下载运营数据信息)，need_vars.csv(包含需要参数的信息)",
)
@click.option(
    "-o",
    "--output-folder",
    show_default=True,
    default="./C919_data",
    help="输出文件路径",
)
@click.option("--log", show_default=True, default="./logs", help="logs日志文件储存目录")
@click.option(
    "-t", "--file-type", type=click.Choice(["JSON", "CSV"], case_sensitive=False)
)
def download_files(user, password, config_dir, output_folder, file_type, log):
    var_date = datetime.datetime.today()
    logging.basicConfig(
        filename=os.path.join(
            log, f"download_{var_date.year}_{var_date.month}_{var_date.day}.log"
        ),
        level=logging.INFO,
        filemode="w",
    )
    # 创建文件夹
    folder_creator(output_folder)
    # 创建log文件夹
    folder_creator(log)

    downloaded_list = os.path.join(config_dir, "list.json")

    chrome, token, cookie = chrome_runner(user, password)
    # 爬取所有的list
    flight_spider = FlightSpider()

    # 预先进行文件夹变换
    time.sleep(10)
    py_dir = os.path.dirname(sys.argv[0])
    os.chdir(py_dir)

    # 获取所有运营数据列表和需要下载的uid8列表,并临时将所有运营数据列表储存在本地
    chrome, added_ui8_set, whole_json_list = json_compare(
        chrome, flight_spider, downloaded_list, token, cookie
    )
    # 临时储存结果到本地
    with open(os.path.join(log, "temp.json"), "w") as f:
        json.dump(json.loads(json.dumps(whole_json_list)), f)

    if file_type.lower() == "JSON".lower():
        # json格式的数据爬取，特点：数据容易不全，但是会精确定位到需求变量
        # 遍历已经下好的文件
        for json_list in whole_json_list:
            front_name = f"{json_list['acReg']}_{json_list['uid8']}"
            if json_list["uid8"] in added_ui8_set:
                file_name = os.path.join(output_folder, "json", front_name)
                flight_spider.download_load_by_uid8(
                    json_list["uid8"], token=token, cookie=cookie, path=file_name
                )
            else:
                logger.info("%s.txt已经存在，跳过", front_name)
    elif file_type.lower() == "CSV".lower():
        for json_list in whole_json_list:
            # 先给一个压缩包的名字
            data_zip = json_list["dataZip"].strip(".zip")
            front_name = (
                f"{data_zip}_{json_list['departureIcao']}_{json_list['arrivalIcao']}"
            )
            # 进行增量下载
            if json_list["uid8"] in added_ui8_set:

                file_name = os.path.join(output_folder, f"{front_name}.zip")
                try:
                    flight_spider.download_by_uid8(
                        json_list["uid8"], token=token, cookie=cookie, path=file_name
                    )
                except requests.HTTPError as e:
                    logger.warning("出现%s, 可能cookies过期，尝试重新获取cookies", e)
                    chrome, token, cookie = chrome_runner()
                    flight_spider.download_by_uid8(
                        json_list["uid8"], token=token, cookie=cookie, path=file_name
                    )

                # 提取zip文件
                extract_zip(file_name, output_folder)
            else:
                logger.info("%s.zip已经存在，跳过", front_name)

    # 当前面的全部执行完毕后，存储最新的可下载数据list
    with open(downloaded_list, "w") as f:
        json.dump(json.loads(json.dumps(whole_json_list)), f)


@cli.command("process")
@click.option(
    "-d",
    "--config-dir",
    show_default=True,
    default="./configs",
    help="配置文件夹路径，里面含有list.json(包含所有已下载运营数据信息)，need_vars.csv(包含需要参数的信息)",
)
@click.option(
    "-o",
    "--output-folder",
    show_default=True,
    default="./C919_data/TXT",
    help="输出文件路径",
)
@click.option(
    "-i",
    "--input-folder",
    show_default=True,
    default="./C919_data",
    help="输入文件路径",
)
@click.option(
    "-a",
    "--archive-folder",
    show_default=True,
    default="./C919_data/Archive",
    help="归档文件路径，用于指明之前处理的文件储存路径，避免重复输出，包含了zip存档和txt存档，在所有转换完成后，程序自动将文件移动到archive中",
)
@click.option("--log", show_default=True, default="./logs", help="logs日志文件储存目录")
def preprocess(config_dir, output_folder, input_folder, archive_folder, log):
    # 获取当天日期
    var_date = datetime.datetime.today()
    py_dir = os.path.dirname(sys.argv[0])
    os.chdir(py_dir)
    logging.basicConfig(
        filename=os.path.join(
            log, f"process_{var_date.year}_{var_date.month}_{var_date.day}.log"
        ),
        level=logging.INFO,
    )

    folder_creator(output_folder)
    folder_creator(archive_folder)

    zip_file_s = glob.glob(os.path.join(input_folder, "*.zip"))
    txt_file_1 = glob.glob(os.path.join(output_folder, "*.txt"))
    txt_file_2 = glob.glob(os.path.join(archive_folder, "*.zip"))
    txt_file_s = txt_file_1 + txt_file_2
    # 生成已经前处理完成的列表，方便接下来进行对比
    txt_file_s = get_pure_name_list(txt_file_s)
    need_vars = get_need_vars_from_csv(os.path.join(config_dir, "need_vars.csv"))

    total = len(zip_file_s)

    cores = multiprocessing.cpu_count()
    with newpool(processes=cores) as p:
        r = list(
            tqdm.tqdm(
                p.imap(
                    process_zip_to_txt,
                    zip_file_s,
                    repeat(txt_file_s),
                    repeat(need_vars),
                    repeat(output_folder),
                ),
                total=total,
            )
        )

    logger.info("转换成功的文件：%s", r)

    # 转移文件到archive文件夹
    for zip_file in zip_file_s:
        shutil.move(zip_file, archive_folder)
    logger.info("zip文件全部转移到Archive目录：%s", archive_folder)

    # 调用7zip对文件进行压缩
    seven_zip_name = f"Release_{var_date.year}_{var_date.month}_{var_date.day}.7z"
    seven_zip_path = os.path.join(output_folder, seven_zip_name)
    if sys.platform.startswith("darwin"):
        seven_zip = "7zz"
    elif sys.platform.startswith("win"):
        seven_zip = "7z.exe"
    cmd = [seven_zip, "a", "-v50m", f"{seven_zip_path}", f"{output_folder}/*.txt"]
    subprocess.run(cmd, check=False)

    # 转移生成的7zip文件到Archive文件夹
    seven_zip_file_s = glob.glob(os.path.join(output_folder, "*.7z*"))
    for seven_zip_file in seven_zip_file_s:
        archive_txt_folder = os.path.join(archive_folder, "TXT")
        folder_creator(archive_txt_folder)
        shutil.move(seven_zip_file, archive_txt_folder)
    logger.info("7z(将TXT文件压缩)文件全部转移到Archive目录: %s", archive_folder)

    shutil.rmtree(output_folder)
    logger.info("删除%s目录本身", output_folder)


if __name__ == "__main__":
    cli()
