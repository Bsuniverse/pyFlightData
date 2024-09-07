import json
import requests
import gzip
import base64
import tqdm
import time
import logging
from utility import save_json_date
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import TimeoutException, WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains


logger = logging.getLogger(__name__)


class FlightSpider:
    def __init__(self) -> None:
        self._session = requests.session()

    def get_uids(self, page_number, token, cookie):
        url = "https://cis.comac.cc:8053/api/warehouse/v2/getFlightDataByHighQuery?sf_request_type=ajax"

        header = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json; charset=utf-8",
            "X-Access-Token": token,
            "Cookie": cookie,
            "version": "v2",
        }
        post_data = {"pageNo": page_number, "pageSize": "100", "sourceId": ""}
        res = self._session.post(url, data=json.dumps(post_data), headers=header)
        if res.status_code == 200:
            return res.text
        else:
            logger.warning("statusCode = %s", res.status_code)
            return False

    def download_load_by_uid8(self, uid_8, token, cookie, path, need_vars):
        url = f"https://cis.comac.cc:8053/api/vis/api/v2/getHiveData?id={uid_8}&dataType=EAFR&params=UTCData,{need_vars}&timeStamp=true&jsonStream=true&sf_request_type=ajax"

        header = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json; charset=utf-8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "X-Access-Token": token,
            "Cookie": cookie,
        }

        with self._session.get(url=url, headers=header) as respond:
            respond.raise_for_status()
            debase = base64.b64decode(respond.content)
            json_str = gzip.decompress(debase).decode()
            try:
                json_data = json.loads(json_str)
                with open(f"{path}.json", "w") as f:
                    json.dump(json_data, f, indent=2)
                    del json_data["UTCData"]
                    del json_data["UTCMark"]
                    del json_data["FLIGHT_PHASE"]
                    save_json_date(json_data, f"{path}.txt")
            except json.decoder.JSONDecodeError:
                logger.warning(
                    "Response is empty or data is not valid JSON, data uid8 is %s.",
                    uid_8,
                )

    def download_by_uid8(self, uid_8, token, cookie, path):
        url = "https://cis.comac.cc:8053/api/qar1/flightTaskDownloadFile?sf_request_type=ajax"

        header = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json; charset=utf-8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "X-Access-Token": token,
            "Cookie": cookie,
            "version": "v2",
        }
        postData = {"freq": "64", "uid8Arr": [uid_8], "dataType": ["EAFR"]}
        with open(path, "wb") as f:
            with self._session.post(
                url, data=json.dumps(postData), headers=header, stream=True
            ) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))

                # tqdm has many interesting parameters. Feel free to experiment!
                tqdm_params = {
                    "desc": uid_8,
                    "total": total,
                    "miniters": 1,
                    "unit": "B",
                    "unit_scale": True,
                    "unit_divisor": 1024,
                }
                with tqdm.tqdm(**tqdm_params) as pb:
                    for chunk in r.iter_content(chunk_size=8192):
                        pb.update(len(chunk))
                        f.write(chunk)


def get_log_options():
    save_folder = "./Downloads"
    option = webdriver.ChromeOptions()
    option.add_argument("log-level=3")  # 控制log-level,抑制error出现
    option.add_argument("--no-sandbox")
    # option.add_argument('--headless')  # 设置无头浏览
    option.add_argument("--disable-extensions")
    option.add_argument("--disable-infobars")  # 禁用浏览器正在被自动化程序控制的提示
    option.add_argument("--allow-running-insecure-content")
    option.add_argument("--ignore-certificate-errors")
    option.add_argument("--ignore-ssl-errors")
    option.add_argument("--enable-chrome-browser-cloud-management")
    # option.add_argument("--disable-single-click-autofill")
    # option.add_argument("--disable-autofill-keyboard-accessory-view[8]")
    # option.add_argument("--disable-full-form-autofill-ios")
    # option.add_argument(
    #     "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:55.0) Gecko/20100101 Firefox/55.0"
    # )
    option.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    # option.add_experimental_option(
    #     "prefs",
    #     {
    #         # 不弹出去请求
    #         "profile.default_content_settings.popups": 0,
    #         # 设置默认下载文件目录
    #         "download.default_directory": save_folder,
    #         # 禁止提示
    #         "profile.default_content_setting_values": {"notifications": 2},
    #     },
    # )

    return option


def get_element(driver, pattern):
    """
    在需要缓冲时间的网页中获取某元素
    """
    try:
        element = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_element_located(pattern)
        )
    except TimeoutException:
        logger.error("Timeout loading page, exit scrawling")
        return False
    else:
        return element


# 获取cookies字典结果
def get_cookies_dict(driver):
    params = {"urls": ["https://cis.comac.cc:8053"]}
    # 直接监测浏览器网络获取cookies，不然需要mitm中间人攻击来获取
    cookies_dict = driver.execute_cdp_cmd("Network.getCookies", params)
    return cookies_dict


# 获取X-Access-Token和Cookies结果
def extract_token_and_cookies(cookies_dict):
    cookies_str = ""
    for cookie in cookies_dict["cookies"]:
        # 拼接cookies
        cookie_seg = f'{cookie["name"]}={cookie["value"]}; '
        cookies_str = cookies_str + cookie_seg
        if cookie["name"] == "X-Access-Token":
            # 从cookies中获取token
            token = cookie["value"]
    # 将cookies加工成header需要的格式
    cookies_str = cookies_str.strip("; ")

    return token, cookies_str


# 获取总共有多少页的结果，用于从接口中下载全部数据列表
def get_ul_list_number(chrome) -> int:
    ul = chrome.find_element(
        by=By.XPATH,
        value='//*[@id="app"]/div/div[2]/section/div/div[2]/div[4]/div[1]/div[2]/ul',
    )
    ul_list = ul.find_elements(by=By.XPATH, value="li")
    li_number = ul_list[-1].text

    return int(li_number)


def chrome_runner(user_name, user_password):
    # 使用工具类来获取options配置，而不是平时的webdriver.ChromeOptions()方法
    options = get_log_options()
    chrome = webdriver.Chrome(options=options)
    chrome.get("https://cis.comac.cc:8053/dashboard")  # "https://www.baidu.com/"
    chrome.maximize_window()

    # # 输入账号和密码
    # element_account = get_element(
    #     chrome, (By.XPATH, '//*[@id="Calc"]/div[2]/div[1]/div[1]/div/div[1]/input')
    # )
    # ActionChains(chrome).send_keys_to_element(element_account, user_name).perform()
    # element_passwd = chrome.find_element(by=By.XPATH, value='//*[@id="loginPwd"]')
    # ActionChains(chrome).click(element_passwd).send_keys_to_element(
    #     element_passwd, user_password
    # ).perform()
    # # 勾选登录框并进行登录
    # chrome.find_element(
    #     by=By.XPATH, value='//*[@id="Calc"]/div[4]/span[1]/div[1]'
    # ).click()
    # chrome.find_element(by=By.XPATH, value='//*[@id="Calc"]/div[5]/button').click()
    # 点击cis自动登录
    time.sleep(30)
    element_cis = get_element(
        chrome, (By.XPATH, '//*[@id="app"]/div/form/div[2]/div/div[4]/span')
    )
    element_cis.click()
    # 点击航线数据，进入下载界面
    element_skyline = get_element(
        chrome,
        (By.XPATH, '//*[@id="app"]/div/div[1]/div[2]/div[1]/div/ul/div[8]/div/li/img'),
    )
    element_skyline.click()

    # 获取token和cookies
    cookies_dict = get_cookies_dict(chrome)
    token, cookies = extract_token_and_cookies(cookies_dict)

    return chrome, token, cookies
