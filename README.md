# README

本脚本用于从运营数据平台批量下载数据，并对数据进行批处理，提取自己想要的参数。

## 下载数据

- 需要安装好chrome和chrome_driver。

```bash
# 第一次登录网站进行下载
uv run main_remote_debug.py download -t CSV -o download数据路径 --from-start Yes
# 已经登录网站获取航班列表，无需再次抓取list
uv run main_remote_debug.py download -t CSV -o download数据路径 --from-start No
```

## 批处理

- 需要修改配置文件夹./configs/need_vars.csv中的参数，实现自己想要的参数处理;

- 如果程序没有按照预期处理数据，需要清除./configs/list.json中的航班列表；

- 需要提前下载7zip，并且加入环境变量，以便命令行直接调用。

```bash
# 推荐批处理命令
uv run main_remote_debug.py process -i download数据路径 -o 转换完的文件储存路径 -a Archive路径（用于备份原始数据）
```
