@echo off
echo 正在安装python所需依赖包
py -2 -m pip install -r requirement.txt
echo 依赖包安装完成
echo 请按任意键退出 & pause
exit