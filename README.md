# tmall_spider

使用提示
--------
        读取csv内的产品号一次不适宜过多，建议尝试10个为单位叠加。

2018.04.03
----------
        1.添加爬取买家秀url且入库功能；
        2.新增配置文件配置日志文件路径；
        3.配置同时爬取的goodsn进程数以及每个SN下打开页面的线程数；
        4.新增操作无响应，30秒后刷新该页面功能；
        5.修复多线程运行时，验证登录弹出无法跳转状态bug

2018.03.30
----------
提交tmall爬虫第一版程序


安装说明
--------
        解压geckodriver-v0.20.0-xxx.zip 到python安装目录，设置path到python的安装目录下
        geckodriver-v0.20.0 - 火狐浏览器驱动 (支持Firefox 55.0以上，selenium 3.5以上)
        双击安装python2.7以后，直接双击setup.bat直接安装爬虫所需依赖
        在tmall.cfg文件内设置基本设定，运行前 请配置好日志文件路径 以及父进程数和子进程数
        父进程为同时爬取的goodsn个数，例如：设置为3，则同时最多爬取3个goodsn下对应页面
        父进程 = 基本数据入库子线程 + 评论数据入库子线程 + 配置的子线程数（子线程由一个goodsn下对应的IID所决定，
        例如一个goodsn下，在淘宝天猫上有10个IID，配置的子线程为5，则对应这个goodsn的线程恒为7个。）

