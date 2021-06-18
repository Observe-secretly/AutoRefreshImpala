# 背景
使用过低版本（3.2及以下）Impala的朋友都知道，每当更新Hive数据后，需要运行`invalidate metadata` 或者 `invalidate metadata [table]`在Impala中更新元数据才能正常查询这张表。它通常会被写在ETL脚本的最后。在HIVE跑一些临时任务的时候，也需要单独登录Impala-shell打开命令行执行更新操作。

# 脚本功能
实时更新Impala元数据

# 技术原理
研究了一番新版本的Impala，新版本的Impala是使用Hive Hook机制实现的元数据刷新。Hive Hook允许用户自己监听Hive表的变动，然后扩展实现一些非标功能。比如`Apache Atlas`使用Hive Hook实现了血缘图。恰巧笔者使用的Ambari集群就有集成Atlas（开心的一批，不用自己写Hive Hook的扩展了）。那我们直接使用Atlas的Hive Hook扩展抓出来的数据，就能完成数据表更新的监听操作。然后更新Impala的某个表元数据。
### Atlas Hive Hook的工作方式和流程
```
Hive --> Atlas Hive Hook --> Kafka --> Atlas
```
Atlas Hive Hook会把数据写入到Kafka中。Topic名字叫`ATLAS_HOOK`。在抓到的json报文中你可以从`message.type`中获取操作类型。如：删除表、创建表、更新表等操作。从`message.entities`中获取本次操作的Hive表名称叫什么。到此脚本的实现原理已经全部介绍完毕

# 如何使用
## 环境要求
Python:2.7
</br>
Impala客户端：impyla( 安装命令：pip install  six bit_array pure_sasl  impyla==0.10.0)

## 如何配置
请把脚本中出现如下字符的地方替换成正确的配置：
```
{env} : 环境信息。用于通知使用
{kafkaHost}：Kafka Broker地址
{kafkaPort}：Kafka Broker端口（Ambari集群：默认6667）
{impalaHost}：Impala DAEMONS地址
{impalaPort}：Impala DAEMONS端口（默认21050）
{access_token}：钉钉机器人的access_token。若不用可以把通知相关方法注释掉
```
## 如何运行
请执行命令
```
nohup python -u impalaMetadataAutoRefresh.py > run.log &
```
