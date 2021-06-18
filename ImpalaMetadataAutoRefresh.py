#!/usr/bin/python
# coding=utf8

import json
import requests
from impala.dbapi import connect
from pykafka import KafkaClient
import sys
reload(sys)
sys.setdefaultencoding('utf8')

is_debug = False

#env
env = '{env}'

#kafka config
host = '{kafkaHost}'
client = KafkaClient(hosts="%s:{kafkaPort}" % host)
topicName = 'ATLAS_HOOK'

#impala config
impalaHost = '{impalaHost}'
impalaPort = {impalaPort}

#dindin config
dindinMachine='https://oapi.dingtalk.com/robot/send?access_token={access_token}'


# 获取impala连接
def get_conn(host, port):
    conn = connect(host=host, port=port)
    return conn


# 执行Impala SQL
def impala_query(conn, sql):
    if is_debug:
        print('\033[36m QUERY: \033[0m')
        print('\033[36m' + sql + '\033[0m')
        print(' ')
    cur = conn.cursor()
    cur.execute(sql)
    data_list = cur.fetchall()
    return data_list


def impala_exec(conn, sql):
    if is_debug:
        print('\033[31m EXEC: \033[0m')
        print('\033[31m' + sql + '\033[0m')
        print(' ')
    cur = conn.cursor()
    cur.execute(sql)


def handel(hookContent):
    try:

        #获取impala连接
        conn = conn = get_conn(impalaHost, impalaPort)

        #转换成json对象
        jsonObj = json.loads(hookContent)
        #获取操作类型
        messageType = jsonObj['message']['type']
        entities = jsonObj['message']['entities'];

        #只处理两种操作类型ENTITY_CREATE_V2和ENTITY_DELETE_V2
        #创建表、清空表、插入覆盖表都会触发ENTITY_CREATE_V2
        #根据这两种类型不同，获取对应的表信息
        tableInfo = None
        if messageType == 'ENTITY_DELETE_V2':
            tableInfo = getDeleteOpTableInfo(entities)
        elif messageType == 'ENTITY_CREATE_V2':
            tableInfo = getCreateOpTableInfo(entities)

        print '操作类型:'+messageType+',表对象：'+tableInfo

        table = tableInfo[0:tableInfo.index('@')]
        #更新impala元数据
        if messageType == 'ENTITY_DELETE_V2':
            sendDinMsg(env+':表'+table+'被删除，元数据已自动刷新')
        elif messageType == 'ENTITY_CREATE_V2':
            sendDinMsg(env+':表'+table+'被更新(Create、Insert overwrite、Truncate)，元数据已自动刷新')

        impala_exec(conn, 'invalidate metadata '+table+';')

        conn.close()
    except Exception as e:
        print e



#获取删除操作的表信息
def getDeleteOpTableInfo(entities):
    for entitie in entities:
        if entitie.has_key('uniqueAttributes'):
            if(entitie['uniqueAttributes'].has_key('qualifiedName')):
                return entitie['uniqueAttributes']['qualifiedName']

#获取创建操作的表信息
def getCreateOpTableInfo(entities):
    for childEntitie in entities['entities']:
            #获取typeName。已知道的有两个hive_process和hive_table他们获取表名称的方式不一样
            typeName = childEntitie['typeName']
            if typeName == 'hive_process':
                return getHiveProcessOpTableInfo(childEntitie)
            elif typeName == 'hive_table':
                return getHiveTableOpTableInfo(childEntitie)

def getHiveProcessOpTableInfo(childEntitie):
    for item in childEntitie['attributes']['outputs']:
        if item['typeName']=='hive_table':
            return item['uniqueAttributes']['qualifiedName']

    return None;

def getHiveTableOpTableInfo(childEntitie):
    return childEntitie['attributes']['qualifiedName']

def sendDinMsg(msg):
    headers = {'Content-Type':'application/json;charset=UTF-8'}
    msg_body = {
        "msgtype":"text",
        "text":{
            "content":msg
        },
        "at":{
            "atMobiles":[
            ],
            "isAtAll":False
        }
    }

    try:
        res = requests.post(dindinMachine,data = json.dumps(msg_body),headers=headers)
        res = res.json()
        print res
    except Exception as e:
        print e





if __name__ == '__main__':
    # 消费者
    topic = client.topics[topicName]
    consumer = topic.get_simple_consumer(consumer_group='impalaMetadataAutoRefresh', auto_commit_enable=True,auto_commit_interval_ms=3000, consumer_id='impalaMetadataAutoRefresh')
    for message in consumer:
        if message is not None:
            #得到Atlas监听在hive元数据变动的内容。分析得到操作类型和表名称。使用impala刷新元数据
            handel(message.value)


