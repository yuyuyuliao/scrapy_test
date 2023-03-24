import pymysql
import logging
import json
import traceback
import configparser
from kafka import KafkaConsumer, KafkaProducer
import uuid
from urllib.parse import urljoin, urlparse
from datetime import datetime
import hashlib
from redis.cluster import RedisCluster as Redis
from redis.cluster import ClusterNode
import requests
import re


class ParseCommonTool:
    # common tool when do parse work
    @staticmethod
    def safeDict(src_dict, kw):
        sample = {}
        if (type(src_dict) == type(sample)):
            return src_dict.get(kw, "")
        else:
            return ""

    @staticmethod
    def safeListHead(mylist):
        if len(mylist) >= 1:
            return mylist[0]
        else:
            return ""

    @staticmethod
    def transQuota(text: str):
        if (type(text) != str): text = str(text)
        pattern = re.compile(r'"(.*?)"')
        result = pattern.findall(text)
        for l in result:
            text = text.replace('"{}"'.format(l), '“{}”'.format(l))
        # 将成对的英文单引号改为成对的中文单引号
        pattern = re.compile(r"'(.*?)'")
        result = pattern.findall(text)
        for l in result:
            text = text.replace("'{}'".format(l), "‘{}’".format(l))
        return text

    @staticmethod
    def urlFix(CurUrl: str, AbsUrl: str) -> str:
        '''
        Trans Relative URL to Absolute URL
        CurUrl: url of current page
        AbsUrl: url in html of current page
        '''
        try:
            fix_url = AbsUrl
            if (AbsUrl.startswith("//")):
                fix_url = CurUrl.split("//")[0] + AbsUrl
            elif (AbsUrl.startswith("/")):
                http_head = CurUrl.split("//")[0]
                domain = CurUrl.split("//")[1].split("/")[0]
                fix_url = http_head + "//" + domain + AbsUrl
            elif (AbsUrl.startswith("./")):
                fix_url = CurUrl[:CurUrl.rfind("/")] + "/" + AbsUrl[2:]
            elif (AbsUrl.startswith("../")):
                curHost = CurUrl[:CurUrl.rfind("/")]
                new_CurUrl = curHost[:curHost.rfind("/")] + "/"
                new_AbsUrl = AbsUrl[3:]
                if (not new_AbsUrl.startswith("../")):
                    new_AbsUrl = "./" + new_AbsUrl
                return ParseCommonTool.urlFix(new_CurUrl, new_AbsUrl)
            return fix_url
        except:
            logging.error("urlFix Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def addslashes(s: str):
        '''
        Returns a string with backslashes added before characters that need to be escaped. These characters are:
            single quote (')
            double quote (")
            backslash (\)
            NUL (the NUL byte)
        '''
        if (type(s) != str): s = str(s)
        d = {'"': '\\"', "'": "\\'", "\0": "\\\0", "\\": "\\\\"}
        return ''.join(d.get(c, c) for c in s)

    @staticmethod
    def initRecordByTopicField(record: dict, topic_field: tuple):
        '''
        Init all fields in record by ""
        '''
        for rd in topic_field:
            field_name = rd[0]
            record[field_name] = ""


class GeneralTopicTool:
    @staticmethod
    def makePicObj(file: dict,
                   bucket_name: str,
                   url: str,
                   file_type: str,
                   file_format="") -> dict:
        file["bucket_name"] = bucket_name
        file["url"] = url
        file["relative_url"] = GeneralTopicTool.getRelativeUrl(url)
        file["file_type"] = file_type
        if (file_format != ""):
            file["file_format"] = file_format
        else:
            file["file_format"] = file["relative_url"].rsplit('.')[-1]
        pass

    @staticmethod
    def getRelativeUrl(url: str) -> str:
        '''
        Generate relative url in MinIO
        '''
        fix_url = urljoin(url, urlparse(url).path)
        suffix = ""
        last_path = urlparse(fix_url).path.rsplit("/")[-1]
        if ("." in last_path and len(last_path) > 1):
            suffix = last_path[last_path.rfind("."):]
        else:
            suffix = ".jpg"
        relative_url = "/" + datetime.now().strftime("%Y-%m-%d") + "/" + str(uuid.uuid1()) + suffix
        return relative_url
        pass

    @staticmethod
    def getId(id_resource):
        req_url = id_resource["url"]
        res = requests.get(req_url)
        return res.text
        pass


class SpiderSQLTool:
    @staticmethod
    def getFromMySql(db_conf: dict, sql: str):
        try:
            conn = pymysql.connect(host=str(db_conf["host"]),
                                   user=str(db_conf["user"]),
                                   password=str(db_conf["password"]),
                                   port=int(db_conf["port"]),
                                   database=str(db_conf["database"]))
            cur = conn.cursor()
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            if (res):
                return res
        except:
            logging.error("getFromMySql Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getResourceConf(resource_name: str) -> dict:
        '''
        Get resource config info from DB like kafka cluster and redis
        '''
        try:
            config = configparser.ConfigParser()
            path = "config.ini"
            config.read(path)
            host = config["DB"]["host"]
            user = config["DB"]["user"]
            password = config["DB"]["password"]
            port = int(config["DB"]["port"])
            database = config["DB"]["database"]
            db_conf = {
                "host": host,
                "user": user,
                "password": password,
                "port": port,
                "database": database
            }
            sql = 'select conf_set from resource_conf where resource_name=\"{}\"'.format(
                ParseCommonTool.addslashes(resource_name))
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                resource_conf = json.loads(res[0])
                return resource_conf
            else:
                logging.error("No resource name matchs")
                return None
        except:
            logging.error("getResource Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getTypeRd(db_conf: dict, platfrom_name: str) -> list:
        '''
        Get record by platfrom from cultural_industry_type
        '''
        try:
            db_conf["database"] = "brand"
            sql = 'select platform_type_id,platform_type,second_platform_type,third_platform_type,type_url from cultural_industry_type where platform_name=\"{}\"'.format(
                ParseCommonTool.addslashes(platfrom_name))
            res = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (res):
                return res
            else:
                logging.error("No type record matchs")
                return None
        except:
            logging.error("getTypeRd Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getTypeDict(db_conf: dict, platfrom_name) -> dict:
        '''
        Make type dict for given platfrom
        '''
        try:
            db_conf["database"] = "brand"
            sql = 'select platform_type_id,platform_type,second_platform_type from cultural_industry_type where platform_name=\"{}\"'.format(
                platfrom_name)
            res = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (res):
                type_dict = {}
                for i in res:
                    platform_type_id = i[0]
                    platform_type = i[1]
                    second_platform_type = i[2]
                    if (second_platform_type == None):
                        second_platform_type = "self"
                    if (platform_type not in type_dict):
                        type_dict[platform_type] = {}
                    type_dict[platform_type][
                        second_platform_type] = platform_type_id
                return type_dict
        except:
            logging.error("getTypeRd Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getHeader(db_conf: dict, spider_name: str) -> dict:
        '''
        Get header by spider_name
        Please Make sure header is not NULL before use
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'select web_conf.header from web_conf,spider WHERE spider.web_conf_id=web_conf.id AND spider.spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                header = json.loads(res[0])
                return header
        except:
            logging.error("getHeader Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getAutoSpiderHeader(db_conf: dict, spider_name: str) -> dict:
        '''
        Get header by spider_name
        Please Make sure header is not NULL before use
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'select web_conf.header from web_conf,auto_spider WHERE auto_spider.web_conf_id=web_conf.id AND auto_spider.spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                header = json.loads(res[0])
                return header
        except:
            logging.error("getAutoSpiderHeader Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getCookie(db_conf: dict, spider_name: str) -> list:
        '''
        Get record by spider_name
        Please Make sure cookie is not NULL before use
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'select web_conf.cookie from web_conf,spider WHERE spider.web_conf_id=web_conf.id AND spider.spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                header = json.loads(res[0])
                return header
        except:
            logging.error("getHeader Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getAutoSpiderRule(db_conf: dict, spider_name: str) -> str:
        try:
            db_conf["database"] = "spider_info"
            sql = 'SELECT private_rules FROM auto_spider WHERE spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                rule = json.loads(res[0])
                return rule
        except:
            logging.error("getAutoSpiderRule Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getTopic(db_conf: dict, spider_name: str) -> str:
        '''
        Get Topic by spider_name
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'SELECT topic.topic_name FROM spider,topic WHERE spider.topic_id=topic.id AND spider.spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                topic_name = res[0]
                return topic_name
        except:
            logging.error("getTopicField Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getAutoSpiderTopic(db_conf: dict, spider_name: str) -> str:
        '''
        Get Topic by spider_name
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'SELECT topic.topic_name FROM auto_spider,topic WHERE auto_spider.topic_id=topic.id AND auto_spider.spider_name=\"{}\"'.format(
                spider_name)
            rd = SpiderSQLTool.getFromMySql(db_conf, sql)
            if (rd):
                res = rd[0]
                topic_name = res[0]
                return topic_name
        except:
            logging.error("getAutoSpiderTopic Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getTopicField(db_conf: dict, spider_name: str) -> list:
        '''
        Get field_name and constraint by spider_name
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'SELECT field_list.field_name,field_list.is_not_null FROM spider,topic,field_list WHERE spider.topic_id=topic.id AND topic.field_list_id=field_list.field_list_id AND spider.spider_name=\"{}\"'.format(
                spider_name)
            field_list = SpiderSQLTool.getFromMySql(db_conf, sql)
            return field_list
        except:
            logging.error("getTopicField Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getAutoSpiderTopicField(db_conf: dict, spider_name: str) -> list:
        '''
        Get field_name and constraint by spider_name
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = 'SELECT field_list.field_name,field_list.is_not_null FROM auto_spider,topic,field_list WHERE auto_spider.topic_id=topic.id AND topic.field_list_id=field_list.field_list_id AND auto_spider.spider_name=\"{}\"'.format(
                spider_name)
            field_list = SpiderSQLTool.getFromMySql(db_conf, sql)
            return field_list
        except:
            logging.error("getAutoSpiderTopicField Error:")
            logging.error(traceback.format_exc())

    @staticmethod
    def getGeneralTopics(db_conf: dict) -> dict:
        '''
        Get all general topics and their field lists
        '''
        try:
            db_conf["database"] = "spider_info"
            sql = "SELECT topic.topic_name,field_list.field_name,field_list.is_not_null FROM topic,field_list WHERE topic.field_list_id=field_list.field_list_id AND topic.topic_group='general_topic'"
            rds = SpiderSQLTool.getFromMySql(db_conf, sql)
            general_topics = {}
            for rd in rds:
                topic_name = rd[0]
                field_name = rd[1]
                is_not_null = rd[2]
                if (topic_name not in general_topics):
                    general_topics[topic_name] = []
                general_topics[topic_name].append((field_name, is_not_null))
            return general_topics
        except:
            logging.error("getGeneralTopics Error:")
            logging.error(traceback.format_exc())


class KProducer:
    def __init__(self, bootstrap_servers, topic):
        """
        kafka 生产者
        :param bootstrap_servers: 地址
        :param topic:  topic
        """
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all", compression_type="gzip",
                                      max_request_size=20971520
                                      )  # json 格式化发送的内容
        self.topic = topic

    def sync_producer(self, data_li: list):
        """
        同步发送 数据
        :param data_li:  发送数据
        :return:
        """
        for data in data_li:
            future = self.producer.send(self.topic, data)
            record_metadata = future.get(timeout=10)  # 同步确认消费
            partition = record_metadata.partition  # 数据所在的分区
            offset = record_metadata.offset  # 数据所在分区的位置
            # logging.info('save success, partition: {}, offset: {}'.format(
            #     partition, offset))
            logging.info('save success, partition: {}, offset: {}, ID: {}, url: {}, dt: {}, source: {}'.format(
                partition, offset, data['ID'], data['url'],
                data['CreatTime'], data['source']))

    def asyn_producer(self, data_li: list):
        """
        异步发送数据
        :param data_li:发送数据
        :return:
        """
        for data in data_li:
            self.producer.send(self.topic, data)
        self.producer.flush()  # 批量提交

    def asyn_producer_callback(self, data_li: list):
        """
        异步发送数据 + 发送状态处理
        :param data_li:发送数据
        :return:
        """

    def sync_producer_one(self, data):
        """
        同步发送 数据
        :param data_li:  发送数据
        :return:
        """
        future = self.producer.send(self.topic, data)
        record_metadata = future.get(timeout=10)  # 同步确认消费
        partition = record_metadata.partition  # 数据所在的分区
        offset = record_metadata.offset  # 数据所在分区的位置
        # print('save success, partition: {}, offset: {}'.format(
        #     partition, offset))
        data = json.loads(data)
        print('save success, partition: {}, offset: {}, ID: {}, url: {}, dt: {}, source: {}'.format(
            partition, offset, data['ID'], data['url'],
            data['CreatTime'], data['source']))


class SimpleHash(object):
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.cap - 1) & ret


class BloomFilter(object):
    def __init__(self, host='localhost', port=6379, db=1, blockNum=1, key='bloomfilter'):
        """
        :param host: the host of Redis
        :param port: the port of Redis
        :param db: witch db in Redis
        :param blockNum: one blockNum for about 90,000,000; if you have more strings for filtering, increase it.
        :param key: the key's name in Redis
        """
        self.redisnode = [
            ClusterNode("spyder-redis54.zxkw-local.com", 7000),
            ClusterNode("spyder-redis54.zxkw-local.com", 7001),
            ClusterNode("spyder-redis53.zxkw-local.com", 7000),
            ClusterNode("spyder-redis53.zxkw-local.com", 7001),
            ClusterNode("spyder-redis52.zxkw-local.com", 7000),
            ClusterNode("spyder-redis52.zxkw-local.com", 7001),
        ]
        self.server = Redis(startup_nodes=self.redisnode, password="pachong-redis123")
        self.bit_size = 1 << 31  # Redis的String类型最大容量为512M，现使用256M
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    def isContains(self, str_input):
        if not str_input:
            return False
        m5 = hashlib.md5()
        m5.update(str_input.encode('utf-8'))
        str_input = m5.hexdigest()
        ret = True
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(str_input)
            ret = ret & self.server.getbit(name, loc)
        return ret

    def insert(self, str_input):
        m5 = hashlib.md5()
        m5.update(str_input.encode('utf-8'))
        str_input = m5.hexdigest()
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(str_input)
            self.server.setbit(name, loc, 1)
