# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import logging
from datetime import datetime
from easy_scrapy.stool import ParseCommonTool,KProducer,BloomFilter
import json
import traceback
from kafka import errors

class EasyScrapyPipeline:
    def __init__(self):
        pass

    def open_spider(self, spider):
        # Init main kafka producer(for unique topic)
        self.topic_name = spider.topic_name
        self.kafka_conf = spider.kafka_conf
        topic_field = spider.topic_field
        self.topic_dict = {}
        # Save constraints for each field
        for rd in topic_field:
            filed_name = rd[0]
            is_not_null = rd[1]
            self.topic_dict[filed_name] = is_not_null
        self.main_producer = KProducer(bootstrap_servers = self.kafka_conf["host"],topic=self.topic_name)
        # Init sub kafka producer(for general topic)
        self.gtopic_using = spider.general_topics_using
        self.general_topics = spider.general_topics
        self.gtopic_dict = {}
        self.sub_producers = {}
        for gt in self.gtopic_using:
            if(gt not in self.gtopic_dict):
                self.gtopic_dict[gt]={}
            for rd in self.general_topics[gt]:
                filed_name = rd[0]
                is_not_null = rd[1]
                self.gtopic_dict[gt][filed_name] = is_not_null
            self.sub_producers[gt]=KProducer(bootstrap_servers = self.kafka_conf["host"],topic=gt)
        # Init filter
        self.filter = BloomFilter(key='news')
        # 对于不同的源，采用不同的去重库,目前新闻为news。也可不填，默认为名为bloomfilter的去重库
        self.filter_fields = spider.filter_fields
        for filter_field in self.filter_fields:
            if(filter_field not in self.topic_dict):
                logging.error(f"Error, filter_field: {filter_field} is not in topic:{self.topic_name}")

    def close_spider(self, spider):
        self.main_producer.producer.close()
        for sub_p in self.sub_producers:
            self.sub_producers[sub_p].producer.close()
        pass

    def process_item(self, item, spider):
        # check data protocol of unique topic
        data = item["record"]
        if(len(data)!=len(self.topic_dict)):
            logging.error("Data length doesn't match protocol.")
            return item
        for field in self.topic_dict:
            flag = self.topic_dict[field]
            if(flag==1):
                if(field not in data or data[field]==""):
                    logging.error("Data doesn't match protocol of unique topic. Field:{}".format(field))
                    return item
            else:
                if(field not in data or data[field]==None):
                    data[field]=""
        # filter data by filter_fields
        filter_flag = False
        for filter_field in self.filter_fields:
            if( (filter_field not in data) or data[filter_field]=="" or data[filter_field]==None):
                logging.error(f"Error,filter_filed:{filter_field} can't be None or void str")
                return item
            if(self.filter.isContains(data[filter_field])):
                filter_flag = True
            else:
                self.filter.insert(data[filter_field])
        if(filter_flag):
            logging.info("Filte success")
            return item
        # check data protocol of general topic
        data_general = item["general"]
        for gt in self.gtopic_using:
            dg_one = data_general[gt]
            gtd_one = self.gtopic_dict[gt]
            for dg in dg_one:
                for field in gtd_one:
                    flag = gtd_one[field]
                    if(flag==1):
                        if(field not in dg or dg[field]==""):
                            logging.error("Data doesn't match protocol of general topic. Topic:{},Field:{}".format(gt,field))
                            return item
                    else:
                        if(field not in dg or dg[field]==None):
                            dg[field]=""
        # send data to kafka
        MAX_RERTY=3
        cur_retry=0
        while(cur_retry<MAX_RERTY):
            try:
                for gt in self.gtopic_using:
                    dg_one = data_general[gt]
                    for dg in dg_one:
                        self.sub_producers[gt].sync_producer_one(json.dumps(dg,ensure_ascii=False).encode('utf-8'))
                self.main_producer.sync_producer_one(json.dumps(data,ensure_ascii=False).encode('utf-8'))
                logging.info("Item save success")
                logging.info('save success, ID: {}, url: {}, dt: {}, source: {}'.format(
                    data['ID'], data['url'],
                    data['CreatTime'], data['source']))
                return item
            except errors.KafkaTimeoutError:
                cur_retry+=1
                logging.error("Save error:KafkaTimeoutError,Resending:")
                logging.error(traceback.format_exc())
                if(cur_retry == MAX_RERTY):
                    logging.error("Stop Resending,reach max time")
            except:
                logging.error("Unknow save error")
                logging.error(traceback.format_exc())
        