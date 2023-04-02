import logging

import scrapy
from loguru import logger
from scrapy.http import JsonRequest


class FySpider(scrapy.Spider):
    name = "fy_spider"
    # 网页地址
    site = "https://www.szggzy.com/jygg/list.html",
    post_url = "https://www.szggzy.com/cms/api/v1/trade/content/page"
    post_data = {
        "pageNo": 1,
        "pageSize": 10,
        "tableName": "ZC_WINBIDBULLETINFO",
        "projectType": "Z2",
        "publishTimeStart": "",
        "areaCode": "",
        "title": "",
        "publishTimeEnd": "",
        "mode": "",
    }
    total_page = 2

    def start_requests(self):
        """
        循环获取每一个栏目
        :return:
        """
        for page_no in range(1, self.total_page + 1):
            post_url = self.post_url
            post_data = self.post_data
            post_data["pageNo"] = page_no
            yield JsonRequest(
                url=post_url,
                callback=self.parse,
                dont_filter=True,
                data=post_data,
            )

    def parse(self, response, **kwargs):
        """
        获取list
        :param response:
        :param kwargs:
        :return:
        """
        info_list = response.json()["data"].get("list", [])
        for info in info_list:
            guid = info["guid"]
            info_url = f"https://jyzx.fy.gov.cn/fyggfwpt-api-home-web/menhuJyxx/detail?&guid={guid}"
            info_title = info["title"]
            origin_url = f"https://jyzx.fy.gov.cn/#/newTradeDetail?guid={guid}&nav=%E6%94%BF%E5%BA%9C%E9%87%87%E8%B4%AD&nav=%E4%B8%AD%E6%A0%87%E7%BB%93%E6%9E%9C%E5%85%AC%E5%91%8A&isNewSp=2"
            yield JsonRequest(
                url=info_url,
                callback=self.parse_detail,
                dont_filter=True,
                cb_kwargs={
                    'info_title': info_title,
                    'origin_url': origin_url
                }
            )

    def parse_detail(self, response, **kwargs):
        """
        获取详情页内容
        :param response:
        :param kwargs:
        :return:
        """
        logger.info(kwargs['origin_url'])
        news_content = response.json()["dataList"]["zbjg"][0]["winbidbulletincontent"]
        content = "".join(news_content).strip()
        item = {
            'url': kwargs['origin_url'],
            'title': kwargs['info_title'],
            'content': content,  # 正文太长了就不写进去了 换成content就能看
        }
        logging.info(item)  # 结果放在default.log


