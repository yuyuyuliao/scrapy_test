import logging

import scrapy
from loguru import logger
from scrapy.http import JsonRequest


class FySpider(scrapy.Spider):
    name = "szggzy_spider"
    # 网页地址
    site = "https://jyzx.fy.gov.cn/#/tradeInfo?collapseInd=1&currentGGInd=0&currentPInd=0&currentYm=1&num=1&publishTimeStart=&publishTimeEnd=",
    post_url = "https://www.szggzy.com/jygg/list.html"
    post_data = {"modelId":1378,"channelId":2850,"fields":[],"title":None,"releaseTimeBegin":None,"releaseTimeEnd":None,"page":0,"size":10}
    total_page = 2

    def start_requests(self):
        """
        循环获取每一个栏目
        :return:
        """
        for page_no in range(1, self.total_page + 1):
            post_url = self.post_url
            post_data = self.post_data
            post_data["page"] = page_no
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
        info_list = response.json()["data"].get("content", [])
        for info in info_list:
            contentid = info["contentid"]
            info_url = f"https://www.szggzy.com/jygg/details.html?contentId={contentid}"
            info_title = info["title"]
            origin_url = f"https://www.szggzy.com/jygg/details.html?contentId={contentid}"
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
        news_content = response.xpath('//tbody')
        content = "".join(news_content).strip()
        item = {
            'url': kwargs['origin_url'],
            'title': kwargs['info_title'],
            'content': content,  # 正文太长了就不写进去了 换成content就能看
        }
        logging.info(item)  # 结果放在default.log
