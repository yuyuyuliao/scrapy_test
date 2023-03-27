import scrapy
import logging

from loguru import logger


def gen_next_page_url(page_no, list_url):
    use_url = list_url.replace(".html", "")
    if page_no == 1:
        res_url = list_url
    else:
        res_url = use_url + f"_{page_no}.html"
    return res_url


class MzlggzySpider(scrapy.Spider):
    name = 'mzlggzy_spider'  # 爬虫名
    start_urls = 'https://www.whb.cn/zhuzhan/sz/index.html'  # 需要爬取的列表页
    total_page = 5
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.51',
    }

    def start_requests(self):
        """
        循环获取每一个栏目
        :return:
        """
        for page_no in range(1, self.total_page + 1):
            list_url = self.start_urls
            # 该方法需要手动修改
            res_url = gen_next_page_url(page_no, list_url)
            yield scrapy.Request(
                url=res_url,
                dont_filter=True,
                callback=self.parse,
                headers=self.headers
            )

    def parse(self, response, **kwargs):
        """
        获取list
        :param response:
        :param kwargs:
        :return:
        """
        info_list = response.xpath("//div[@class='info_body']/div[1]/a")
        for info in info_list:
            info_url = info.xpath("./@href").get('')
            info_url = response.urljoin(info_url)
            info_title = info.xpath("./text()").get('')
            yield scrapy.Request(
                url=info_url,
                dont_filter=True,
                callback=self.parse_detail,
                cb_kwargs={'info_title': info_title},
                headers=self.headers
            )

    def parse_detail(self, response, **kwargs):
        """
        获取详情页内容
        :param response:
        :param kwargs:
        :return:
        """
        logger.info(response.url)
        news_content = response.xpath('//div[@class="content_info"]/*').extract()
        content = "".join(news_content).strip()
        # attachments = []
        # download_urls = response.xpath("//div[@class='detail-con1']/a")
        # for download_url in download_urls:
        #     file_url = download_url.xpath("./@href").get()
        #     attachments.append(
        #         {
        #             "url": response.urljoin(file_url),
        #             "filename": "",
        #         }
        #     )
        item = {
            'url': response.url,
            'title': kwargs['info_title'],
            'content': '正文',  # 正文太长了就不写进去了 换成content就能看
            # 'attachments_ary': attachments
        }
        logging.info(item)  # 结果放在default.log
