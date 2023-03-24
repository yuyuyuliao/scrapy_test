import re
import bs4
import pymysql
from html import unescape
from urllib import parse
from posixpath import normpath


def subword(word) -> str:
    word = re.sub('\t', ' ', word)
    word = re.sub('\n', ' ', word)
    word = re.sub('\n', ' ', word)
    word = re.sub(r'[\ue004\xa0\u3000\xad\u003c\u003e\u0001\u2002\u2003\u0001\uf096\x96\u200c\x7f\u200f\u202a\u202b]', ' ', word)
    word = re.sub(r'[\u202c\u2028\ue010\ufeff\ue862\ue861\u200d\u200b\u2005\u2006\u2009\u202f\ue5e5\u200e\ue004\uf0b2]', ' ', word)
    word = re.sub(r'[\uf06c\ue3b8\ue0be\ue0cf\ue0cc\ue0f0\ue19b\ue0bc\uf06e\uf0b7\ue004\uf0a7\uf09f\ue3bc\ue003\ue3b2]', ' ', word)
    word = re.sub('&nbsp;', ' ', word)
    word = re.sub('&nbsp', ' ', word)
    word = re.sub('<span .*?>', '', word)
    word = re.sub('</span>', '', word)
    word = re.sub('<strong>', '', word)
    word = re.sub('</strong>', '', word)
    word = re.sub('<br/>', '', word)
    word = re.sub('</br>', '', word)
    word = re.sub('</font>', '', word)
    word = re.sub('<font>', '', word)
    word = re.sub('\\\\', '', word)
    word = re.sub(r'\\', '', word)
    word = re.sub(r"[\"\']+", '“', word)
    word = re.sub(r"[\"\']+", '‘', word)
    word = word.replace('\\', '')
    word = word.strip()
    return word


def special_character_filter(filter_str) -> str:
    """
    过滤特殊字符
    :return:str
    """
    try:
        filter_str = str(filter_str)
        filter_str = unescape(filter_str)
        if '"' in filter_str:
            filter_str = re.sub(r"[\"\']+", '“', filter_str)
        elif "'" in filter_str:
            filter_str = re.sub(r"[\"\']+", '‘', filter_str)

        filter_str = re.sub(r"[\n\t\r]+", " ", filter_str)
        filter_str = re.sub(
            r"[\xa0\u3000\u200b\u0001\u2002\u2003\u0001\uf096\x96\u200d\ue010\ufeff\ue862\ue861\u2005\u200f\u2028\u202c\u202b\u202a\u200b\u2006\u2009\u202f\ue5e5\uf075\u200e\ue004\uf0b2\uf06c\ue3b2\ue3b8\ue0be\ue004]",
            " ", filter_str).replace("&nbsp;", " ")
        return filter_str
    except Exception:
        print("过滤失败")


def url_fix(response_url: str, detail_url: str) -> str:
    """
    根据请求url拼接不完整的url
    :param response_url: 请求url
    :param detail_url: 需要拼接的url
    :return: 拼接完成的url
    """
    url1 = parse.urljoin(response_url, detail_url)
    arr = parse.urlparse(url1)
    path = normpath(arr[2])
    return parse.urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))


def replace_char_entity(html_str: str) -> str:
    """
    替换常用HTML字符实体.
    使用正常的字符替换HTML中特殊的字符实体.
    你可以添加新的实体字符到char_entities中,处理更多HTML字符实体.
    :param html_str:html字符串
    :return:过滤后的字符串
    """
    char_entities = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"', }

    re_char_entity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_char_entity.search(html_str)
    while sz:
        # entity = sz.group()  # entity全称，如>
        key = sz.group('name')  # 去除&;后entity,如>为gt
        try:
            html_str = re_char_entity.sub(char_entities[key], html_str, 1)
            sz = re_char_entity.search(html_str)
        except KeyError:
            # 以空串代替
            html_str = re_char_entity.sub('', html_str, 1)
            sz = re_char_entity.search(html_str)
    return html_str


def filter_tags(html_str: str) -> str:
    """
    过滤HTML中的标签,将HTML中标签等信息去掉
    :param html_str: html字符串
    :return: 过滤后的字符串
    """
    # 先过滤CDATA
    re_cdata = re.compile(r'//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    # re_script = re.compile(r'<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile(r'<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile(r'<br\s*?/?>')  # 处理换行
    re_h = re.compile(r'</?\w+[^>]*>')  # HTML标签
    # re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', html_str)  # 去掉CDATA
    # s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    # s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    s = replace_char_entity(s)  # 替换实体
    return s


def content_remove_duplicates(content_list: list) -> list:
    """
    正文去重
    :param content_list: 原正文列表
    :return: 过滤后正文列表
    """
    new_content_list = list()
    for content_dict in content_list:
        if content_dict not in new_content_list:
            new_content_list.append(content_dict)
    return new_content_list


def save_original_link(record, response, content_list):
    """
    保存原始链接
    :param record: 单个新闻json
    :param response: 新闻html
    :param content_list: 新闻正文
    :return: proto_list, record, content_list
    """
    proto_list = list()
    # 保存封面
    if record['imgUrl']:
        img = record['imgUrl']
        img = url_fix(response.url, img)
        # 保存原链
        record['imgUrl'] = img
        proto_dict = {"type": "cover_img", "content": img}
        proto_list.append(proto_dict)

    # 保存图片原链
    for c_dict in content_list:
        if c_dict.get("type") == "image":
            # 判断图片是否加前缀路径
            img = c_dict.get('content')
            img = url_fix(response.url, img)
            # 保存原链
            c_dict['content'] = img
            proto_dict = {"type": "image", "content": img}
            proto_list.append(proto_dict)
        elif c_dict.get("type") == "video":
            # 判断视频是否加前缀路径
            video = c_dict.get('content')
            video = url_fix(response.url, video)
            # 保存原链
            c_dict['content'] = video

    return proto_list, record, content_list


def get_type_list(platfrom_name):
    conn = pymysql.connect(host="data-mysql-master.zxkw-local.com",
                           user='test',
                           password='test1234',
                           port=3306,
                           database='brand')
    cur = conn.cursor()
    sql = 'select platform_type_id,platform_type,type_url,platform_id,platform_name,second_platform_type,third_platform_type from cultural_industry_type where platform_name=\"{}\"'.format(
        platfrom_name)
    cur.execute(sql)
    res = cur.fetchall()
    cur.close()
    conn.close()
    type_list = list()
    # stair_type_dict = dict()
    if res:
        for i in res:
            if i[2]:
                type_list.append({
                    "platform_type_id": i[0],
                    "platform_type": i[1],
                    "type_url": i[2],
                    "second_platform_type": i[5] if i[5] else "",
                    "third_platform_type": i[6] if i[6] else "",
                    "platform_id": i[3],
                    "platform_name": i[4],
                })
        # print(type_list)
        # for type_dict in type_list:
        #     if not (type_dict.get('second_platform_type') and type_dict.get('second_platform_type')):
        #         stair_type_dict[type_dict.get('platform_type')] = type_dict.get("platform_type_id")
    return type_list


def get_content(response) -> list:
    """获取新闻详情"""
    # print(response.text)
    word_list = list()  # 预存放待去重文本
    dom = bs4.BeautifulSoup(response, 'html.parser')
    [s.extract() for s in dom('style')]
    # 去除注释
    comments = dom.findAll(text=lambda text: isinstance(text, bs4.Comment))
    [comment.extract() for comment in comments]
    content_list = list()
    for i in dom.descendants:
        if isinstance(i, bs4.element.Tag):
            # print(i.descendants)
            if isinstance(i, bs4.Comment):
                continue
            for node in i.descendants:
                if node.name == 'style':
                    continue

                elif node.name == 'img':
                    if node.get('src') or node.get("data-src"):
                        img_url = node.get('src')
                        if not img_url:
                            img_url = node.get('data-src')
                        img_start_url = img_url
                        if 'data:image' not in img_start_url:
                            img = {"type": "image",
                                   "content": img_start_url}
                            content_list.append(img)

                elif node.name == 'video':
                    video_url = node.get('src')
                    if (video_url == "") or (video_url == "null") or (video_url is None):
                        continue
                    video = {
                        "type": "video",
                        "content": video_url
                    }
                    content_list.append(video)

                elif node.string:
                    if (node.string == '') or (node.string == '\n') or (node.string == '\xa0') or (node.string == 'None') or (
                            node.string == '&nbsp;') or ('window.' in node.string) or ('function' in node.string) or (
                            'options' in node.string) or ('TRS_Editor' in node.string) or ('var' in node.string):
                        continue
                    str1 = special_character_filter(node.string)
                    # 过滤
                    str1 = filter_tags(str1)
                    str1 = subword(str1)
                    if str1:
                        text = {"type": "text",
                                "content": unescape(str1.strip())}
                        content_list.append(text)
    new_content_list = list()
    for i in content_list:
        if i not in new_content_list:
            new_content_list.append(i)
    # print(new_content_list)
    return new_content_list


def content_remove_duplicates_long(word_list):
    """
    有些网站文本有一个隐藏的大段包含所有段落
    该方法通过找出最长语句后用其他文本在列表中遍历 如果相同数大于1 则删去最长行
    :return:
    """
    while True:
        word_sum = 0
        word_max = max(word_list, key=len, default="")  # 获取最长的一段

        for i in word_list:
            if i in word_max:
                word_sum = word_sum + 1
        if word_sum > 1:
            word_list.remove(word_max)
        else:
            break
    word_list_use = list(set(word_list))
    word_list_use.sort(key=word_list.index)
    return (word_list_use)


def get_content_xjh(souppage, label_list) -> list:
    """
    BS4 方法 不到万不得已不用

    """
    content = list()
    word_list = list()  # 预存放待去重文本
    # 文章
    label = ''
    for label_n in label_list:
        if label_n:
            if souppage.select(label_n):  # 切换标签id
                label = label_n + ' '
                break
    content_text = souppage.select(label + 'p')
    # print(content_text, len(content_text))
    if len(content_text) == 0:
        content_text = souppage.select(label + 'span')
    if content_text:
        for word_n in content_text:
            if len(word_n) > 0:
                word_needs = word_n.text.strip()
                word_need_list = word_needs.split('\n')
                for word_need in word_need_list:
                    if len(word_need) > 0:
                        word_need = subword(word_need)
                        word_need = filter_tags(word_need)
                        word_list.append(word_need)
        '''长去重'''
        text_list = content_remove_duplicates_long(word_list)  # 长去重
        for text_n in text_list:
            if text_n:
                text = {"type": "text",
                        "content": text_n}
                content.append(text)
    # 图片
    content_img = souppage.select(label + ' img')
    if content_img:
        for img_n in content_img:
            img_need = img_n['src']
            if len(img_need) > 0:
                content_dict = {}
                content_dict['type'] = 'image'
                content_dict['content'] = img_need
                content.append(content_dict)
    content_video = souppage.select(label + ' video')
    if content_video:
        for video_n in content_video:
            video_need = video_n['src']
            if len(video_need) > 0:
                content_dict = {}
                content_dict['type'] = 'video'
                content_dict['content'] = video_need
                content.append(content_dict)
    new_content_list = list()
    for i in content:
        if i not in new_content_list:
            new_content_list.append(i)
    # print(new_content_list)
    return new_content_list
