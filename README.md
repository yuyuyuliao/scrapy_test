# scrapy_test  
scrapy_easy  
scrapy简单入门教学  
目前仅针对实例操作抓取的教学  

前期准备
安装scrapy
pip install scrapy / pycharm包管理工具安装  

创建爬虫项目   
scrapy startproject xxx  
cd xxx  

创建爬虫文件 系统模板没啥用 可以自定义   
scrapy genspider example example.com  

执行爬虫文件  
entrypoint为执行程序，代码中输入爬虫名字执行  

教学案例  
教学案例没有数据库  作为简单入门教学 从理解如何使用scrapy抓取网页开始    
easy_scrapy为教学案例 框架为scrapy自主生成没有任何修改  
fy_spider为接口案例  
mzlggzy_spider为html案例  
items middlewares pipelines settings stool各有功能  
items 用于定义抓取结果 需要存放的字段  
middleware 中间件可以用来设置例如请求重试，请求加代理等功能   
pipelines 用作数据库传输  
stool 链接数据库  
settings  用于对爬虫任务做一些设置  例如 速率,执行目录,管道开关,中间件使用,日志存放等  



scrapy 特点：  


scrapy 运行流程：  


scrapy 案例:    

https://www.whb.cn/zhuzhan/sz/index.html 5页 url title content  
https://www.szggzy.com/jygg/list.html 2页 url title content 


