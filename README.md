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

git教学  
git作为代码仓库可以被很多人一起维护 通过推送各自分支由拥有master权限的人代码进行合并    
main分支为主要分支，作为线上分支，本地不能也不建议去修改main分支。    
想要对代码进行修改需要先从main分支新建一个分支再对内容进行修改后push。     
从main新建分支的操作相当于是对main分支的代码进行一次复制，对分支的修改不会影响其他分支  
因此建议每个任务的启动都从main分支中新建一个分支操作，保证代码的可追溯性。  
当分支合并上线后，就需要对本地main分支进行更新，如果不及时更新，下次推送分支的时候可能就会遇到冲突，    
此时就需要进行变基  

切记 切记 切记 不要修改main分支！！！！！！！！！！！

git 命令
git clone  克隆代码  
git commit 提交代码  
git push 推送代码  
维护者 @yuyuyuliao @momoluo 


scrapy 特点：  


scrapy 运行流程：  


scrapy 案例:    

https://www.whb.cn/zhuzhan/sz/index.html 5页 url title content  
https://www.szggzy.com/jygg/list.html 2页 url title content 


