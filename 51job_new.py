#coding=utf-8
#author:ok0755@126.com
#python3.7

from fake_useragent import UserAgent
import csv
import urllib
from time import sleep
import os
import requests
from lxml import etree

import gevent
from gevent import monkey, pool
monkey.patch_socket()
#-------------------------------------------------------------------------------------------------------------------------------------------------
""" 返回分页地址 """
def get_pages(keys,page):
    for i in range(1,page+1):
        url='http://search.51job.com/list/040000,000000,0000,00,9,99,{},2,{}.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=1&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='.format(keys,i)
        yield url
#-------------------------------------------------------------------------------------------------------------------------------------------------
""" 返回详细页链接 """
def get_links(link,num_retry=2):
    sleep(2)
    header={'User-Agent':ua.random}
    try:
        response=requests.get(link,headers=header,timeout=10)
        txt = response.text
        response.close()
        html=etree.HTML(txt)
        result=html.xpath('//a[@onmousedown=""]/@href')
        return result
    except:
        if num_retry > 0:
            sleep(1)
            return get_links(link,num_retry-1)
#-------------------------------------------------------------------------------------------------------------------------------------------------
""" 写入表格 """
def write_excel(url,num_retry=2):
    try:
        header={'User-Agent':ua.random}
        req=requests.get(url,headers=header,timeout=3)
        req.encoding='gbk'
        respon=req.text.replace('&nbsp;','')
        req.close()
        selector=etree.HTML(respon)
        selectors=selector.xpath('.//div[@class="tCompany_center clearfix"]')[0]

        job_company=selectors.xpath('.//a[@class="catn"]/@title')                               ## 公司名称
        job_name=selectors.xpath('.//h1/@title')                                                ## 职位
        job_date=selectors.xpath('normalize-space(.//p[@class="msg ltype"]/@title)')            ## 经验
        job_salary=selectors.xpath('.//div[@class="cn"]//strong/text()')                        ## 薪水
        job_area=selectors.xpath('normalize-space(.//div[@class="bmsg inbox"]/p)')              ## 工作地址
        job_area = job_area.replace(u'上班地址：','')
        job_detail = selectors.xpath('normalize-space(.//div[@class="bmsg job_msg inbox"])')    ## 职责

        job_company = job_company[0] if len(job_company)>0 else 'Null'
        job_name = job_name[0] if len(job_name)>0 else 'Null'
        ##job_date = job_date.replace(' ','')
        job_salary = job_salary[0] if len(job_salary)>0 else 'Null'

        arr = [job_company,job_name,job_date,job_salary,job_area,job_detail]
        with open(r'd:\job.csv','a+',newline='') as f:
            wr = csv.writer(f)
            wr.writerow(arr)
        print('reading...',url)
    except:
        sleep(2)
        if num_retry > 0:
            return write_excel(url,num_retry-1)
        elif num_retry == 0:
            print('出错:%s'%url)
#-------------------------------------------------------------------------------------------------------------------------------------------------
## 删除原文件
def del_old_csv():
    path = r'd:\job.csv'
    if os.path.isfile(path):
        os.remove(path)

#-------------------------------------------------------------------------------------------------------------------------------------------------
""" 主入口 """
def main():
    del_old_csv()
    keys=input(u'关键词,结尾页码:').split(',')
    quote_string = urllib.parse.quote(keys[0])
    p = pool.Pool(30)
    ua = UserAgent()
    for urls in get_pages(quote_string,int(keys[1])):
        lists = get_links(urls)
        th = []
        for l in lists:
            th.append(p.spawn(write_excel,l))
        gevent.joinall(th)

if __name__=='__main__':
    main()