#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-10-15 18:37:46
# Project: soufanghuang

from pyspider.libs.base_handler import *
from bs4 import  BeautifulSoup
from help import dictone
import json
from pymongo import MongoClient


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=24 * 60)
    def on_start(self):
        url ='http://cd.sofang.com/ajax/map/search?keyword=&totalprice=&areas=&houseRoom=&faceTo=&housetype1=3&housetype2=&type=sale&zm=12&swlng=103.802887&swlat=30.532487&nelng=104.33296&nelat=30.827171&year=0&floor=0&decorate=0&buildtype=0&structure=0&peitao=&tags=&pg=1&sort=desc'
        self.crawl(url, callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        f_data = json.loads(response.text)
        fA_data = map(lambda x:dict(x,**f_data[u'lefthead']),f_data[u'r']) 
        
        #添加buff ---fir
        fir_data =[] 
        for  Dict in  fA_data:
            keyList=[]
            for i in Dict.keys():
                if i[0:2]=='fir':
                    keyList.append(i)
                else:
                    keyList.append(u'fir'+i)

            fir_data.append(dict(zip(keyList,Dict.values()))) 
        for Dict in fir_data:
            url ='http://cd.sofang.com/ajax/map/getbusinessarea?type=sale&sort=desc&caid=%s&housetype1=3&housetype2=&keyword=&totalprice=&areas=&houseRoom=&faceTo='
            self.crawl(url%Dict['firid'], callback=self.index_page2,save={'a':Dict})
            
    @config(age=10 * 24 * 60 * 60)        
    def index_page2(self, response):
        s_data = json.loads(response.text) 
        temp =dict(s_data['lefthead'],**{'timeLocUpdate':s_data['cdate']['timeLocUpdate']})
        SA_data =map(lambda x:dict(x,**temp),s_data['hdate'][u'r'])
        
        #添加buff ---sec
        sec_data =[]         
        for  Dict in  SA_data:
            keyList=[]
            for i in Dict.keys():
                if i[0:2]=='sec':
                    keyList.append(i)
                else:
                    keyList.append(u'sec'+i)

            sec_data.append(dict(zip(keyList,Dict.values())))         

        firsend =  response.save['a']   

        Sec_data = map(lambda x:dict(x,**firsend),sec_data)
        
        #下一层调用
        for Dict in Sec_data:
            url ='http://cd.sofang.com/ajax/map/getcommunity?type=sale&sort=desc&bid=%s&housetype1=3&housetype2=&keyword=&totalprice=&areas=&houseRoom=&faceTo=' 
            self.crawl(url%Dict['secid'], callback=self.index_page3,save={'a':Dict})

    @config(age=10 * 24 * 60 * 60)       
    def index_page3(self, response):
        tire_data = json.loads(response.text) 
        temp =dict(tire_data['lefthead'],**{'timeLocUpdate':tire_data['cdate']['timeLocUpdate']})
        ThrA_data =map(lambda x:dict(x,**temp),tire_data['hdate'][u'r'])  
        
         #添加buff ---sec
        thr_data =[]         
        for  Dict in  ThrA_data:
            keyList=[]
            for i in Dict.keys():
                if i[0:2]=='thr':
                    keyList.append(i)
                else:
                    keyList.append(u'thr'+i)

            thr_data.append(dict(zip(keyList,Dict.values())))         

        thrsend =  response.save['a']   
        Thr_data = map(lambda x:dict(x,**thrsend),thr_data)  
        
        
        url ='http://cd.sofang.com/ajax/map/gethouse?type=sale&sort=desc&housetype1=3&housetype2=&communityid=%s&keyword=&totalprice=&areas=&houseRoom=&faceTo=&page=%d'

        #下一层调用
        for Dict in Thr_data:

             page  = (int(Dict['thrcount'])+6)/6
             for pg in range(1,page+1):
                print pg
                
                Dict['thrpage'] =pg

                self.crawl(url%(Dict['thrid'],pg), callback=self.index_page4,save={'a':Dict})        

    @config(age=10 * 24 * 60 * 60)                
    def index_page4(self, response):
        foure_data = json.loads(response.text)
        print foure_data
        try:
            temp = foure_data['hdate']['hits']['hits'][0]
        except:
            temp =None
          
        #数据清洗(获取扁平化dict)
        if temp != None:
            dList =  dictone(temp) #获取dict组 
        
            four_data= reduce(lambda x,y:dict(x,**y),dList)              
            #给数据加buff
            Four_data ={} 
            keyList =[]
            for i in four_data.keys():
                if i[0:2]=='fou':
                    keyList.append(i)
                else:
                    keyList.append(u'fou'+i)
            Four_data = dict(zip(keyList,four_data.values()))
            foursend  = response.save['a']
            Four_data = dict(Four_data,**foursend)
            url  ='http://cd.sofang.com/housedetail/ss%s.html'
        

            self.crawl(url%(Four_data['fou_id']), callback=self.detail_page,save={'a':Four_data})              


        
    @config(priority=2)
    def detail_page(self, response):
        fisoup= BeautifulSoup(response.text,"lxml") 

        '''
        房子系统信息
        '''
        fihouseSys ={}

        fihousetag = fisoup.find('div',{'class':'house_name'})
        #housename
        fihousename = fihousetag.find('h2').text
        fihouseSys['fihousename'] =fihousename

        #更新时间
        fihouseupdate    = fihousetag.find('span',{'class':'id'}).find('span').text.split('\n')[1].strip()
        fihouseSys['fihouseupdate'] =fihouseupdate
        #房源ID
        fihouseid   = fihousetag.find('span',{'class':'id'}).text.split(' ')[0].split(u'：')[1]
        fihouseSys['fihouseid'] =fihouseid

        '''
        房子价格信息
        '''
        fihousepriceA={}
        fimianmsg      = fisoup.find('div',{'class':'house_msg'})
        #万/套
        fihousepirceAll   = fimianmsg.find('p',{'class':'house_price'}).find('span',{'class':'font_size'}).text
        fihousepriceA['fihousepirceAll'] =fihousepirceAll

        fihousepircesg = fimianmsg.find('div',{'class':'house_price house_price2'})

        #元/平
        fihousepriceSq = fihousepircesg.findAll('span','sale_price')[0].find('span',{'class':'font_words'}).text
        fihousepriceA['fihousepriceSq'] = fihousepriceSq
        #参考首付
        fippercent     = fihousepircesg.find('div',{'class':'jsq-yg'}).find('option',{'selected':'selected'}).attrs['value']
        fihousepriceFp = int(fihousepirceAll)*(1-int(fippercent)/10.0)
        fihousepriceA['fihousepriceFp'] = fihousepriceFp

        '''
        #房子基本信息
        '''
        #info_nav -房型,建筑面积，朝向，所在楼层,装修，物业类型，小区名，小区地址
        fihouseinfo = fimianmsg.findAll('ul',{'class':'info_nav'})
        fihouseinfoLst =  []
        for i in fihouseinfo:
            fihouseinfoLst.extend(i.findAll('li'))    
        fihousebase=map(lambda x:x.find('span').text.strip(),fihouseinfoLst[:-1])
        fihousebasenmlst =[u'房型',u'建筑面积',u'朝向',u'所在楼层',u'装修程度',u'物业类型']
        fihousebase = dict(zip(fihousebasenmlst,fihousebase)) 
        #小区名字
        fihousebase[u'bulid_name'] = fihouseinfoLst[-1].find('a').text
        #小区地址
        fihousebase[u'adress'] = fihouseinfoLst[-1].find('span',{'class':'address'}).text

        '''
        #房子销售员信息
        '''
        fihousesale={}
        #contact销售联系信息
        fihousesale['phone']= fimianmsg.find('span',{'class':'phone_c'}).text.strip()
        fihousesale['saler']= fimianmsg.find('dl',{'class':'broker_info'}).find('p',{'class':'p1'}).text
        fihousesale['workwide'] = filter(lambda x:x!='',fimianmsg.find('dl',{'class':'broker_info'}).find('p',{'class':'p2'}).text.split('\n'))

        finfoList = [fihousepriceA,fihousepriceA,fihousebase,fihousesale]
        finfoDict = reduce(lambda x,y:dict(x,**y),finfoList) 

        '''
        统一第五个页面的加fi buff
        '''
        keyList =[]
        for i in finfoDict.keys():
            if i[0:2]=='fi':
                keyList.append(i)
            else:
                keyList.append(u'fi'+i)

        finfoDict =dict(zip(keyList,finfoDict.values())) 
        
        
        '''
        统一合并数据
        '''
        fi_data={}
        fisend  = response.save['a']
        try:
            fi_data = dict(finfoDict,**fisend)        
        except:
            fi_data = fisend

        
        return fi_data
       
    def on_result(self, result):
        if (result!={})&(result!=None):
            connl = MongoClient(host = '192.168.0.10', port = 27017)
            dbl = connl.fes
            dbl.soufangwang.insert_one(result)
            connl.close()
        

