# -*- coding: utf-8 -*-
##################################################################################
# Paso 1 : crear environment
# https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
#
# conda create --name webmining_env
# 
# webmining_env
# conda activate webmining_env
# 
# pip install -r requirements.txt

# pip list --outdated # Output a list of outdated packages 
# pip install -U PackageName  # It is also possible to upgrade everything

# Documentation
# https://docs.scrapy.org/en/latest/topics/spiders.html
# https://www.youtube.com/watch?v=h-M0XHOj6k8


# TOPICOS A CLASIFICAR:
# “el mundo” “economía” y “sociedad” y “el país”.

# from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from urllib.parse import urlparse
from scrapy.utils.response import open_in_browser
from scrapy.crawler import CrawlerProcess
from urllib import parse
from os import path
from scrapy.http.response.html import HtmlResponse
import re
import pandas as pd
import time

class NewsSpider(CrawlSpider):

    name = 'crawler_pagina12'

    item_count=0

    # solo descargar paginas desde estos dominios
    allowed_domains = ('www.pagina12.com.ar','pagina12.com.ar')
    
    rules = {
        # solo bajar las paginas cuya url incluye "/secciones", pero no aquellas cuya url include "/catamarca12" o "/dialogo"
        # normaliza las urls para no descargarlas 2 veces la misma pagina con distinta url.
        Rule(LinkExtractor(allow=(),restrict_xpaths=('//h1[@class="title-list"]')),callback='parse_response', follow=False), # Preguntar por follow
        Rule(LinkExtractor(allow=(),restrict_xpaths=('//h3[@class="title-list"]')),callback='parse_response', follow=False),  
        Rule(LinkExtractor(allow=(),restrict_xpaths=('//h2')),callback='parse_response', follow=False)
        # Rule(LinkExtractor(allow=(),restrict_xpaths=('//a[@class="next"]')))                                   
    }
     
    # configuracion de scrappy, ver https://docs.scrapy.org/en/latest/topics/settings.html
    custom_settings = {
      # mentir el user agent
     'USER_AGENT': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
     'LOG_ENABLED': True,
     'LOG_LEVEL': 'INFO',
      # no descargar mas de 1 link desde la pagina de origen
     'DEPTH_LIMIT': 2,
      # ignorar robots.txt (que feo)
     'ROBOTSTXT_OBEY': False,
     # esperar entre 0.5*DOWNLOAD_DELAY y 1.5*DOWNLOAD_DELAY segundo entre descargas
     'DOWNLOAD_DELAY': 1,
     'RANDOMIZE_DOWNLOAD_DELAY': True
    }

    def __init__(self, save_pages_in_dir='.',topic='', *args, **kwargs):
          super().__init__(*args, **kwargs)
          # guardar el directorio en donde vamos a descargar las paginas
          self.basedir = save_pages_in_dir
          self.topic = topic
    
    def parse_response(self, response:HtmlResponse):
          """
          Este metodo es llamado por cada url que descarga Scrappy.
          response.url contiene la url de la pagina,
          response.body contiene los bytes del contenido de la pagina.
          """
          # el nombre de archivo es lo que esta luego de la ultima "/"
          print(type(response))

          contenido_raw=response.xpath('//main[@class="article-text"]/p/text()').extract()
          contenido_array=re.findall('[a-zA-ZáéíóúñÁÉÍÓÚÜü\s]',str(contenido_raw))
          
          contenido=''.join(contenido_array)
          fecha=response.xpath('//time/text()').extract()[0]
          autor_array=response.xpath('//div[@class="author-detailed"]/a/span/strong').extract()
          autor=autor_array[0] if len(autor_array)>0 else ''
          id=re.findall('\d{6}', response.url)[0]
          topico=self.topic
          filepath=self.basedir+'\\dataset_'+topico+'.csv'

          print(contenido[0:30]+'\n')
          print(fecha)
          print(autor)
          print('\n')
          print(id)
    
          df=pd.DataFrame({'id':id, 
                           'fecha': fecha,
                           'nota':contenido,
                           'topico':topico,
                           'autor':autor,
                           'url':response.url
                           },index=[0])
          df.to_csv(filepath,sep='|', mode='a', index=False, header=False,encoding='UTF-8')
          
        #   print(type(response))
        #   html_filename = path.join(self.basedir,parse.quote(response.url[response.url.rfind("/")+1:]))
        #   if not html_filename.endswith(".html"):
        #       html_filename+=".html"
        #   print("URL:",response.url, "Pagina guardada en:", html_filename)
        #   with open(html_filename, "wt") as html_file:
        #       html_file.write(response.body.decode("utf-8"))
          

if __name__ == "__main__": 
    
    stat_url_list_el_pais=[]
    stat_url_list_el_mundo=[]
    stat_url_list_el_econocmia=[]
    stat_url_list_el_sociedad=[]

    for page in range(0,2000): #4520
        if page==0:
            stat_url_list_el_pais.append('https://www.pagina12.com.ar/secciones/el-pais')
            stat_url_list_el_mundo.append('https://www.pagina12.com.ar/secciones/el-mundo')
            stat_url_list_el_econocmia.append('https://www.pagina12.com.ar/secciones/economia')
            stat_url_list_el_sociedad.append('https://www.pagina12.com.ar/secciones/sociedad')
        else:  
            stat_url_list_el_pais.append('https://www.pagina12.com.ar/secciones/el-pais?page='+str(page) )
            stat_url_list_el_mundo.append('https://www.pagina12.com.ar/secciones/el-mundo?page='+str(page))
            stat_url_list_el_econocmia.append('https://www.pagina12.com.ar/secciones/economia?page='+str(page))
            stat_url_list_el_sociedad.append('https://www.pagina12.com.ar/secciones/sociedad?page='+str(page))
        
    process  = CrawlerProcess()
    # for topic in ['el-pais','el-mundo']:#,'el-pais','el-mundo','economia','sociedad'  
    process.crawl(NewsSpider, save_pages_in_dir='./download',topic='el-pais', start_urls = stat_url_list_el_pais) 
    process.crawl(NewsSpider, save_pages_in_dir='./download',topic='el-mundo', start_urls = stat_url_list_el_mundo) 
    process.crawl(NewsSpider, save_pages_in_dir='./download',topic='economia', start_urls = stat_url_list_el_econocmia) 
    process.crawl(NewsSpider, save_pages_in_dir='./download',topic='sociedad', start_urls = stat_url_list_el_sociedad) 

    process.start()
           
