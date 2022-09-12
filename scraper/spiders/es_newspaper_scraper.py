import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import pymongo

import re
from datetime import datetime

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = myclient['Newspapers']
today = datetime.now().strftime('%d-%m-%Y')
country = 'Spain'
language = 'Spanish'

months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio',
          'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']

class ElMundoSpider(scrapy.Spider):
    name = 'ElMundoSpider'
    start_urls = ['https://www.elmundo.es/']
    allowed_domains = ['elmundo.es']

    
    def parse(self, response):
        news_links = response.xpath('//header/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.elmundo.es'+link
                yield response.follow(url=url, callback=self.parse_links)
            
    def parse_links(self, response):
        title = response.xpath('//h1[@class="headline-text"]/text()').get()
        epigraph = response.xpath('//p[@class="ue-c-article__standfirst"]/text()').get()
        date = response.xpath('//time/@datetime').get()
        paragraph = ''.join(response.xpath('//div[@data-section="articleBody"]/p/text()').getall())
        
        # data pipeline
        try:
            date_temp = date[:10]
            date = datetime.datetime.strptime(date_temp, '%Y-%m-%d').strftime('%d-%m-%Y')
        except:
            date = 'no_date'
        
        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'El ElMundo',
            'language' : language
        }

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class EldiarioSpider(scrapy.Spider):
    name = 'EldiarioSpider'
    start_urls = ['https://www.eldiario.es/']
    allowed_domains = ['eldiario.es']

    def parse(self, response):
        news_links = response.xpath('//h2/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.eldiario.es'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2/text()').get()
        date = response.xpath('//time/@datetime').get()
        paragraph = ''.join(response.xpath('//p[@class="article-text"]/text()').getall())
        
        # data pipeline
        try:
            date_temp = date[:10]
            date = datetime.datetime.strptime(date_temp, '%Y-%m-%d').strftime('%d-%m-%Y')
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'El Diario',
            'language' : language
        }

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)
            
class ElEspañolSpider(scrapy.Spider):
    name = 'ElEspañolSpider'
    start_urls = ['https://www.elespanol.com/']
    allowed_domains = ['elespanol.com']

    def parse(self, response):
        news_links = response.xpath('//h2/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.elespanol.com'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):       
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2/text()').get()
        date = response.xpath('//span[@class="article-header__time-date article-header__time--zonan-date"]/text()').get()
        paragraph = ''.join(response.xpath('//p[contains(@id, "paragraph")]/text()').getall())
        
        # data pipeline
        try:
            date_temp = re.search('\d\d.+\d\d\d\d', date_temp).group(0)
            date_temp = re.sub('de', '', date_temp)
            month = re.search('\D+', date_temp).group(0).strip(',')
            if month in months:
                month = str(months.index(month)+1)
            day = re.search('\d\d', date_temp).group(0)
            year = re.search('\d\d\d\d', date_temp).group(0).strip()
            date = day+'-'+month+'-'+year  
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'ElEspañol',
            'language' : language
        }               

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class LaRazonSpider(scrapy.Spider):
    name = 'LaRazonSpider'
    start_urls = ['https://www.larazon.es/']
    allowed_domains = ['larazon.es']

    def parse(self, response):
        news_links = response.xpath('//h3/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.larazon.es'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2/text()').get()
        date = response.xpath('//meta[@name="date"]/@content').get()
        paragraph = ''.join(response.xpath('//section/p//text()').getall())
        
        # data pipeline
        try:
            date_temp = date[:10]
            date = datetime.datetime.strptime(date_temp, '%Y-%m-%d').strftime('%d-%m-%Y')
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'Pagina 12',
            'language' : language
        }               

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class ABCSpider(scrapy.Spider):
    name = 'ABCSpider'
    start_urls = ['https://www.abc.es/']
    allowed_domains = ['abc.es']

    def parse(self, response):
        news_links = response.xpath('//h3/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.abc.es/'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2/text()').get()
        date = response.xpath('//meta[@name="date"]/@content').get()
        paragraph = ''.join(response.xpath('//p[@class="voc-p"]//text()').getall())

        # data pipeline
        try:
            date_temp = date[:10]
            date = datetime.datetime.strptime(date_temp, '%Y-%m-%d').strftime('%d-%m-%Y')
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'ABC',
            'language' : language
        }               

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)


configure_logging()
runner = CrawlerRunner()

runner.crawl(ElMundoSpider)
runner.crawl(EldiarioSpider)
runner.crawl(ElEspañolSpider)
runner.crawl(LaRazonSpider)
runner.crawl(ABCSpider)

d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()