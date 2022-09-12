import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import pymongo

import re
import datetime


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = myclient['Newspapers']
today = datetime.datetime.now().strftime('%d-%m-%Y')
country = 'Uruguay'
language = 'Spanish'

months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio',
          'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']

class ElPaisSpider(scrapy.Spider):
    name = 'ElPaisSpider'
    start_urls = ['https://www.elpais.com.uy/']
    allowed_domains = ['elpais.com.uy']

    def parse(self, response):
        news_links = response.xpath('//h2[@class="title"]//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.elpais.com.uy'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//h2[@class="epigraph"]//text()').get()
        date = response.xpath('//div[@class="published-date"]/span/text()').get()
        paragraph = ''.join(response.xpath('//div[@class="article-content"]//p[@class="paragraph selectionShareable"]//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.search('\d\d.+\d\d\d\d', date).group(0)
            month = re.search('\D+', date_temp).group(0).strip()
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
            'newspaper': 'El Pais',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class Lr21Spider(scrapy.Spider):
    name = 'Lr21Spider'
    start_urls = ['https://www.lr21.com.uy/']
    allowed_domains = ['lr21.com.uy']

    def parse(self, response):
        news_links = response.xpath('//h2//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.lr21.com.uy' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//h2[@class="lead"]/text()').get()
        date = response.xpath('//time[@class="date"]/text()').get()
        paragraph = ''.join(response.xpath('//div[@class="article-story--content"]/p//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.sub('de ', '', date)
            date_temp = re.search('\d\d.+\d\d\d\d', date_temp).group(0)
            month = re.search('\D+', date_temp).group(0).strip()
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
            'newspaper': 'LR21',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class ElObservadorSpider(scrapy.Spider):
    name = 'ElObservadorSpider'
    start_urls = ['https://www.elobservador.com.uy/']
    allowed_domains = ['elobservador.com.uy']

    def parse(self, response):
        news_links = response.xpath('//h2//@href | //h1//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.elobservador.com.uy' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//h2[@class="article-deck article__deck"]//text()').get()
        date = response.xpath('//span[@class="date small"]//text()').get()
        paragraph = ''.join(response.xpath('//div[@class="article-body paywalled-content"]/p//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.sub('de ', '', date)
            date_temp = re.search('\d\d.+\d\d\d\d', date_temp).group(0)
            month = re.search('\D+', date_temp).group(0).strip()
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
            'newspaper': 'El Observador',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class LaDiariaSpider(scrapy.Spider):
    name = 'LaDiariaSpider'
    start_urls = ['https://ladiaria.com.uy/']
    allowed_domains = ['ladiaria.com.uy']

    def parse(self, response):
        news_links = response.xpath('//h4//@href | //h3//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://ladiaria.com.uy' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//div[@class="field-field-noticia-bajada"]//text()').get()
        date = response.xpath('//div[@class="date-created-node"]/span/text()').get()
        paragraph = ''.join(response.xpath('//div[@class="content content-node"]//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.sub('de', '', date)
            date_temp = re.search('.+ \d\d\d\d', date_temp).group(0)
            month = re.search('\D+', date_temp).group(0).strip().lower()
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
            'newspaper': 'La Diaria',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class MontevideoSpider(scrapy.Spider):
    name = 'MontevideoSpider'
    start_urls = ['https://www.montevideo.com.uy/']
    allowed_domains = ['montevideo.com.uy']

    def parse(self, response):
        news_links = response.xpath('//h2/a/@href | //article/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.montevideo.com.uy' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h2[@class="title"]/text()').get()
        epigraph = response.xpath('//div[@itemprop="description"]/text()').get()
        date = response.xpath('//p[@class="fecha-hora"]/text()').get()
        paragraph = ''.join(response.xpath('//div[@itemprop="articleBody"]/p/text()').getall())
        
        # data pipeline
        try:
            date_temp = re.search('\d\d.+\d\d\d\d', date).group(0)
            date = re.sub('\.', '-', date_temp)
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'Montevideo',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

configure_logging()
runner = CrawlerRunner()

runner.crawl(ElPaisSpider)
runner.crawl(Lr21Spider)
runner.crawl(ElObservadorSpider)
runner.crawl(LaDiariaSpider)
runner.crawl(MontevideoSpider)

d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()