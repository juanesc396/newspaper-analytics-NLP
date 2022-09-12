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
country = 'Chile'
language = 'Spanish'

months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio',
          'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']



class LaTerceraSpider(scrapy.Spider):
    name = 'LaTerceraSpider'
    start_urls = ['https://www.latercera.com/']
    allowed_domains = ['latercera.com']

    def parse(self, response):
        news_links = response.xpath('//h4//@href | //h3//@href | //h2//@href | //h1//@href | //h5//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.latercera.com'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//p[@class="excerpt"]//text()').get()
        date = response.xpath('//meta[@property="og:updated_time"]/@content').get()
        paragraph = ''.join(response.xpath('//p[@class="paragraph  "]//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.search('.+T', date).group(0).strip('T')
            date_temp = datetime.datetime.strptime(date_temp, '%Y-%m-%d')
            date = str(datetime.datetime.strftime(date_temp, '%d-%m-%Y'))
        except:
            date = 'no_date'
        
        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'La Tercera',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class EmolSpider(scrapy.Spider):
    name = 'EmolSpider'
    start_urls = ['https://www.emol.com/']
    allowed_domains = ['emol.com']

    def parse(self, response):
        news_links = response.xpath('//div[@class="contenedor-titulo"]//@href | //h1//@href | //h2//@href | //h3//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.emol.com' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//div[@class="cont_iz_titulobajada"]//h2//text()').get()
        date = response.xpath('//div[@class="info-notaemol-porfecha"]/text()').get()
        paragraph = ''.join(response.xpath('//div[@id="cuDetalle_cuTexto_textoNoticia"]//text()').getall())
        
        # data pipeline
        try:
            data_temp = re.sub('de', '', date)
            data_temp = re.search('.+ \d\d\d\d', data_temp).group(0).strip('|')
            month = re.search('\D+', data_temp).group(0).strip().lower()
            if month in months:
                month = str(months.index(month)+1)

            day = re.search('\d*', data_temp).group(0)
            year = re.search(' \d+', data_temp).group(0).strip()
            date = day+'-'+month+'-'+year
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'Emol',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class LaRazonSpider(scrapy.Spider):
    name = 'LaRazonSpider'
    start_urls = ['https://www.larazon.cl/']
    allowed_domains = ['larazon.cl']

    def parse(self, response):
        news_links = response.xpath('//h4//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.larazon.cl' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1//text()').get()
        epigraph = response.xpath('//div[@class="article__announce-text"]//text()').get()
        date = response.xpath('//i[@class="fal fa-clock"]/following-sibling::text()').get()
        paragraph = ''.join(response.xpath('//div[@class="article__text"]//text()').getall())
        
        # data pipeline
        try:
            data_temp = re.search('.+ \d\d\d\d', data_temp).group(0)
            month = re.search('\D+', data_temp).group(0).strip().lower()
            if month in months:
                month = str(months.index(month)+1)

            day = re.search('\d\d', data_temp).group(0)
            year = re.search('\d\d\d\d', data_temp).group(0)
            date = day+'-'+month+'-'+year
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'La Razon',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class ElNorteñoSpider(scrapy.Spider):
    name = 'ElNorteñoSpider'
    start_urls = ['https://www.elnortero.cl/']
    allowed_domains = ['elnortero.cl']

    def parse(self, response):
        news_links = response.xpath('//h4//@href | //h3//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.elnortero.cl' + link
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
            'newspaper': 'El Norteño',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class ElMostradorSpider(scrapy.Spider):
    name = 'ElMostradorSpider'
    start_urls = ['https://www.elmostrador.cl/']
    allowed_domains = ['elmostrador.cl']

    def parse(self, response):
        news_links = response.xpath('//h3//@href | //h4//@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                yield response.follow(url=link, callback=self.parse_links)
            else:
                url = 'https://www.elmostrador.cl' + link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h2[@class="titulo-single"]/text()').get()
        epigraph = response.xpath('//article[@class="col-sm-12 col-md-12"]//figcaption/text()').get()
        date = response.xpath('//p[@class="col-sm-12 col-md-12 autor-y-fecha"]/text()[2]').get()
        paragraph = ''.join(response.xpath('//div[@id="noticia"]/p/text()').getall())
        
        # data pipeline
        try:
            date_temp = re.sub('de', '', date)
            date_temp = re.search('.+ \d\d\d\d', date_temp).group(0)
            month = re.search('\D+,', date_temp).group(0).strip().lower().replace(',', '')
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
            'newspaper': 'El Mostrador',
            'language' : language
        }
        
        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

configure_logging()
runner = CrawlerRunner()

runner.crawl(LaTerceraSpider)
runner.crawl(EmolSpider)
runner.crawl(LaRazonSpider)
runner.crawl(ElNorteñoSpider)
runner.crawl(ElMostradorSpider)

d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()