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
country = 'Argentina'
language = 'Spanish'

months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio',
          'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']

class LitoralSpider(scrapy.Spider):
    name = 'LitoralSpider'
    start_urls = ['https://www.ellitoral.com/']
    allowed_domains = ['ellitoral.com']

    
    def parse(self, response):
        news_links = response.xpath('//div[@class="flex-content _cdcy1 "]//div/div/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.ellitoral.com'+link
                yield response.follow(url=url, callback=self.parse_links)
            
    def parse_links(self, response):
        title = response.xpath('//h1[@class="headline-text"]/text()').get()
        epigraph = response.xpath('//h2[@class="styles_note-hook__ZhY7D"]//text()').get()
        date = response.xpath('//div[@class="styles_dynamic-content__TRcVW"]/div/div/text()').get()
        paragraph = ''.join(response.xpath('//div[@class="styles_dynamic-content__TRcVW"]//p/text()').getall())
        
        # data pipeline
        try:
            date = re.sub('[a-zA-Z ]', '', date)
            date = re.sub('\.', '-', date)
        except:
            date = 'no_date'
        
        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'El Litoral',
            'language' : language
        }

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class LaNacionSpider(scrapy.Spider):
    name = 'LaNacionSpider'
    start_urls = ['https://www.lanacion.com.ar/']
    allowed_domains = ['lanacion.com.ar']

    def parse(self, response):
        news_links = response.xpath('//h2/a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.lanacion.com.ar'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2[@class="com-subhead --bajada --m-xs"]/text()').get()
        date = response.xpath('//span[@class="mod-date"]//text()').get()
        paragraph = ''.join(response.xpath('//p[@class="com-paragraph   --s"]//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.sub('de', '', date)
            month = re.search(' .+ ', date_temp).group(0).strip()
            if month in date_temp:
                month = str(date_temp.index(month)+1)
            day = re.search('\d', date_temp).group(0)
            year = re.search(' \d+', date_temp).group(0).strip()
            date = day+'-'+month+'-'+year
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'La Nacion',
            'language' : language
        }

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)
            
class ClarinSpider(scrapy.Spider):
    name = 'ClarinSpider'
    start_urls = ['https://www.clarin.com/']
    allowed_domains = ['clarin.com']

    def parse(self, response):
        news_links = response.xpath('//li/@aria-label | //a[@class="link_article"]/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.clarin.com'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):       
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//div[@itemprop="description"]/h2/text()').get()
        date = response.xpath('//span[@class="publishedDate"]//text()').get()
        paragraph = ''.join(response.xpath('//div[@class="body-nota"]/p//text()').getall())
        
        # data pipeline
        try:
            date = re.search('.+ ', date).group(0).replace('/', '-').strip()  
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'Clarin',
            'language' : language
        }               

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)

class Pagina12Spider(scrapy.Spider):
    name = 'Pagina12Spider'
    start_urls = ['https://www.pagina12.com.ar/']
    allowed_domains = ['pagina12.com.ar']

    def parse(self, response):
        news_links = response.xpath('//div[@class="article-title "]//a/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.pagina12.com.ar'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//div[@class="col 2-col"]/h3/text()').get()
        date = response.xpath('//div[@class="article-info"]//text()').get()
        paragraph = ''.join(response.xpath('//div[@class="article-main-content article-text "]//text()').getall())
        
        # data pipeline
        try:
            date_temp = re.search('.+ -', date).group(0).strip('-').strip()
            a = re.sub('de', '', date_temp)
            month = re.search(' .+ ', date_temp).group(0).strip()
            if month in months:
                month = str(months.index(month)+1)
            day = re.search('\d', a).group(0)
            year = re.search(' \d+', a).group(0).strip()
            date = day+'-'+month+'-'+year
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

class InfobaeSpider(scrapy.Spider):
    name = 'InfobaeSpider'
    start_urls = ['https://www.infobae.com/']
    allowed_domains = ['infobae.com']

    def parse(self, response):
        news_links = response.xpath('//a[@class="cst_ctn"]/@href').getall()
        for link in news_links:
            if link[0:5] == 'https':
                pass
            else:
                url = 'https://www.infobae.com'+link
                yield response.follow(url=url, callback=self.parse_links)

    def parse_links(self, response):
        title = response.xpath('//h1/text()').get()
        epigraph = response.xpath('//h2[@class="article-subheadline"]/text()').get()
        date = response.xpath('//div[@class="byline-datetime"]/text()').get()
        paragraph = ''.join(response.xpath('//div[@class="nd-body-article"]//p/text()').getall())

        # data pipeline
        try:
            date_temp = re.sub('de', '', date)
            month = re.search(' .+ ', date_temp).group(0).strip()
            if month in date_temp:
                month = str(date_temp.index(month)+1)
            day = re.search('\d', date_temp).group(0)
            year = re.search(' \d+', date_temp).group(0).strip()
            date = day+'-'+month+'-'+year
        except:
            date = 'no_date'

        temp = {
            'title': title,
            'epigraph': epigraph,
            'paragraph': paragraph,
            'date': date,
            'scrape_date': today,
            'newspaper': 'Infobae',
            'language' : language
        }               

        if temp['title'] == None and temp['epigraph'] == None:
            pass
        else:
            my_collection = my_db[country]
            my_collection.insert_one(temp)


configure_logging()
runner = CrawlerRunner()

runner.crawl(LitoralSpider)
runner.crawl(LaNacionSpider)
runner.crawl(ClarinSpider)
runner.crawl(Pagina12Spider)
runner.crawl(InfobaeSpider)

d = runner.join()
d.addBoth(lambda _: reactor.stop())

reactor.run()