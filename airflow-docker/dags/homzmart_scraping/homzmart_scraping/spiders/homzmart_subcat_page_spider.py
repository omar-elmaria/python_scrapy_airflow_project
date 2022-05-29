from gc import callbacks
from ssl import VERIFY_DEFAULT
from sys import dont_write_bytecode
import sys
try:
    sys.path.append('/home/oelmaria/python_projects/python_scrapy_project/airflow-docker/dags') # For running the script locally - You can also insert this in the terminal --> export PYTHONPATH="${PYTHONPATH}:/home/oelmaria/homzmart_scraping/homzmart_scraping"
except FileNotFoundError:
    print("Airflow is being used. No need to add a new path to account for the host machine's environment")
import os
from dotenv import load_dotenv
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from homzmart_scraping.homzmart_scraping.items import SubCatPageItem
from scrapy.loader import ItemLoader
import json
from scraper_api import ScraperAPIClient

# Access the output of the "homzmart_cat_page_spider" script stored in the JSON file 'Output_Cat_Page.json'
try:
    with open('/opt/airflow/data/Output_Cat_Page.json', 'r') as file: # ADDED /opt/airflow/data to tell Airflow where to look for the JSON file
        data = json.load(file)
except FileNotFoundError:
    with open('/home/oelmaria/python_projects/python_scrapy_project/airflow-docker/data/Output_Cat_Page.json', 'r') as file: # If you are using the local env, the interpreter will look for the JSON file through a different directory
        data = json.load(file)

urls_from_cat_page = [d['sub_cat_url'] for d in data] # No need to define first_url in the spider class below anymore as we are reading the output of the previous script
urls_from_cat_page = urls_from_cat_page[-3:-2:1] # For TESTING purposes

# Load environment variables
load_dotenv()
# Scrapper API for rotating through proxies
client = ScraperAPIClient(os.environ['SCRAPER_API_KEY'])

class SubCatPageSpider(scrapy.Spider): # Extract the individual product page links from the sub-category pages
    name = 'sub_cat_page_spider'
    allowed_domains = ['homzmart.com']
    custom_settings = {"FEEDS":{"Output_SubCat_Page.json":{"format":"json", "overwrite": True}}}

    def start_requests(self):
        for url in urls_from_cat_page:
            yield scrapy.Request(client.scrapyGet(url = url, render=True, country_code='de'), callback = self.parse, dont_filter = True, meta = dict(master_url = url))

    async def parse(self, response):
        last_page = response.xpath('//ul[contains(@class, "v-pagination")]/li[last() - 1]/button/text()').get()
        current_page = response.xpath('//*[contains(@aria-label, "Current Page")]/text()').get()
        
        if last_page is not None and current_page is not None:
            if int(current_page) == 1:
                for i in range(2, int(last_page)+1): 
                    yield scrapy.Request(client.scrapyGet(url = response.meta['master_url'].replace('#1', '#{}').format(i), render=True, country_code='de'), callback = self.parse, dont_filter = True)

        for prod in response.css('div.card-body'):
            l = ItemLoader(item = SubCatPageItem(), selector = prod)
            l.add_css('prod_name', 'a h3')
            l.add_xpath('last_pg', '//ul[contains(@class, "v-pagination")]/li[last() - 1]/button')
            l.add_xpath('prod_pg_rank', '//*[contains(@aria-label, "Current Page")]')
            l.add_css('prod_url', 'a::attr(href)')
            l.add_value('response_url', response.headers['Sa-Final-Url'])
            
            yield l.load_item()

#Run the spider
process = CrawlerProcess(settings = {
    # Adjusting the scraping behavior to rotate appropriately through proxies and user agents
    "CONCURRENT_REQUESTS": 5, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "RETRY_TIMES": 5, # Catch and retry failed requests up to 5 times
    "ROBOTSTXT_OBEY": False, # Saves one API call
})
process.crawl(SubCatPageSpider)
process.start()