from gc import callbacks
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
from scrapy.signalmanager import dispatcher
from scrapy.utils.project import get_project_settings
from homzmart_scraping.homzmart_scraping.items import HomePageItem
from scrapy.loader import ItemLoader
from scraper_api import ScraperAPIClient

# Load environment variables
load_dotenv()
# Scrapper API for rotating through proxies
client = ScraperAPIClient(os.environ['SCRAPER_API_KEY'])

class HomePageSpider(scrapy.Spider): # Extract the names and URLs of the categories from the homepage
    name = 'home_page_spider'
    allowed_domains = ['homzmart.com']
    first_url = 'https://homzmart.com/en'
    custom_settings = {"FEEDS":{"Output_Home_Page.json":{"format":"json", "overwrite": True}}}

    def start_requests(self):
        yield scrapy.Request(client.scrapyGet(url = HomePageSpider.first_url, render=True, country_code='de'), callback = self.parse)

    async def parse(self, response):
        for cat in response.css('div.site-menu__item'):
            l = ItemLoader(item = HomePageItem(), selector = cat)
            l.add_css('cat_name', 'a')
            l.add_css('cat_url', 'a::attr(href)')
            l.add_value('response_url', response.headers['Sa-Final-Url'])
            
            yield l.load_item()

# Run the spider
process = CrawlerProcess(settings = {
    # Adjusting the scraping behavior to rotate appropriately through proxies and user agents
    "CONCURRENT_REQUESTS": 5, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "RETRY_TIMES": 5, # Catch and retry failed requests up to 5 times
    "ROBOTSTXT_OBEY": False, # Saves one API call
})
process.crawl(HomePageSpider)
process.start()