# iMPORT NECESSARY LIBRARIES
from creds import IP, DB, DB_PASS
import logging
from sqlalchemy import create_engine
import requests
from fake_useragent import UserAgent
import socket
import mysql.connector
import os
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import time
import random
import re
import json
from selenium.webdriver import Chrome, ChromeOptions, DesiredCapabilities
from lxml import html
import pandas as pd
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
socket.setdefaulttimeout(50)

print = logging.error

# Database connect


class DB_Connect():
    def __init__(self, db):
        self.db = db
        self.connection = self.get_connection()

    def get_connection(self):
        engine = create_engine("mysql://root:"+DB_PASS+"@"+IP+"/"+self.db)
        connection = engine.connect()
        cursor = engine.raw_connection().cursor()
        return connection, cursor

    def close_connection(self):
        self.connection.close()


def master_db_login():
    connection = mysql.connector.connect(host=IP,
                                         database='brand_master',
                                         user='root',
                                         password=DB_PASS,
                                         port=3306, auth_plugin='caching_sha2_password')
    cursor = connection.cursor()
    return connection, cursor


def get_proxy():
    db_conn = DB_Connect('facebook_feed')
    connection, cursor = db_conn.connection
    cursor.execute(
        "SELECT login,password,proxy from fb_account_proxies_merged ORDER BY `id` ASC")
    rows = cursor.fetchall()
    return rows


def get_useragent():
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    ]
    useragent = user_agent_list[random.randint(0, 3)]
    return useragent


# Proxy browser
def getdriver(proxy):
    options = ChromeOptions()
    options.add_argument('log-level=3')
    options.add_argument("--window-size=1880x1020")
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-gpu')
    options.add_argument("--headless")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-blink-features")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    prefs = {"profile.default_content_setting_values.notifications": 2}
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    userAgent = get_useragent()

    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("test-type")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    # ChromeDriverManager().install())
    driver = Chrome(
        options=options, executable_path='/root/Desktop/scrapping_full/cron/chromedriver')
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                           "userAgent": userAgent})
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
    Object.defineProperty(navigator, 'webdriver', {
     get: () => undefined
    })
    """})
    return driver


def waitfor(driver, xpth):
    try:
        WebDriverWait(driver, 9).until(
            EC.presence_of_element_located((By.XPATH, xpth)))
    except Exception as e:
        print(e)


def jsclick(driver, xpth):
    try:
        waitfor(driver, xpth)
        element = driver.find_element_by_xpath(xpth)
        driver.execute_script("arguments[0].click();", element)
    except Exception as e:
        print(e)


def scrollDown(driver):
    while True:
        lastHeight = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(5)
        newHeight = driver.execute_script("return document.body.scrollHeight")
        if newHeight == lastHeight:
            break


def fix_url(url):
    fixed_url = url.strip()
    if not fixed_url.startswith('http://') and \
       not fixed_url.startswith('https://'):
        fixed_url = 'https://' + fixed_url

    return fixed_url.rstrip('/')


def shopify_master_only(query):
    connection, cursor = master_db_login()
    brands_categroy = pd.read_sql_query(query, connection)
    print('connection established, data fetched')
    connection.close()
    brands_categroy.drop_duplicates(subset=['BrandName'], inplace=True)
    brands_categroy = brands_categroy[brands_categroy['ShopifySite'] != 'NULL']
    brands_categroy = brands_categroy[brands_categroy['ShopifySite']
                                      != 'No shopify site found']
    brands_categroy.dropna(subset=['BrandName'], inplace=True)
    brands_categroy.dropna(subset=['ShopifySite'], inplace=True)
    brands_categroy = brands_categroy[brands_categroy['BrandName'] != 'N/A']
    brands_categroy = brands_categroy[~brands_categroy['BrandSite'].isnull()]
    brands_categroy.reset_index(drop=True, inplace=True)
    return brands_categroy
