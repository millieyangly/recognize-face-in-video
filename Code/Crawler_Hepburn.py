#-*- coding:utf-8    -*-
import urllib
from bs4 import BeautifulSoup
import os
import re
import datetime

url_head = 'https://www.gettyimages.com'

def get_page_url(page_num):
    # page_url = 'https://www.gettyimages.com/photos/audrey-hepburn?mediatype=photography&page=1&phrase=audrey%20hepburn&sort=mostpopular&family=editorial'
    left = 'https://www.gettyimages.com/photos/audrey-hepburn?mediatype=photography&page='
    num = str(page_num)
    right = '&phrase=audrey%20hepburn&sort=mostpopular&family=editorial'
    return left+num+right
    
def get_detail_urls(page_url, url_head):
    soup = BeautifulSoup(urllib.request.urlopen(page_url), features='lxml')
    items = soup.find_all('a',class_='asset-link draggable')
    hrefs = [url_head+item['href'] for item in items]
    return hrefs
def download_img(detail_url, dir='/Users/david/Downloads/HB'):
    soup = BeautifulSoup(urllib.request.urlopen(detail_url), features='lxml')
    item = soup.find('div', class_='zoom-wrapper')
    soup = BeautifulSoup(urllib.request.urlopen(detail_url), features='lxml')
    item = soup.find('div', class_='zoom-wrapper')
    img_url = item.select('img')[0]['src']
    tmp = item.select('img')[0]['title'].strip()
    img_title = re.split(':|,|',tmp)[1].strip() 
    # urllib.request.urlretrieve(img_url, os.path.join(dir,img_title+'.jpg'))
    name = img_title+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+'.jpg'
    f = open(os.path.join(dir,name),'wb')
    f.write(urllib.request.urlopen(img_url).read())
    f.close()
    return name


if __name__ == "__main__":
    dir = '/Users/david/Downloads/HB'
    if not os.path.isdir(dir):
        os.mkdir(dir)
    # for each page
    for i in range(4,60):
        page_num = i+1
        dir2 = os.path.join(dir,'page-'+str(page_num))
        if not os.path.isdir(dir2):
            os.mkdir(dir2)
        page_url = get_page_url(page_num)
        detail_urls = get_detail_urls(page_url, url_head)
        for du in detail_urls:
            try:
                name = download_img(du,dir=dir2)
                print(name)
            except:
                print('error', name)

            

