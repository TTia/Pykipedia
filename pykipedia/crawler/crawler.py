from crawler.openAnything import *
from neo4j.driver import *
import json
import urllib
import re

from crawler.openAnything import *


class Node():
    
    def __init__(self, name):
        self.name = name
        self.neighbours = []
        
    def addNeighbour(self, neighbour):
        self.neighbours.append(neighbour)



class Crawler():
    
    def __init__(self, startPage='Eugenio_Moggi'):
        self.startPage = startPage
        #self.startUrl = "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(startPage)
        self.steps = 32
        self.regEx = re.compile(r"\b[a-zA-Z0-9_\s]+\b", re.ASCII)
        self.pages = []
        self.driver = Driver()
        self.driver.resetDB()

    def getApiUrl(self, str):
        return "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(urllib.parse.quote(str))
                
    def startCrawler(self):
        nodeName = self.startPage
        url = self.getApiUrl(self.startPage)
        
        for c in range(self.steps):
            print("--------------------------------------------------")
            print (str(c)+ "->" +url)
            response = fetch(openAnything(url))
            # print(type(response)) --> <class 'dict'>
            # print(type(response['data'])) --> <class 'bytes'>
            #print( response['data'])
            self.driver.createNode([url, nodeName])
            try:
                j = json.loads(response['data'].decode('utf-8'))
                
                for k in j['parse']['links']:
                    if self.regEx.match(k['*']):
                        parsedName = "".join(i for i in k['*'] if ord(i)<128)
                        self.pages.append(parsedName) #solve unicode problem
                        self.driver.createEdge(url, self.getApiUrl(parsedName))
                        #print (k['*'])
                #print (self.pages)
            except KeyError:
                pass
            nodeName = self.pages.pop(0)
            url = self.getApiUrl(nodeName)
            print("--------------------------------------------------\n")
        #print (self.pages)
