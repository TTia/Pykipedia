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
        self.steps = 120
        self.regEx = re.compile(r"\b[a-zA-Z0-9_\s]+\b", re.ASCII)
        self.pageList = []
        self.driver = Driver()
        self.driver.resetDB()

    def getApiUrl(self, str):
        return "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(urllib.parse.quote(str))
                
    def startCrawler(self):
        vistitingName = self.startPage
        vistitingUrl = self.getApiUrl(self.startPage)
        self.driver.createNode([vistitingUrl, vistitingName])
        
        for c in range(self.steps):
            print("--------------------------------------------------")
            print (str(c)+ ". Visiting => " +vistitingUrl)
            response = fetch(openAnything(vistitingUrl))
            # print(type(response)) --> <class 'dict'>
            # print(type(response['data'])) --> <class 'bytes'>
            #print( response['data'])
            try:
                j = json.loads(response['data'].decode('utf-8'))
                for k in j['parse']['links']:
                    if self.regEx.match(k['*']):
                        parsedName = "".join(i for i in k['*'] if ord(i)<128) #solve unicode problem
                        parsedUrl = self.getApiUrl(parsedName)
                        self.pageList.append(parsedName) #add new node to list
                        self.driver.createNode([parsedUrl, parsedName])
                        self.driver.createEdge(vistitingUrl, parsedUrl)
                        print (vistitingName + "-->" + parsedName)
                        #print (k['*'])
                #print (self.pageList)
            except KeyError:
                pass
            vistitingName = self.pageList.pop(0)
            vistitingUrl = self.getApiUrl(vistitingName)
            print("--------------------------------------------------\n")
        #print (self.pageList)
