from crawler.openAnything import *
from neo4j.driver import *
from neo4j.gexf import *
import json
import urllib
import re


class Crawler():
    
    def __init__(self, startPage='Alan_turing'):
        self.startPage = startPage
        self.regEx = re.compile("[a-zA-Z0-9_\s]+$", re.ASCII)
        self.driver = Driver()
        self.driver.resetDB()
        self.pageList = []
        self.steps = 32
        self.numEdges = 0
    
    def __str__(self):
        #return "Visit start: {0}\nVisit end:{1}\nEdges:{2}".format(self.timeStart, self.timeEnd, self.numEdges)
        pass
    
    def getApiUrl(self, str):
        return "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(urllib.parse.quote(str))
                
    def startCrawler(self):
        vistitingName = self.startPage
        vistitingUrl = self.getApiUrl(self.startPage)
        self.driver.createNode([vistitingUrl, vistitingName])
        
        for c in range(self.steps):
            print("--------------------------------------------------")
            print (str(c)+ ". Visiting => " +vistitingUrl)
            response = fetch(openAnything(vistitingUrl)) # <class 'dict'>
            try:
                j = json.loads(response['data'].decode('utf-8'))
                for k in j['parse']['links']:
                    if self.regEx.match(k['*']):
                        parsedName = "".join(i for i in k['*'] if ord(i)<128) #solve unicode problem
                        parsedUrl = self.getApiUrl(parsedName)
                        self.pageList.append(parsedName) #add new node to list
                        self.driver.createNode([parsedUrl, parsedName])
                        self.driver.createEdge(vistitingUrl, parsedUrl)
                        self.numEdges = self.numEdges + 1
                        print (vistitingName + "-->" + parsedName)
            except KeyError:
                pass
            vistitingName = self.pageList.pop(0)
            vistitingUrl = self.getApiUrl(vistitingName)
            print("--------------------------------------------------\n")
        '''
        gen = GexfGenerator()
        gen.generateGexfFile(self.driver)
        '''
