from pykipedia.crawler.openAnything import *
from pykipedia.neo4j.driver import Driver
from pykipedia.neo4j.gexf import GexfGenerator
import json
import urllib
import re

from queue import Queue
from threading import Thread    

class Node:
    def __init__(self, visitingUrl, visitingName):
        self.visitingUrl = visitingUrl
        self.visitingName = visitingName

    def isNode(self):
        return True

class Edge:
    
    def __init__(self, visitingUrl, parsedUrl):
        self.visitingUrl = visitingUrl
        self.parsedUrl = parsedUrl

    def isNode(self):
        return False

class Populator(Thread):
    
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.driver = Driver()
        self.driver.resetDB()
    
    def run(self):        
        while True:
            item = self.queue.get(True)
            if type(item) is Node:
                self.consumeNode(item)
            else:
                self.consumeEdge(item)
            self.queue.task_done()

    def consumeNode(self, node):
        self.driver.createNode([node.visitingUrl, node.visitingName])
        print("Node: "+node.visitingName)

    def consumeEdge(self, edge):
        self.driver.createEdge(edge.visitingUrl, edge.parsedUrl)
        print("\t\t\t--> "+edge.parsedUrl)

class Crawler():
    
    def __init__(self, startPage='Alan Turing', steps = 32):
        self.startPage = startPage
        self.regEx = re.compile("[a-zA-Z0-9_\s]+$", re.ASCII)

        self.pageList = []
        self.steps = steps
        
        self.numEdges = 0
    
    def __str__(self):
        #return "Visit start: {0}\nVisit end:{1}\nEdges:{2}".format(self.timeStart, self.timeEnd, self.numEdges)
        pass
    
    def getApiUrl(self, str):
        return "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(urllib.parse.quote(str))
                
    def startCrawler(self):
        visitingName = self.startPage
        visitingUrl = self.getApiUrl(self.startPage)
        
        queue = Queue()
        populator = Populator(queue)
        populator.daemon = True
        populator.start()

        queue.put(Node(visitingUrl, visitingName), True)     
        #self.driver.createNode([visitingUrl, visitingName])
        
        for c in range(self.steps):
            #print("--------------------------------------------------")
            #print (str(c)+ ". Visiting => " +visitingUrl)
            response = fetch(openAnything(visitingUrl)) # <class 'dict'>
            try:
                j = json.loads(response['data'].decode('utf-8'))
                for k in j['parse']['links']:
                    if self.regEx.match(k['*']):
                        parsedName = "".join(i for i in k['*'] if ord(i)<128) #solve unicode problem
                        parsedUrl = self.getApiUrl(parsedName)

                        self.pageList.append(parsedName) #add new node to list
                        
                        #self.driver.createNode([parsedUrl, parsedName])
                        queue.put(Node(parsedUrl, parsedName), True)
                        #self.driver.createEdge(visitingUrl, parsedUrl)
                        queue.put(Edge(visitingUrl, parsedUrl), True)
                        self.numEdges = self.numEdges + 1
                        #print (visitingName + "-->" + parsedName)
                        
            except KeyError:
                pass
            visitingName = self.pageList.pop(0)
            visitingUrl = self.getApiUrl(visitingName)
            #print("--------------------------------------------------\n")
            
        queue.join()
        '''
        gen = GexfGenerator()
        gen.generateGexfFile(self.driver)
        '''