import json

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
        self.startUrl = "http://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page={0}".format(startPage)
        
    def startCrawler(self):
        response = fetch(openAnything(self.startUrl))
        # print(type(response)) --> <class 'dict'>
        # print(type(response['data'])) --> <class 'bytes'>
        #print( response['data'])
        j = json.loads(response['data'].decode('utf-8'))
        pageLinks = []
        for k in j['parse']['links']:
            pageLinks.append(k['*'])
        print (pageLinks)