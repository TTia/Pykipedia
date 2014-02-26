from datetime import datetime
from pykipedia.crawler.crawler import Crawler
from pykipedia.neo4j.gexf import GexfGenerator
from pykipedia.neo4j.driver import Driver
from pykipedia.neo4j.eraser import Eraser
from pykipedia.neo4j.pageRank import PageRank

if __name__ == "__main__":
    '''
    pageRank = PageRank()
    pageRank.rank()
    '''
    '''
    start = str(datetime.now())
    c = Crawler(steps = 100)
    c.startCrawler()
    end = str(datetime.now())
    print("Visit start: {0}\nVisit end: {1}\nEdges: {2}".format(start, end, c.numEdges))
    '''
    driver = Driver()
    gen = GexfGenerator()
    gen.generateGexfFile(driver)
    '''
    driver = Driver()
    eraser = Eraser(driver)
    print(str(driver.countNodes()))
    #eraser.eraseGraph(eraser.attackByDegree)
    eraser.eraseGraph(eraser.failure)
    print(str(driver.countNodes()))
    '''