from datetime import datetime
from pykipedia.crawler.crawler import Crawler
from pykipedia.neo4j.driver import Driver
from pykipedia.neo4j.eraser import Eraser

if __name__ == "__main__":
    start = str(datetime.now())
    c = Crawler()
    c.startCrawler()
    end = str(datetime.now())
    
    print("Visit start: {0}\nVisit end: {1}\nEdges: {2}".format(start, end, c.numEdges))
    '''
    driver = Driver()
    eraser = Eraser(driver)
    print(str(driver.countNodes()))
    #eraser.eraseGraph(eraser.attackByDegree)
    eraser.eraseGraph(eraser.failure)
    print(str(driver.countNodes()))
    '''