from datetime import datetime, date
from pykipedia.crawler.crawler import Crawler

if __name__ == "__main__":
    startTime = datetime.now()
    c = Crawler(steps = 8)
    c.startCrawler()
    endTime = datetime.now()
    
    print("Visit start: {0}\nVisit end: {1}\nEdges: {2}".format(str(startTime), str(endTime), c.numEdges))
    delta = endTime - startTime
    print(delta)