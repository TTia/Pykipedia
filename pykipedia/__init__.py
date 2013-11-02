from datetime import datetime
from crawler.crawler import Crawler

if __name__ == "__main__":
    start = str(datetime.now())
    c = Crawler()
    c.startCrawler()
    end = str(datetime.now())
    
    print("Visit start: {0}\nVisit end: {1}\nEdges: {2}".format(start, end, c.numEdges))