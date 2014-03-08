'''
Created on 20/feb/2014

@author: Mattia
'''
from pykipedia.neo4j.driver import Driver

'''
START n=node(*)
MATCH (n)-[r?]->(m)
RETURN Id(n), count(r) as ograde, extract(x IN collect(m) | Id(x)) as neighbours
ORDER BY Id(n);
'''

class PageRank():
    
    def __init__(self, driver = Driver()):
        self.driver = driver

    def rank(self, alpha = 0.85, it_count = 50, k = 20):
        pi, pi_next = self.initializeDistribution()
        n = self.driver.countNodes()
        for i in range(it_count):
            print("Processing PageRank Algorithm [It. %d/%d]" % (i+1,it_count))
            self.it(pi, pi_next, alpha, n)
            temp = pi
            pi = pi_next
            pi_next = temp

        return self.naiveTopK(k, pi)
    
    def it(self, pi, pi_next, alpha, n):
        for key in pi_next:
            pi_next[key] = 0
        p = 1 - alpha
        
        for node in self.driver.getNodesAndNeighbours(): #Query
            m = node[1] #Grado uscente del nodo J
            j = node[0] #Id del nodo J - L'id dato dal database
            if m == 0:
                p += alpha * pi[j]
            else:
                for k in node[2]: #Collezione degli id dei vicini a J (J->K)
                    pi_next[k] += alpha * (pi[j]/m)

        for key in pi_next:
            pi_next[key] += p/n

    def naiveTopK(self, k, pi):
        print("Sorting...")
        orderedView = [(pr,k) for k,pr in pi.items()]
        orderedView.sort(reverse=True)
        
        print("Id\tPageRank Score")
        i = 0
        for v,k in orderedView:
            print("%d\t%e" % (k,v))
            if i>=k:
                break
            i+=1
        return orderedView

    def initializeDistribution(self):
        dist = 1.0/self.driver.countNodes()
        pi = {}
        pi_next = {}
        for node in self.driver.getNodes():
            pi[node[0]] = dist
            pi_next[node[0]] = 0
        return (pi, pi_next)