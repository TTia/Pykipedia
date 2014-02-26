'''
Created on 30/dic/2013

@author: Mattia
'''
import random
from py2neo import neo4j
from pykipedia.neo4j.gexf import GexfGenerator

'''
Attacchi da scrivere:
+ correggere degree in outDegree
+ inDegree
+ degree
+ betweenness statica
+ pageRank statico

Plotting:
+individuare una misura di riferimento (efficienza?)
+invece che produrre i gexf produrre file per gnuplot
'''

class Eraser(object):
    def __init__(self, driver):
        self.random = random.Random()
        self.driver = driver
        deleteNode_query_text = """START n=node(*)
                                    MATCH (n)-[r?]-()
                                    WHERE n.title = {Title}
                                    DELETE r,n;"""
        self.queryDeleteNode = neo4j.CypherQuery(self.driver.db, deleteNode_query_text)

        randomNode_query_text = """START n=node(*)
                                    WITH n
                                    SKIP {R}
                                    LIMIT 1
                                    MATCH (n)-[r]-(neighbors)
                                    RETURN neighbors.title;"""
        self.queryRandomNode = neo4j.CypherQuery(self.driver.db, randomNode_query_text)
        
        degreeNode_query_text = """START n = node(*) 
                                    MATCH (n)-[r?]->()
                                    WITH n.title as title, count(r) as connections
                                    ORDER BY connections DESC
                                    LIMIT 1
                                    RETURN title;"""
        self.queryDegreeNode = neo4j.CypherQuery(self.driver.db, degreeNode_query_text)
        
        diameter_query_text = """MATCH p = allShortestPaths((source)-->(destination))
                                WHERE source <> destination and length(nodes(p)) > 2
                                RETURN max(length(nodes(p))) as Diametro;"""
        self.queryDiameter = neo4j.CypherQuery(self.driver.db, diameter_query_text)

    def eraseGraph(self, deleteMethod, plots = 25):
        aliveRelationships = M = self.driver.countRelationship()
        previousRelationshipsCount = 0
        iPlots = plots
        '''
        if deleteMethod == self.attackByBetweeness:
            self.queryBetweenessA.run()
            self.queryBetweenessB.run()
        '''
            
        while aliveRelationships > 0:
            self.deleteNode(deleteMethod)
            if aliveRelationships <= (M / plots)*iPlots and previousRelationshipsCount != aliveRelationships:
                #diameter = self.diameter()
                self.plot(deleteMethod.__name__, plots - iPlots)
                iPlots -= 1
            #print("Nodes: "+str(aliveNodes)+" Rels: "+str(aliveRelationships))
            previousRelationshipsCount = aliveRelationships
            aliveRelationships = self.driver.countRelationship()
            print("Rels: "+str(aliveRelationships))
    '''
    def diameter(self):
        diameter = self.queryDiameter.execute_one()
        print("\t\t\tDiameter:"+str(diameter))
        return diameter
    '''
        
    def deleteNode(self, deleteMethod):
        if deleteMethod == self.failure:
            aliveNodes = self.driver.countNodes()
            titles = deleteMethod(aliveNodes)
            for title in titles:
                #print(title[0])
                self.queryDeleteNode.run(Title = title[0])
        else:
            title = deleteMethod()
            self.queryDeleteNode.run(Title = title)
        #print(title)
        
    def plot(self, deleteMethodName, step):
        print("\t\t\tPlot to: "+"wikipedia_"+deleteMethodName+"_"+str(int(step))+".gexf")
        gen = GexfGenerator()
        gen.generateGexfFile(self.driver, filename="wikipedia_"+deleteMethodName+"_rels="+str(step)+".gexf")

    
    def attackByDegree(self):
        title = self.queryDegreeNode.execute_one()
        return title
    
    def failure(self, aliveNodes):
        r = random.randrange(aliveNodes)
        titles = self.queryRandomNode.stream(R = r)
        return titles
    
'''
    def attackByBetweeness(self):
        title = self.queryBetweenessC.execute_one()
        return title
---
        betweenessNode_qt_A = """MATCH (n)
                                SET n.o = 0;"""
        betweenessNode_qt_B = """MATCH p = allShortestPaths((source)-->(destination))
                                WHERE source <> destination and length(nodes(p)) > 2
                                FOREACH (n in nodes(p) | SET n.o = n.o + 1);"""
        betweenessNode_qt_C = """MATCH (n)
                                RETURN n.title
                                ORDER BY n.o DESC
                                LIMIT 1;"""
        self.queryBetweenessA = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_A)
        self.queryBetweenessB = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_B)
        self.queryBetweenessC = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_C)
'''