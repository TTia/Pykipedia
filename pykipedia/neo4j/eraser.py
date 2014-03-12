'''
Created on 30/dic/2013

@author: Mattia
'''
import random
from py2neo import neo4j
from pykipedia.neo4j.pageRank import PageRank
from py2neo.packages.httpstream import SocketError

class Eraser(object):
    def __init__(self, driver):
        self.random = random.Random()
        self.driver = driver
        deleteNode_query_text = """START n=node(*)
                                    MATCH (n)-[r?]-()
                                    WHERE Id(n) = {Id}
                                    DELETE r,n
                                    RETURN count(r) as rels
                                    LIMIT 1;"""
        self.queryDeleteNode = neo4j.CypherQuery(self.driver.db, deleteNode_query_text)

        self.defineDegreeAttacks()
        self.defineFailureAttack()
        self.definePointedAttack()
        self.definePointingAttack()

    def eraseGraph(self, deleteMethod, plots = 20):
        self.aliveNodes = self.driver.countNodes()
        self.aliveRelationships = M = self.driver.countRelationship()
        previousRelsCount = 0
        iPlots = plots
        
        if deleteMethod ==  self.attackByPageRank:
            self.defineAttackByPageRank()
            
        while self.aliveNodes > 0 and self.aliveRelationships > 0:
            (dn, dr) = self.deleteNode(deleteMethod)
            self.aliveNodes -= dn
            self.aliveRelationships -= dr
            if self.aliveRelationships <= (M / plots)*iPlots and previousRelsCount != self.aliveRelationships:
                efficiency = self.driver.getEfficency(self.aliveNodes) 
                #self.plot(self.aliveNodes, efficiency, deleteMethod.__name__)
                self.plot(self.aliveNodes, efficiency, "pino")
                iPlots -= 1
            previousRelsCount = self.aliveRelationships
            print("Nodes: "+str(self.aliveNodes)+" Rels: "+str(self.aliveRelationships))
        self.plot(self.aliveNodes, 0.0, deleteMethod.__name__)
        self.plot(0, 0.0, deleteMethod.__name__)
        
    def deleteNode(self, deleteMethod):
        deletedNodes = 0
        deletedRels = 0
        ids = deleteMethod()
        for _id in ids:
            r = 0
            k = 3
            while k>0:
                try:
                    r = self.queryDeleteNode.execute_one(Id = _id)
                    break
                except SocketError:
                    print("Try again...")
                    k-=1
            deletedNodes += 1
            deletedRels += r
        return (deletedNodes, deletedRels)
        
    def plot(self, nodes, efficiency, deleteMethodName):
        with open(deleteMethodName+".txt", "a+") as outputFile:
            print(str(nodes)+" "+str(efficiency), file=outputFile)
        outputFile.close()

    def defineDegreeAttacks(self):
        query_text = """START n = node(*) 
                                    MATCH (n)-[r?]-()
                                    WITH n, count(r) as connections
                                    ORDER BY connections DESC
                                    LIMIT 1
                                    RETURN Id(n);"""
        self.queryDegreeNode = neo4j.CypherQuery(self.driver.db, query_text)
        query_text = """START n = node(*) 
                                    MATCH (n)-[r?]->()
                                    WITH n, count(r) as connections
                                    ORDER BY connections DESC
                                    LIMIT 1
                                    RETURN Id(n);"""
        self.queryOutDegreeNode = neo4j.CypherQuery(self.driver.db, query_text)
        query_text = """START n = node(*) 
                                    MATCH (n)<-[r?]-()
                                    WITH n, count(r) as connections
                                    ORDER BY connections DESC
                                    LIMIT 1
                                    RETURN Id(n);"""
        self.queryInDegreeNode = neo4j.CypherQuery(self.driver.db, query_text)
    
    def attackByDegree(self):
        try:
            _id = self.queryDegreeNode.execute_one()
        except SocketError:
            print("Try again...")
            return self.attackByDegree()
        return [_id]
    
    def attackByInDegree(self):
        try:
            _id = self.queryInDegreeNode.execute_one()
        except SocketError:
            print("Try again...")
            return self.attackByInDegree()
        return [_id]
    
    def attackByOutDegree(self):
        try:
            _id = self.queryOutDegreeNode.execute_one()
        except SocketError:
            print("Try again...")
            return self.attackByOutDegree()
        return [_id]

    def definePointingAttack(self):
        randomNode_query_text = """START n=node(*)
                                    WITH n
                                    SKIP {R}
                                    LIMIT 1
                                    MATCH (n)<-[r]-(neighbors)
                                    RETURN Id(neighbors);"""
        self.queryRandomPointingNeighbours = neo4j.CypherQuery(self.driver.db, randomNode_query_text)

    def definePointedAttack(self):
        randomNode_query_text = """START n=node(*)
                                    WITH n
                                    SKIP {R}
                                    LIMIT 1
                                    MATCH (n)-[r]->(neighbors)
                                    RETURN Id(neighbors);"""
        self.queryRandomPointedNeighbours = neo4j.CypherQuery(self.driver.db, randomNode_query_text)

    def defineFailureAttack(self):
        randomNode_query_text = """START n=node(*)
                                    RETURN Id(n)
                                    SKIP {R}
                                    LIMIT 1;"""
        self.queryRandomNode = neo4j.CypherQuery(self.driver.db, randomNode_query_text)
    
    def failure(self):
        r = random.randrange(self.aliveNodes)
        try:
            _id = self.queryRandomNode.execute_one(R = r)
        except SocketError:
            print("Try again...")
            return self.failure()
        return [_id]

    def failurePointed(self):
        r = random.randrange(self.aliveNodes)
        try:            
            ids = self.queryRandomPointedNeighbours.stream(R = r)
        except SocketError:
            print("Try again...")
            return self.failurePointed()
        return [t[0] for t in ids]

    def failurePointing(self):
        r = random.randrange(self.aliveNodes)
        try:       
            ids = self.queryRandomPointingNeighbours.stream(R = r)
        except SocketError:
            print("Try again...")
            return self.failurePointing()
        return [t[0] for t in ids]
    
    def defineAttackByPageRank(self, alpha = 0.85, it_count = 50, k = 0):
        pageRank = PageRank()
        self.orderedView = pageRank.rank(alpha, it_count, k)
            
    def attackByPageRank(self):
        (_, _id) = self.orderedView.pop(0)
        return [_id]