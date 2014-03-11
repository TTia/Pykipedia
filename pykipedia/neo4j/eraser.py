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
        aliveNodes = self.driver.countNodes()
        aliveRelationships = M = self.driver.countRelationship()
        previousRelsCount = 0
        iPlots = plots
        
        if deleteMethod == self.attackByBetweeness:
            self.defineAttackByByBetweeness()
        if deleteMethod ==  self.attackByPageRank:
            self.defineAttackByPageRank()
            
        while aliveNodes > 0 and aliveRelationships > 0:
            (dn, dr) = self.deleteNode(deleteMethod, aliveNodes)
            aliveNodes -= dn
            aliveRelationships -= dr
            if aliveRelationships <= (M / plots)*iPlots and previousRelsCount != aliveRelationships:
                efficiency = self.driver.getEfficency(aliveNodes) 
                self.plot(aliveNodes, efficiency, deleteMethod.__name__)
                iPlots -= 1
            previousRelsCount = aliveRelationships
            print("Nodes: "+str(aliveNodes)+" Rels: "+str(aliveRelationships))
        self.plot(aliveNodes, 0.0, deleteMethod.__name__)
        self.plot(0, 0.0, deleteMethod.__name__)
        
    def deleteNode(self, deleteMethod, aliveNodes = 0):
        deletedNodes = 0
        deletedRels = 0
        if deleteMethod == self.failurePointed or deleteMethod == self.failurePointing:
            ids = deleteMethod(aliveNodes)
            for _id in ids:
                while True:
                    try:
                        r = self.queryDeleteNode.execute_one(Id = _id[0])
                        break
                    except SocketError:
                        print("Try again...")
                deletedNodes += 1
                deletedRels += r
        elif deleteMethod == self.failure:
            _id = deleteMethod(aliveNodes)
            r = self.queryDeleteNode.execute_one(Id = _id)
            deletedNodes = 1
            deletedRels = r
        else:
            _id = deleteMethod()
            while True:
                try:
                    r = self.queryDeleteNode.execute_one(Id = _id)
                    break
                except SocketError:
                    print("Try again...")
            deletedNodes = 1
            deletedRels = r
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
        return _id
    
    def attackByInDegree(self):
        try:
            _id = self.queryInDegreeNode.execute_one()
        except SocketError:
            print("Try again...")
            return self.attackByInDegree()
        return _id
    
    def attackByOutDegree(self):
        try:
            _id = self.queryOutDegreeNode.execute_one()
        except SocketError:
            print("Try again...")
            return self.attackByOutDegree()
        return _id

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
    
    def failure(self, aliveNodes):
        r = random.randrange(aliveNodes)
        try:
            ids = self.queryRandomNode.execute_one(R = r)
        except SocketError:
            print("Try again...")
            return self.failure(aliveNodes)
        return ids

    def failurePointed(self, aliveNodes):
        r = random.randrange(aliveNodes)
        try:            
            ids = self.queryRandomPointedNeighbours.stream(R = r)
        except SocketError:
            print("Try again...")
            return self.failurePointed(aliveNodes)
        return ids

    def failurePointing(self, aliveNodes):
        r = random.randrange(aliveNodes)
        try:       
            ids = self.queryRandomPointingNeighbours.stream(R = r)
        except SocketError:
            print("Try again...")
            return self.failurePointing(aliveNodes)
        return ids

    def defineAttackByByBetweeness(self):
        betweenessNode_qt_A = """MATCH (n)
                                SET n.o = 0;"""
        betweenessNode_qt_B = """MATCH p = shortestPath((source)-[*]->(destination))
                                WHERE source <> destination and length(nodes(p)) > 2
                                FOREACH (n in nodes(p) | SET n.o = n.o + 1);"""
        betweenessNode_qt_C = """MATCH (n)
                                RETURN Id(n)
                                ORDER BY n.o DESC
                                LIMIT 1;"""
        queryBetweenessA = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_A)
        queryBetweenessB = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_B)
        self.queryBetweenessC = neo4j.CypherQuery(self.driver.db, betweenessNode_qt_C)
        queryBetweenessA.execute()
        print("Attack by betweeness defined [1/2]")
        queryBetweenessB.execute()
        print("Attack by betweeness defined [2/2]")
        
    def attackByBetweeness(self):
        return self.queryBetweenessC.execute_one()
    
    def defineAttackByPageRank(self, alpha = 0.85, it_count = 50, k = 0):
        pageRank = PageRank()
        self.orderedView = pageRank.rank(alpha, it_count, k)
            
    def attackByPageRank(self, _id = None):
        (_, _id) = self.orderedView.pop(0)
        return _id