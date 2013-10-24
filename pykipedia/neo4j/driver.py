'''
Created on 17/ott/2013
@author: Mattia
'''

from py2neo import neo4j
import unittest
import datetime
from random import randint

class DriverStressTest(unittest.TestCase):

	def setUp(self):
		self.driver = Driver()
		self.driver.resetDB()
	
	def tearDown(self):
		self.driver.resetDB()
		
	'''
	Stress Test - "Mocked" Crawler
	(test_#Nodes_#Edges)
	'''
	def test_1k_1k(self):
		self.__runStressTest(1000, 1000, "1k, 1k")		

	#def test_10k_10k(self):
	#	self.__runStressTest(10000, 10000, "10k, 10k")

	#def test_100k_100k(self):
	#	self.__runStressTest(100000, 100000, "100k, 100k")
	
	def __runStressTest(self, N, M, label = ""):
		print(label)
		startTime = datetime.datetime.now()
		
		for node in self.__generateNodes(N):
			self.driver.createNode([node["url"], node["title"]])
		
		for edge in self.__generateEdges(M):
			self.driver.createNode([edge["startUrl"], edge["endUrl"]])
					
		delta = (datetime.datetime.now()-startTime)
		print(divmod(delta.days * 86400 + delta.seconds, 60))
	
	def __generateEdges(self, M):
		edges = 0		
		while edges<M:
			startUrl = "wiki.it/Pagina%s" % str(randint(0, self.nodeCounter-1))
			endUrl = "wiki.it/Pagina%s" % str(randint(0, self.nodeCounter-1))
			
			if edges % 250 == 0:
				print("edges: "+str(edges))
				
			yield {"startUrl" : startUrl, "endUrl": endUrl}
			edges += 1
		
	def __generateNodes(self, N):
		self.nodeCounter = 0		
		while self.nodeCounter<N:
			pagina = "Pagina%s" % str(self.nodeCounter)
			
			if self.nodeCounter % 250 == 0:
				print("nodes: "+str(self.nodeCounter))
			
			yield {"url": "wiki.it/%s" % pagina, "title": pagina}
			self.nodeCounter += 1

class DriverUnitTesting(unittest.TestCase):	
	def setUp(self):
		self.driver = Driver()
		self.driver.resetDB()

	def tearDown(self):
		self.driver.resetDB()
		
	def test_isDBEmpty(self):
		assert(self.driver.countNodes() == 0)
		assert(self.driver.countRelationship() == 0)

	def test_createSingleNode(self):
		assert(self.driver.countNodes() == 0)
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		assert(self.driver.countNodes() == 1)
		assert(self.driver.countRelationship() == 0)

	def test_duplicateNode(self):
		assert(self.driver.countNodes() == 0)
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		assert(self.driver.countNodes() == 1)
		assert(self.driver.countRelationship() == 0)
	
	def test_duplicateRel(self):
		assert(self.driver.countNodes() == 0)
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		self.driver.createNode(["wiki.it/Pagina2","Pagina2"])
		self.driver.createEdge("wiki.it/Pagina", "wiki.it/Pagina2")
		
		assert(self.driver.countNodes() == 2)
		assert(self.driver.countRelationship() == 1)
		assert(self.driver.countRelationshipBeetween("wiki.it/Pagina", "wiki.it/Pagina2") == 1)

	def test_populateDB(self):
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		self.driver.createNode(["wiki.it/Pagina2","Pagina2"])
		self.driver.createNode(["wiki.it/Pagina3","Pagina3"])
		self.driver.createNode(["wiki.it/Pagina4","Pagina4"])
		self.driver.createEdge("wiki.it/Pagina", "wiki.it/Pagina2")
		self.driver.createEdge("wiki.it/Pagina", "wiki.it/Pagina3")
		self.driver.createEdge("wiki.it/Pagina2", "wiki.it/Pagina4")
		
		assert(self.driver.countNodes() == 4)
		assert(self.driver.countRelationship() == 3)
		assert(self.driver.countRelationshipBeetween("wiki.it/Pagina2", "wiki.it/Pagina") == 0)
	
	def test_queries(self):
		self.driver.createNode(["wiki.it/Pagina","Pagina"])
		self.driver.createNode(["wiki.it/Pagina2","Pagina2"])
		self.driver.createNode(["wiki.it/Pagina3","Pagina3"])
		self.driver.createNode(["wiki.it/Pagina4","Pagina4"])
		self.driver.createEdge("wiki.it/Pagina", "wiki.it/Pagina2")
		self.driver.createEdge("wiki.it/Pagina", "wiki.it/Pagina3")
		self.driver.createEdge("wiki.it/Pagina2", "wiki.it/Pagina4")
		
		nodes = 0
		for n in self.driver.getNodes():
			nodes += 1
		assert(self.driver.countNodes() == 4)
		assert(nodes == 4)
		edges = 0
		for n in self.driver.getEdges():
			edges += 1
		assert(self.driver.countRelationship() == 3)
		assert(edges == 3)

class Driver:
	def __init__(self):
		self.db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/") 

	def __getNodeLabel(self):
		return "Page"
	
	def __getNodeAttributes(self):
		return ["url","title"]
	
	def __getRelationshipLabel(self):
		return "LinkedTo"
	
	def __initDB(self):
		constraint_query = "CREATE CONSTRAINT ON (p:{Label}) ASSERT p.{Pk} IS UNIQUE;"
		constraint_query = constraint_query.format(Label = self.__getNodeLabel(),\
									Pk = self.__getNodeAttributes()[0])

		index_query = "CREATE INDEX ON :{Label}({Pk});"
		index_query = index_query.format(Label = self.__getNodeLabel(),\
							Pk = self.__getNodeAttributes()[0])
		
		try:
			self.__runQuery(constraint_query)
		except neo4j.CypherError:
			print("Already constrained.")
		try:
			self.__runQuery(index_query)
		except neo4j.CypherError:
			print("Already indexed.")
	
	def createNode(self, attributes):
		'''
		Crea un nuovo nodo sul db.
		:param attributes: lista degli attributi di un singolo nodo. [url, titolo]
		'''
		
		fields = self.__getNodeAttributes()
		
		query_text = "CREATE (p:{Label}{{ {Url}:'{UrlValue}', {Title}: '{TitleValue}' }});"
		query_text =  query_text.format(Label = self.__getNodeLabel(),\
									Url = fields[0], UrlValue = attributes[0],\
									Title = fields[1], TitleValue = attributes[1])
		self.__runQuery(query_text)
		
	def createEdge(self, startURL, endURL):
		'''
		Crea un nuovo arco orientato sul db.
		:param startURL: url della voce che contiene il link.
		:param endURL: url della voce a cui il link si riferisce. 
		'''
		'''
		query_text = ("MATCH (left:{LeftLabel}), (right:{RightLabel}) "
					"WHERE left.{LeftAttr} = '{LeftValue}' and right.{RightAttr} = '{RightValue}' "
					"CREATE UNIQUE (left)-[:{RelLabel}]->(right);")
		query_text =  query_text.format(LeftLabel = self.__getNodeLabel(),\
						RightLabel = self.__getNodeLabel(),\
						LeftAttr = self.__getNodeAttributes()[0], LeftValue = startURL,\
						RightAttr = self.__getNodeAttributes()[0], RightValue = endURL,\
						RelLabel = self.__getRelationshipLabel())		
		self.__runQuery(query_text)
		'''
		
	
	def __runQuery(self, query_text):	
		query = neo4j.CypherQuery(self.db, query_text)
		try:
			query.run()
		except neo4j.CypherError as ce:
			if(str(ce).find("already exist") == -1):
				raise ce
			
	def __executeOne(self, query_text):	
		query = neo4j.CypherQuery(self.db, query_text)
		return query.execute_one()
	
	def __iterateOverResult(self, query_text):		
		query = neo4j.CypherQuery(self.db, query_text)
		return query.stream()
		
	def countNodes(self):
		'''
		Restituisce il numero di nodi presenti sul db.
		'''
		query_text = "MATCH (p:{Label}) RETURN count(p) as nodi;";
		query_text = query_text.format(Label = self.__getNodeLabel())
		return self.__executeOne(query_text)
		
	def countRelationship(self):
		'''
		Restituisce il numero di relazioni presenti sul db.
		'''
		query_text = "MATCH ()-[r:{Label}]->() RETURN count(r) as archi;";
		query_text = query_text.format(Label = self.__getRelationshipLabel())
		return self.__executeOne(query_text)
	
	def countRelationshipBeetween(self, startURL, endURL):
		'''
		Restituisce il numero di link diretti dalla pagina 'startURL' a 'endURL'.
		'''		
		query_text = """MATCH (left:{LeftLabel})-[l:{RelLabel}]->(right:{RightLabel})
					\nWHERE left.{LeftAttr} = '{LeftValue}' and right.{RightAttr} = '{RightValue}'
					\nRETURN count(l) as archi;"""
		query_text = query_text.format(LeftLabel = self.__getNodeLabel(),\
						RelLabel = self.__getRelationshipLabel(),\
						RightLabel = self.__getNodeLabel(),\
						LeftAttr = self.__getNodeAttributes()[0], LeftValue = startURL,\
						RightAttr = self.__getNodeAttributes()[0], RightValue = endURL)	
									
		return self.__executeOne(query_text)
	
	def resetDB(self):
		'''
		Elimina ogni nodo e relazione presente sul DB.
		Se il DB viene creato ex-novo e' definito anche lo schema e
		gli indici.
		'''
		self.db.clear()
		self.__initDB()
	
	def getNodes(self):
		query_text = "MATCH (n:Page) RETURN Id(n) as Id, n.url, n.title;"
		return self.__iterateOverResult(query_text)
		
	def getEdges(self):
		query_text = "MATCH (sx:Page)-[l:LinkedTo]->(dx:Page) RETURN Id(l) as eId, Id(sx) as Id1, Id(dx) as Id2;"
		return self.__iterateOverResult(query_text)