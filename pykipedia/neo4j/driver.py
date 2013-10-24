'''
Created on 17/ott/2013
@author: Mattia
'''

from py2neo import neo4j
import unittest

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
	
	def __connect(self):
		return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	
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
		query_text = ("MATCH (left:{LeftLabel}), (right:{RightLabel}) "
					"WHERE left.{LeftAttr} = '{LeftValue}' and right.{RightAttr} = '{RightValue}' "
					"CREATE UNIQUE (left)-[:{RelLabel}]->(right);")
		query_text =  query_text.format(LeftLabel = self.__getNodeLabel(),\
						RightLabel = self.__getNodeLabel(),\
						LeftAttr = self.__getNodeAttributes()[0], LeftValue = startURL,\
						RightAttr = self.__getNodeAttributes()[0], RightValue = endURL,\
						RelLabel = self.__getRelationshipLabel())		
		self.__runQuery(query_text)
	
	def __runQuery(self, query_text):
		graph_db = self.__connect()		
		query = neo4j.CypherQuery(graph_db, query_text)
		try:
			query.run()
		except neo4j.CypherError as ce:
			if(str(ce).find("already exist") == -1):
				raise ce
			
	def __executeOne(self, query_text):
		graph_db = self.__connect()		
		query = neo4j.CypherQuery(graph_db, query_text)
		return query.execute_one()
	
	def __iterateOverResult(self, query_text):
		graph_db = self.__connect()		
		query = neo4j.CypherQuery(graph_db, query_text)
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
		self.__initDB()
		query_delete_rel = "MATCH ()-[l:{Label}]->() DELETE l;"
		query_delete_nodes = "MATCH (n:{Label}) DELETE n;"
		query_delete_rel = query_delete_rel.format(Label = self.__getRelationshipLabel())
		query_delete_nodes = query_delete_nodes.format(Label = self.__getNodeLabel())
		
		self.__runQuery(query_delete_rel)
		self.__runQuery(query_delete_nodes)
	
	def getNodes(self):
		query_text = "MATCH (n:Page) RETURN Id(n) as Id, n.url, n.title;"
		return self.__iterateOverResult(query_text)
		
	def getEdges(self):
		query_text = "MATCH (sx:Page)-[l:LinkedTo]->(dx:Page) RETURN Id(l) as eId, Id(sx) as Id1, Id(dx) as Id2;"
		return self.__iterateOverResult(query_text)