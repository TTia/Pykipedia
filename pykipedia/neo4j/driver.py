'''
Created on 17/ott/2013
@author: Mattia
'''

from py2neo import neo4j

class Driver:
	
	def __defineConstraint__(self):
		pass
	
	def __defineIndex__(self):
		pass
	
	def __getConnection(self):
		return neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	
	def initDb(self):
		pass
	
	def createNode(self, title):
		'''
		Crea un nuovo nodo sul db.
		:param title: titolo della voce di Wikipedia
		'''
		graph_db = self.__getConnection()
		query_text = "CREATE UNIQUE (p:Page{title:'%s'});" % title
		query = neo4j.CypherQuery(graph_db, query_text)
		query.run()
	
	def createEdge(self, startTitle, endTitle):
		'''
		Crea un nuovo arco orientato sul db.
		:param startTitle: titolo della voce che contiene il link.
		:param endTitle: titolo della voce a cui il link si riferisce. 
		'''
		pass
	
	def createMultipleNodes(self):
		pass
	
	def createMultipleEdges(self):
		pass
	
	def __queryExample(self, label="Page"):
		graph_db = self.__getConnection()
		query_text = "MATCH (n:%s) RETURN n;" % label
		query = neo4j.CypherQuery(graph_db, query_text)
		# for record in query.stream():
		# 	print record[0]





