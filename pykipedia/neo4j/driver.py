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
	def createNode(self):
		pass
	def createMultipleNodes(self):
		pass
	def createEdge(self):
		pass
	def createMultipleEdges(self):
		pass
	
	def testConnection(self):
		graphDb = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
		graphDb.neo4j_version()
		return "!"
		#print(a,b,c,d)
		
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
query = neo4j.CypherQuery(graph_db, "MATCH (t:Tag) RETURN t;")
for record in query.stream():
	print(record[0])
print("Done")