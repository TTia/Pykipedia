'''
Created on 22/ott/2013

@author: Mattia
'''
import datetime
import unittest
from unittest.mock import Mock

from pykipedia.neo4j.driver import Driver
import xml.dom.minidom as MiniDOM
import xml.etree.ElementTree as ET


class DriverUnitTesting(unittest.TestCase):	
	
	def test_emptyDB(self):
		gen = GexfGenerator()
		gen.generateGexfFile(self.__buildNeo4JMockDriver(0,0))
		assert(self.__validateGexfFile())

	def test_onlyNodes(self):
		gen = GexfGenerator()
		gen.generateGexfFile(self.__buildNeo4JMockDriver(10,0))
		assert(self.__validateGexfFile())
	
	def test_onlyEdges(self):
		gen = GexfGenerator()
		gen.generateGexfFile(self.__buildNeo4JMockDriver(0,10))
		assert(self.__validateGexfFile())	
	
	def test_nodesAndEdges(self):
		gen = GexfGenerator()
		gen.generateGexfFile(self.__buildNeo4JMockDriver(10,10))
		assert(self.__validateGexfFile())

	'''
	Stress Test - Mocked
	(test_#Nodes_#Edges)
	'''
	def test_1k_1k(self):
		self.__runStressTest(1000, 1000, "1k, 1k")		

	def test_10k_10k(self):
		self.__runStressTest(10000, 10000, "10k, 10k")

	def test_100k_100k(self):
		self.__runStressTest(100000, 100000, "100k, 100k")
		
	def test_1kk_1kk(self):
		self.__runStressTest(1000000, 1000000, "1kk, 1kk")
	
	def __runStressTest(self, N, M, label = ""):
		gen = GexfGenerator()
		gen.generateGexfFile(self.__buildNeo4JMockDriver(N,M))

		#assert(self.validateGexfFile())
	
	def __buildNeo4JMockDriver(self, N, M):
		driver = Driver()
		driver.getNodes = Mock(return_value = self.__generateNodes(N))
		driver.getEdges = Mock(return_value = self.__generateEdges(M))
		return driver
			
	def __generateEdges(self, M):
		'''
		MATCH (sx:Page)-[l:LinkedTo]->(dx:Page) RETURN Id(l) as eId, Id(sx) as Id1, Id(dx) as Id2;
		'''
		i = 0		
		while i<M:
			yield {"eId" : i, "Id1" : i+1, "Id2": i+2}
			i += 1
		
	def __generateNodes(self, N):
		'''
		MATCH (n:Page) RETURN Id(n) as Id, n.url, n.title;
		'''
		i = 0		
		while i<N:
			pagina = "Pagina%s" % str(i)
			yield {"Id" : i, "url": "wiki.it/%s" % pagina, "title": pagina}
			i += 1
	
	def __validateGexfFile(self):
		ET.parse("wikipedia.gexf")
		return True
		
class GexfGenerator:
	
	def generateGexfFile(self, driver, filename="wikipedia.gexf"):
		'''
		<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
		'''
		rootAttributes = {'xmlns': "http://www.gexf.net/1.2draft", 'version': "1.2"}
		root = ET.Element('gexf', attrib = rootAttributes);
		root.append(self.__generateMetadata())
		root.append(self.__generateGraph())
		
		self.gexfFile = open(filename,"w+")
		splittedXml = self.__indent(root).split("###")
		self.__writeToFile(splittedXml[0])
		
		for node in driver.getNodes():
			nodeGexf = self.__generateNode(node[0], node[1], node[2])
			#self.gexfFile.write(ET.tostring(nodeRappr, 'utf-8'))
			self.gexfFile.write(nodeGexf)
			
			
		splittedXml = splittedXml[1].split("@@@")
		self.gexfFile.write(splittedXml[0])
		
		for edge in driver.getEdges():
			edgeGexf = self.__generateEdge(edge[0], edge[1], edge[2])
			#self.gexfFile.write(ET.tostring(edgeRappr, 'utf-8'))
			self.gexfFile.write(edgeGexf)
		
		self.gexfFile.write(splittedXml[1])
		self.gexfFile.close()
	
	def __generateMetadata(self):
		'''
		<meta lastmodifieddate="2009-03-20">
			<creator>Gexf.net</creator>
			<description>A hello world! file</description>
		</meta>
		'''
		creator = ET.Element("creator");
		creator.text = "Pykipedia"
		description = ET.Element("description")
		description.text = "Wikipedia graph - Crawled with Pykipedia"
		meta = ET.Element("meta",\
						{"lastmodifieddate" : datetime.datetime.now().strftime("%Y-%m-%d")})
		
		meta.append(creator)
		meta.append(description)
		return meta
	
	def __generateGraph(self):
		'''
		<graph mode="static" defaultedgetype="directed">
		'''
		graph = ET.Element("graph", {"mode" : "static", "defaultedgetype" : "directed"})
		graph.append(self.__generateNodes())
		graph.append(self.__generateEdges())
		
		return graph

	def __generateNodes(self):
		nodes = ET.Element("nodes")
		nodes.text = "###"
		return nodes
	
	def __generateEdges(self):
		edges = ET.Element("edges")
		edges.text = "@@@"
		return edges
	
	def __generateNode(self, nodeId, url, title):
		nodeGexf = "\t\t<node id=\"{nodeId}\" label=\"{title}\" />\n"
		return nodeGexf.format(nodeId = nodeId, title = title)
		#return ET.Element("node", {"id": nodeId, "label": title})
	
	def __generateEdge(self, edgeId, sourceId, targetId):
		edgeGexf = "\t\t<edge id=\"{edgeId}\" source=\"{sourceId}\" target=\"{targetId}\" />\n"
		return edgeGexf.format(edgeId = edgeId, sourceId = sourceId, targetId = targetId)
		#return ET.Element("edge", {"id": edgeId, "source": sourceId, "target": targetId})

	def __indent(self, element):
		xmlText = ET.tostring(element, 'utf-8')
		xmlText = MiniDOM.parseString(xmlText)
		return xmlText.toprettyxml(indent="\t")
	
	def __writeToFile(self, xmlRappresentation):
		self.gexfFile.write(xmlRappresentation)
