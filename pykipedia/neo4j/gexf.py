'''
Created on 22/ott/2013

@author: Mattia
'''
import xml.etree.ElementTree as ET
import xml.dom.minidom as MiniDOM
import datetime
import unittest
from unittest.mock import Mock
from pykipedia.neo4j.driver import Driver

class DriverUnitTesting(unittest.TestCase):	

	def buildNeo4JMockDriver(self, N, M):
		driver = Driver()
		driver.getNodes = Mock(return_value = self.generateNodes(N))
		driver.getEdges = Mock(return_value = self.generateEdges(M))
		return driver
			
	def generateEdges(self, M):
		'''
		MATCH (sx:Page)-[l:LinkedTo]->(dx:Page) RETURN Id(l) as eId, Id(sx) as Id1, Id(dx) as Id2;
		'''
		i = 0		
		while i<M:
			yield {"eId" : i, "Id1" : i+1, "Id2": i+2}
			i += 1
		
	def generateNodes(self, N):
		'''
		MATCH (n:Page) RETURN Id(n) as Id, n.url, n.title;
		'''
		i = 0		
		while i<N:
			pagina = "Pagina%s" % str(i)
			yield {"Id" : i, "url": "wiki.it/%s" % pagina, "title": pagina}
			i += 1
	
	def validateGexfFile(self):
		return False
		
class GexfGenerator:
	
	def generateGexfFile(self, driver):
		'''
		<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
		'''
		rootAttributes = {'xmlns': "http://www.gexf.net/1.2draft", 'version': "1.2"}
		root = ET.Element('gexf', attrib = rootAttributes);
		root.append(self.__generateMetadata())
		root.append(self.__generateGraph())
		
		splittedXml = self.__indent(root).split("###")
		self.__writeToFile(splittedXml[0])
		
		gexfFile = open("wikipedia.gexf","w+")

		for node in driver.getNodes():
			gexfFile.write(self.__generateNode(node["Id"], node["url"], node["title"]))
			
		splittedXml = splittedXml[1].split("@@@")
		gexfFile.write(splittedXml[0])
		
		for edge in driver.getEdges():
			gexfFile.write(self.__generateEdge(edge["eId"], edge["Id1"], edge["Id2"]))
		
		gexfFile.write(splittedXml[1])
		gexfFile.close()
	
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
		return ET.Element("node", {"id": nodeId, "label": title})
	
	def __generateEdge(self, edgeId, sourceId, targetId):
		return ET.Element("edge", {"id": edgeId, "source": sourceId, "target": targetId})

	def __indent(self, element):
		xmlText = ET.tostring(element, 'utf-8')
		xmlText = MiniDOM.parseString(xmlText)
		return xmlText.toprettyxml(indent="\t")
	
	def __writeToFile(self, xmlRappresentation):
		gexfFile = open("wikipedia.gexf","w")
		gexfFile.write(xmlRappresentation)
		gexfFile.close()

'''
    <graph mode="static" defaultedgetype="directed">
        <nodes>
            <node id="0" label="Hello" />
            <node id="1" label="Word" />
        </nodes>
        <edges>
            <edge id="0" source="0" target="1" />
        </edges>
    </graph>
'''