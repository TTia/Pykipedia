Pykipedia
=========

Wannabe a crawler.

"""
		-M[_,_] = 0
		-Imposto a 1 M[i,j] se j è vicino di i (stessa query di PR)
		-cambiato = false
		-do
			-for_i 
				for_j
					for_k
						se M[j,k] == 0 || i == k
							continue
						se M[j,k] + M[i,j] < M[i,k]
							|| M[i,k] == 0
								M[i,k] = M[j,k]+M[i,j]
								cambiato = true
		-while cambiato
		"""

START n = node(32), m = node(3373)
MATCH path = shortestPath((n)-[*]->(m))
RETURN extract(x in nodes(path) | Id(x))