"""
Non-interactive demonstration of the clusterers with simple 2-D data.
"""

import numpy
from nltk.cluster import GAAClusterer

# use a set of tokens with 2D indices
vectors = [numpy.array(f) for f in [[3, 3], [1, 2], [4, 2], [4, 0], [2, 3], [3, 1]]]

# test the GAAC clusterer with 4 clusters
clusterer = GAAClusterer(4)
clusters = clusterer.cluster(vectors, True)

print('Clusterer:', clusterer)
print('Clustered:', vectors)
print('As:', clusters)
print()

# show the dendrogram
clusterer.dendrogram().show()
