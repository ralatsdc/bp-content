# All code based on NLTK documentation

# K-Means clusterer
# example from figure 14.9, page 517, Manning and Schutze 
 
from __future__ import division
from nltk import cluster
from nltk.cluster import euclidean_distance
from numpy import array

print "K-Means clustering"
vectors = [array(f) for f in [[2, 1], [1, 3], [4, 7], [6, 7]]] 
means = [[4, 3], [5, 5]] 
   
clusterer = cluster.KMeansClusterer(2, euclidean_distance, initial_means=means) 
clusters = clusterer.cluster(vectors, True, trace=True) 

print 'Clustered:', vectors 
print 'As:', clusters 
print 'Means:', clusterer.means() 
print 
   
vectors = [array(f) for f in [[3, 3], [1, 2], [4, 2], [4, 0], [2, 3], [3, 1]]] 
       
# test k-means using the euclidean distance metric, 2 means and repeat 
# clustering 10 times with random seeds 
   
clusterer = cluster.KMeansClusterer(2, euclidean_distance, repeats=10) 
clusters = clusterer.cluster(vectors, True) 

print 'Clustered:', vectors 
print 'As:', clusters 
print 'Means:', clusterer.means() 
print 
   
# classify a new vector 
vector = array([3, 3]) 
print 'classify(%s):' % vector, 
print clusterer.classify(vector) 
print

print
print "GAAC Clustering"
print
# use a set of tokens with 2D indices 
vectors = [array(f) for f in [[3, 3], [1, 2], [4, 2], [4, 0], [2, 3], [3, 1]]] 

# test the GAAC clusterer with 4 clusters 
clusterer = cluster.GAAClusterer(4) 
clusters = clusterer.cluster(vectors, True) 
 
print 'Clusterer:', clusterer 
print 'Clustered:', vectors 
print 'As:', clusters 
print 
      
# show the dendrogram 
print "The dendogram"
print
clusterer.dendrogram().show() 
    
# classify a new vector
print "Classify the vector [3,3]"
vector = array([3, 3]) 
print 'classify(%s):' % vector, 
print clusterer.classify(vector) 
print
