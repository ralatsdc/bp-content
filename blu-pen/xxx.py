# Demonstrates image clustering

from PIL import Image
from pylab import *
from numpy import *

import os
from PCV.clustering import hcluster

path = "/Users/raymondleclair/Projects/Blue-Peninsula/bp-content/content/authors/flickr"

imdirs = [os.path.join(path, d) for d in os.listdir(path) if d.startswith('by_')]

imfnms = []
[imfnms.extend([os.path.join(d, f) for f in os.listdir(d) if f.endswith('.jpg')]) for d in imdirs]

imfnms = imfnms[0:len(imfnms):60]

features = zeros([len(imfnms), 512])

for i, f in enumerate(imfnms):

    im = array(Image.open(f))

    h, edges = histogramdd(im.reshape(-1, 3), 8, normed=True, range=[(0, 255), (0, 255), (0, 255)])

    features[i] = h.flatten()

tree = hcluster.hcluster(features)
hcluster.draw_dendrogram(tree, imfnms, filename="xxx.png")
