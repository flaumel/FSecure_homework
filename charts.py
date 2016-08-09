from matplotlib import pyplot
from numpy import arange

# source : plotting.py : https://gist.github.com/nitstorm/9479926

import bisect

def barplot(labels, data):
    pos = arange(len(data))
    pyplot.xticks(pos + 0.4, labels)
    pyplot.bar(pos, data)
    pyplot.show()

def barchart(x, y, numbins = 5):
    datarange = max(x) - min(x)
    bin_width=float(datarange)/numbins
    pos=min(x)
    bins=[0 for i in range(numbins + 1)]

    for i in range(numbins):
        bins[i] = pos
        pos += bin_width
    bins[numbins] = max(x) + 1
    binsum=[0 for i in range(numbins)]
    bincount=[0 for i in range(numbins)]
    binaverage=[0 for i in range(numbins)]

    for i in range(numbins):
        for j in range(len(x)):
            if (x[j] >= bins[i] and x[j] < bins[i + 1]):
                bincount[i] += 1
                binsum[i] += y[j]

    for i in range(numbins):
        binaverage[i]=float(binsum[i]) / bincount[i]
    barplot(range(numbins), binaverage)
