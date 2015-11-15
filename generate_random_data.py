import pandas
import numpy

data = pandas.DataFrame(numpy.random.rand(1000, 3))
data[0] = numpy.abs(numpy.ceil((data[1] - data[2])/1000).astype(float))
data.to_csv("data/logreg.txt", sep=" ", header=False, index=False)
