install.packages("ggplot2")
data = read.table("data/logreg.txt", sep=" ", header=FALSE, col.names = c("label", "x", "y"))
library(ggplot2)
qplot(x=x, y=y, colour=label, data=data)
