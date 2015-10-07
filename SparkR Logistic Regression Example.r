SPARK.HOME=paste("/opt/spark")
Sys.setenv("SPARK_HOME"=SPARK.HOME)

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
print(.libPaths())

Sys.setenv("PATH" = paste(Sys.getenv("PATH"),file.path(Sys.getenv("SPARK_HOME"), "bin"),sep=":"))
print(Sys.getenv("PATH"))

# We are all set, let's start up Spark
library(SparkR)

sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)

# Logistic Regression
data.dir = "/Users/zoltanctoth/src/my/spark-ml-intro/data/" # Change me!

training = SparkR::read.df(sqlContext, file.path(data.dir, "training.parquet"))
test = SparkR::read.df(sqlContext, file.path(data.dir, "test.parquet"))

is_late <- function(delay) SparkR::ceil(delay/100000)
training$label <- is_late(training$arrdelay)

head(training)

model <- glm(is_late ~ month + deptime, training, family = "binomial")
predicted.data <- predict(model, test)

printSchema(predicted.data)
predicted.data$features <- NULL

SparkR::write.df(predicted.data, file.path(data.dir,"prediction.json"), "json")
