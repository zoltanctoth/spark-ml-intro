if (!"SparkR" %in% installed.packages()){ 
  # Install Spark and SparkR
  SPARK_INSTALL_DIR="/opt/spark-1.5"
  SNAPSHOT_NAME="spark-1.5.0-SNAPSHOT-bin-hadoop2.6"
  if (Sys.getenv("SPARK_HOME") == ""){
    if(!dir.exists(SPARK_INSTALL_DIR)){
      dir.create(SPARK_INSTALL_DIR)
      download.file(paste("http://people.apache.org/~pwendell/spark-nightly/spark-master-bin/latest/",SNAPSHOT_NAME,".tgz",sep=""),
                    paste(SPARK_INSTALL_DIR,"/",SNAPSHOT_NAME,".tgz",sep=""))
      wd = getwd()
      setwd(SPARK_INSTALL_DIR)
      untar(paste(SPARK_INSTALL_DIR,"/",SNAPSHOT_NAME,".tgz",sep=""), compressed=TRUE)
      setwd(wd)
    }
    SPARK_HOME=paste(SPARK_INSTALL_DIR,"/",SNAPSHOT_NAME,sep="")
    Sys.setenv("SPARK_HOME"=SPARK_HOME)
  }
  print(Sys.getenv("SPARK_HOME"))
  
  
  #Start SparkR
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  print(.libPaths())
  Sys.setenv("PATH" = paste(Sys.getenv("PATH"),file.path(Sys.getenv("SPARK_HOME"), "bin"),sep=":"))
  print(Sys.getenv("PATH"))
}

printe <- function(x) write(x, stderr())

# RUN!
library(SparkR)

sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)

training = SparkR::read.df(sqlContext, "/opt/data/flight-training.nonulls.onlyints.parquet")
test = SparkR::read.df(sqlContext, "/opt/data/flight-test.nonulls.onlyints.parquet")

rm_hive_keywords <-c("FUNCTION", "OVERWRITE", "FUNCTIONS", "ROW", "CLUSTERSTATUS", "ENABLE", "NOT", "UNDO", "DIRECTORIES", "INPUTDRIVER", "DBPROPERTIES", "IMPORT", "CREATE", "RECORDWRITER", "USING", "PRECEDING", "SERDEPROPERTIES", "REGEXP", "ANALYZE", "EXPORT", "SHOW", "DISTRIBUTE", "DISABLE", "SHARED", "PRESERVE", "GROUPING", "ASC", "ELSE", "MATERIALIZED", "LOAD", "UTC_TMESTAMP", "INT", "RLIKE", "TABLESAMPLE", "RESTRICT", "$VALUE$", "MSCK", "INTERSECT", "GROUP", "FETCH", "MORE", "OUT", "READ", "USER", "DESCRIBE", "LOCATION", "BUCKET", "SKEWED", "SERDE", "LOCKS", "END", "UNLOCK", "RENAME", "ALTER", "OPTION", "PARTITIONED", "NOSCAN", "LINES", "TABLE", "VARCHAR", "FLOAT", "SCHEMAS", "$ELEM$", "DATABASES", "AS", "THEN", "REPLACE", "NO_DROP", "LEFT", "TRUNCATE", "COLUMN", "PARTIALSCAN", "PLUS", "EXISTS", "PRETTY", "LIKE", "ADD", "OUTER", "BY", "UNIQUEJOIN", "DELIMITED", "OUTPUTFORMAT", "TO", "SORT", "SET", "RIGHT", "ITEMS", "HAVING", "MINUS", "IGNORE", "READONLY", "SEMI", "UNION", "CURRENT", "CHANGE", "COLUMNS", "SCHEMA", "UNSIGNED", "DATABASE", "DECIMAL", "DROP", "BIGINT", "WHEN", "INPUTFORMAT", "STREAMTABLE", "READS", "ROWS", "DIRECTORY", "REVOKE", "FORMAT", "LONG", "UNARCHIVE", "TOUCH", "BETWEEN", "STRING", "FIRST", "CAST", "EXTERNAL", "CLUSTERED", "RECORDREADER", "WHILE", "ARCHIVE", "SETS", "STORED", "COMPUTE", "TRIGGER", "CASE", "CHAR", "UNSET", "CASCADE", "TERMINATED", "EXPLAIN", "EXTENDED", "REPAIR", "FULL", "INSERT", "LESS", "TINYINT", "BOTH", "DOUBLE", "PROTECTION", "OFFLINE", "COMMENT", "SELECT", "INTO", "ARRAY", "SORTED", "VIEW", "ROLE", "FILEFORMAT", "ROLLUP", "NULL", "ON", "DELETE", "LOCAL", "OF", "FORMATTED", "TABLES", "IDXPROPERTIES", "HOLD_DDLTIME", "OUTPUTDRIVER", "SSL", "CUBE", "STATISTICS", "OR", "INPATH", "ESCAPED", "USE", "FROM", "UNBOUNDED", "FALSE", "TEMPORARY", "RCFILE", "DISTINCT", "CURSOR", "TIMESTAMP", "OVER", "WHERE", "INNER", "ORDER", "LIMIT", "BUCKETS", "TEXTFILE", "MAPJOIN", "TRANSFORM", "UPDATE", "FOR", "DEFERRED", "EXCLUSIVE", "FOLLOWING", "AND", "UTC", "CROSS", "ORC", "REBUILD", "LOCK", "IF", "INDEX", "BOOLEAN", "IN", "CONTINUE", "$KEY$", "PARTITION", "IS", "PARTITIONS", "ALL", "EXCHANGE", "COLLECTION", "WITH", "GRANT", "PERCENT", "LATERAL", "DATETIME", "RANGE", "INDEXES", "STRUCT", "PURGE", "BEFORE", "DEPENDENCY", "FIELDS", "AFTER", "TRUE", "PROCEDURE", "JOIN", "SEQUENCEFILE", "CONCATENATE", "SHOW_DATABASE", "UNIONTYPE", "MAP", "TBLPROPERTIES", "CLUSTER", "WINDOW", "LOGICAL", "KEYS", "DESC", "BINARY", "DATE", "MACRO", "DATA", "REDUCE", "SMALLINT")
get_canonical_name <- function(name, default_prefix_underscore="attribute"){
  if (is.null(name) || is.na(name) || length(name) == 0){
    return(name)
  }
  if (length(grep("^[0-9]+$", name)) > 0 || (toupper(name) %in% rm_hive_keywords)){
    return(paste(name, "_", sep=""))
  }
  canonical_name = gsub("[^0-9a-zA-Z]", "_", name)
  if (substring(canonical_name, 1, 1) == '_'){
    canonical_name = paste(default_prefix_underscore, canonical_name, sep="")
  }
  return(tolower(canonical_name))
}
canonize_field_names <- function(df){
  corrected_df = df
  for (field in schema(df)$fields()){
    field_name = field$name()
    canonical_name = get_canonical_name(field_name)
    if (field_name != canonical_name){
      print(paste(corrected_df, field_name, canonical_name))
      corrected_df = withColumnRenamed(corrected_df, field_name, canonical_name)
      withColumnRenamed()
    }
  }
  return(corrected_df)
}

#### ----------------------------
rm_main = function(training, test)
{
  model <- glm(is_late ~ month, training, family = "binomial")
  predicted.data <- predict(model, test)
  
  predicted.data$features <- NULL
  predicted.data$rawPrediction <- NULL
  predicted.data$probability <- NULL
  
  return(predicted.data)
}

#### ----------------------------
results <- rm_main(training,test)

#### With SELECT ----------------
rm_main_2 = function(training, test)
{
  model <- glm(is_late ~ month + dayofweek + deptime + distance, 
               training, 
               family = "binomial")
  
  predicted <- predict(model, test)
  
  result = select(predicted, "month", "dayofweek", "deptime", "distance", "prediction");
  return(result)
}
#### ----------------------------

outputs = c()
if (class(results) != "list"){
  outputs = c(results)
} else {
  outputs = results
}
if (class(outputs[[1]]) != "DataFrame"){
  stop(paste("Error: outputs[[1]] is not an instance of DataFrame, but ", class(outputs[[1]]), sep=""))
}
canonized_output = canonize_field_names(outputs[[1]])

#write.df(canonized_output,"/tmp/radoop/zoltanctoth/tmp_1442651351237_csrycmi/","parquet","overwrite")
#schemaInfo = sapply(schema(canonized_output)$fields(), function(field) { paste(field$name(),";",field$dataType.simpleString(),sep = "") })
#SparkR:::saveAsTextFile(SparkR:::coalesce(SparkR:::parallelize(sc, schemaInfo), 1), "/tmp/radoop/zoltanctoth/tmp_1442651351237a_im6h4fa/")
#sparkR.stop()
