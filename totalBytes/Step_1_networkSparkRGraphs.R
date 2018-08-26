#https://bookdown.org/alizaidi/mrs-spark-ml/starting-your-machine-learning-pipeline.html#creating-a-spark-context
#https://hortonworks.com/blog/integrate-sparkr-and-r-for-better-data-science-workflow/

library(networkD3)
library('ggplot2')
library('forecast')
library('tseries')
library('igraph')
library('GGally')
library('network')
library('SparkR')
library('yaml')

#Setting base directory
setwd("/home/ubuntu/datafile/network") 

props = yaml.load_file("config/config.yml")

Sys.setenv(SPARK_HOME = props$config$sparkHome)
#, spark.executor.cores = "1",spark.executor.memory ="2g",, spark.shuffle.service.enabled = "true", spark.scheduler.pool = "pool1"
sparkConfig <- list(spark.executor.instance = '2', spark.executor.cores = "2",
                    spark.executor.memory ="6g", spark.dynamicAllocation.enabled = "false", spark.task.maxFailures = "1000")#, spark.driver.maxResultSize ="4g")

credentials <-list(auth_url = props$config$auth_url, project = props$config$project, username = props$config$username, password = props$config$password, container = props$config$container, filename = props$config$filename, name = props$config$name, endpoint = props$config$endpoint)

sc_session <- sparkR.session(master = props$config$master, appName = props$config$appName, sparkHome = Sys.getenv("SPARK_HOME"), sparkJars = props$config$sparkJars,  sparkConfig = sparkConfig)

setHadoopConfig <- function(sc_session, credentials) {
  prefix = paste("fs.swift.service" , credentials[['name']], sep =".")
  sc = SparkR::sparkR.callJMethod(sc_session, "sparkContext")
  hConf = SparkR:::callJMethod(sc, "hadoopConfiguration")
  SparkR:::callJMethod(hConf, "set", "fs.swift.service.sahara.auth.url", paste(credentials[["auth_url"]],"/v2.0/tokens/",sep=""))
  SparkR:::callJMethod(hConf, "set", "fs.swift.service.sahara.tenant", credentials[["project"]])
  SparkR:::callJMethod(hConf, "set", "fs.swift.service.sahara.username", credentials[["username"]])
  SparkR:::callJMethod(hConf, "set", "fs.swift.service.sahara.password", credentials[["password"]])
  SparkR:::callJMethod(hConf, "set", "fs.swift.service.sahara.auth.endpoint.prefix", credentials[["endpoint"]])
}

setHadoopConfig(sc_session, credentials)
filePath <- paste("swift://" , credentials[['container']] , "." , credentials[['name']] , "/" , credentials[['filename']], sep="")

#file.names <-     system("swift -A https://kaizen.massopen.cloud:5000/v2.0 -V 2 --os-username  --os-password  --os-tenant-id TENANT_ID list batch1", intern=TRUE)
#container.names <-system("swift -A https://kaizen.massopen.cloud:5000/v2.0 -V 2 --os-username  --os-password  --os-tenant-id TENANT_ID list -p batch", intern=TRUE)
  
SparkR.DataFrameResults <-read.df("swift://networkdata.sahara/totalBytesFilebatch*.csv", source = "csv", header = "false")
SparkR.DataFrameResults<-withColumnRenamed(SparkR.DataFrameResults,"_c0", "index")
SparkR.DataFrameResults<-withColumnRenamed(SparkR.DataFrameResults, "_c1", "timeoriginal")
SparkR.DataFrameResults<-withColumnRenamed(SparkR.DataFrameResults,"_c2", "data")
head(SparkR.DataFrameResults)

results <- withColumn(SparkR.DataFrameResults, "time", monotonically_increasing_id())
head(results)
nrow(results)
class(results)
write.df(results, append=FALSE, col.names=F, file = "/dev/shm/results.csv", sep = ",")
results_df <- collect(results)

View(results_df)
####################################################
for(i in 1:length(file.names)){
  #out.file <- rbind(out.file, file)
  
  #nrow(b2)#b2 <- read.df("file://localhost/home/ubuntu/datafile/xaa", source = "csv", header = "true")
  #SparkR.DataFrame3 <- read.df("swift://batch1.sahara/data_2018-02-10T0050_redacted.csv", source = "csv", header = "true")
  file =  paste("swift://network.sahara/", file.names[i], sep="")
  SparkR.DataFrameGraph <-read.df(file, source = "csv", header = "true")
  
  #SparkR.DataFrame3 <- union(SparkR.DataFrame1, SparkR.DataFrame2)
  #persist(SparkR.DataFrame3, "MEMORY_ONLY")
  
  #SparkR.DataFrame3$'Total Bytes' <-  cast(gsub(",", "",SparkR.DataFrame3$'Total Bytes'))
  SparkR.DataFrame3$destinationbytes <- cast(alias(regexp_replace(SparkR.DataFrame3$destinationbytes, ",", ""), "destinationbytes"), "int")
  SparkR.DataFrame3$sourcebytes <- cast(alias(regexp_replace(SparkR.DataFrame3$sourcebytes, ",", ""), "sourcebytes"), "int")
  SparkR.DataFrame3$firstpackettimemod <- SparkR.DataFrame3$firstpackettime/1000
  #SparkR.DataFrame3$firstpackettimemod <- from_unixtime(SparkR.DataFrame3$firstpackettimemod)
  #str(SparkR.DataFrame3)
  
  #newdf = SparkR.DataFrame3.withColumn('total Bytes', sum(df[col] for col in df.columns))
  SparkR.DataFrame3$'total_bytes' <- SparkR.DataFrame3$sourcebytes  +  SparkR.DataFrame3$destinationbytes
  #str(SparkR.DataFrame3)
  
  SD3_S <-(select(SparkR.DataFrame3, SparkR.DataFrame3$firstpackettimemod, SparkR.DataFrame3$lastpackettime, SparkR.DataFrame3$firstpackettime,SparkR.DataFrame3$sourceip, SparkR.DataFrame3$destinationip, SparkR.DataFrame3$`total_bytes`, SparkR.DataFrame3$protocol, SparkR.DataFrame3$destinationport, SparkR.DataFrame3$destinationport)) 

  hourly_data <-summarize(groupBy(SD3_S, SD3_S$firstpackettimemod), total_bytes=sum(SD3_S$`total_bytes`))
  #str(hourly_data)
  #nrow(hourly_data)
  
  hourly_data_df <- collect(hourly_data)
  #View(hourly_data_df)
  #str(hourly_data_df)
  
  class(hourly_data_df$firstpackettimemod) = c('POSIXt','POSIXct')
  #hourly_data_df$FirstPacketTimeMod2 <- as.POSIXct(strptime(hourly_data_df$firstpackettime, format="%b %d, %Y, %H:%M:%S %t"))
  hourly_data_df <- hourly_data_df[order(hourly_data_df$firstpackettimemod),]
  #View(hourly_data_df)
  #hourly_data_df <- na.omit(hourly_data_df)
  #summary(hourly_data_df$total_bytes)
  #quantile(hourly_data_df$total_bytes, c(.32, .57, .95)) 
  
  #ggplot(hourly_data_df, aes(firstpackettimemod, total_bytes, group = 1)) +
  #geom_line() + ylim(c(0,1.943e+08)) +
  #labs(x = "Time", y = "Total Bytes", 
  #     title = "Total Bytes vs. Time")
  
  #z <- df[hourly_data_df$total_bytes > quantile(hourly_data_df$total_bytes, .1) - 1.5*IQR(hourly_data_df$total_bytes) & hourly_data_df$total_bytes < quantile(hourly_data_df$total_bytes, .9) + 1.5*IQR(hourly_data_df$total_bytes)]
  
  colnames(hourly_data_df)[which(names(hourly_data_df) == "total_bytes")] <- "data"
  #View(hourly_data_df)
  #logFile <- file("/dev/shm/test.log", "w")
  #totalBytesFile <- file("/dev/shm/totalBytesFile.csv", "w")
  
  cat(file.names[i], file = "/dev/shm/test.log",append = TRUE, sep ="\n" )
  write.table(hourly_data_df, append=TRUE,col.names=F,file = "/dev/shm/totalBytesFile.csv", sep = ",")
  
  #close(logFile)
  #close(totalBytesFile)
  #write.csv(port, file ="port.csv")
  #write.csv(protocol, file = "protocol.csv")
  
  #dataset_hourly_data_df = read.csv('/home/ubuntu/datafile/hourly_data_df.csv', header=TRUE, stringsAsFactors = FALSE)
  #str(dataset_hourly_data_df)
}

#write.table(out.file, file = "cand_Brazil.txt",sep=";", row.names = FALSE, qmethod = "double",fileEncoding="windows-1252")
##################graph#########################################
str(SparkR.DataFrame)
dim(SparkR.DataFrame)
network_data<- select(SparkR.DataFrame, 'Source_IP', 'Destination_IP')
                            
class(network_data)
#df_dups <- network_data[c(4,6)]
dim(network_data)s
#Eliminate dups
test<-distinct(network_data)
#test<-network_data[!duplicated(network_data),]
dim(test)
class(test)
r_df <- collect(test) 
networkData <- data.frame(r_df[,1],r_df[,2])
#simpleNetwork(networkData)

rm(list=ls())
sparkR.session.stop()