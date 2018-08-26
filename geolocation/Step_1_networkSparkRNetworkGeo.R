#https://bookdown.org/alizaidi/mrs-spark-ml/starting-your-machine-learning-pipeline.html#creating-a-spark-context
#https://hortonworks.com/blog/integrate-sparkr-and-r-for-better-data-science-workflow/

#install.packages("reshape2")
#remove.packages("reshape", .libPaths())

library('GGally')
library('network')
library('SparkR')
library('reshape2')
library('yaml')

#.libPaths()

#Setting base directory
#setwd("home/ubuntu/datafile/network") 

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

getDataFromObjectStore <-  function(myFile){
  SparkR.DF <-read.df(myFile, source = "csv", header = "true")
  return(SparkR.DF)
}

myWhileLoop <- function(myFile){
  errorMessage = paste("Attempting myWhileLoop for " , myFile, sep="")
  print(errorMessage)
  SparkR.DF <- NULL
  while(TRUE) {
    tryCatch({
      SparkR.DF <- getDataFromObjectStore(myFile)
      break;
    }, error = function(e) {
      errorMessage = paste("I had error processing " , file, sep="")
      print(errorMessage)
      cat(errorMessage, file = logFile,append = TRUE, sep ="\n" )
      #.rs.restartR() 
      sparkR.session.stop()
      sc_session <- sparkR.session(master = props$config$master, appName = props$config$appName, sparkHome = Sys.getenv("SPARK_HOME"), sparkJars = props$config$sparkJars,  sparkConfig = sparkConfig)
      setHadoopConfig(sc_session, credentials)
      errorMessage = paste("Attempting to call myWhileLoop after error for " , myFile, sep="")
      print(errorMessage)
      SparkR.DF <- myWhileLoop(myFile)
      Sys.sleep(2)
    })
  }
  return(SparkR.DF)
}

setHadoopConfig(sc_session, credentials)
filePath <- paste("swift://" , credentials[['container']] , "." , credentials[['name']] , "/" , credentials[['filename']], sep="")

container.names <- NULL
while(is.null(container.names)){
  container.names <-system("swift -A https://kaizen.massopen.cloud:5000/v2.0 -V 2 --os-username  --os-password  --os-tenant-id TENANT_ID list -p batch", intern=TRUE)
  
}
container.names
Sys.time()
#length(container.names)
for(j in 1:length(container.names)) {
  swiftCommand <- paste("swift -A https://kaizen.massopen.cloud:5000/v2.0 -V 2 --os-username  --os-password  --os-tenant-id TENANT_ID list", container.names[j], sep = " ")
  
  file.names <- NULL
  while(is.null(file.names)){
    file.names <-  system(swiftCommand, intern=TRUE)
  }
  
  logFile <- paste("/dev/shm/test_networkgeo", container.names[j], ".log", sep="")
  dataFileNetworkGraph <- paste("/dev/shm/networkGraphFile",container.names[j], ".csv", sep="")
  dataFileGeo <- paste("/dev/shm/geoFile",container.names[j], ".csv", sep="")
  
  dataFileGeoSource <- paste("/dev/shm/geoSourceFile",container.names[j], ".csv", sep="")
  dataFileGeoDest <- paste("/dev/shm/geoDestFile",container.names[j], ".csv", sep="")
  
  #length(file.names)
  for(i in 92:length(file.names)){
    file =  paste("swift://", container.names[j], ".sahara/", file.names[i], sep="")
    SparkR.DF <- myWhileLoop(file)
    
    #browser()
    #head(SparkR.DF)
    #persist(SparkR.DF, "MEMORY_ONLY")
    
    SparkR.DF$destinationbytes <- cast(alias(regexp_replace(SparkR.DF$destinationbytes, ",", ""), "destinationbytes"), "int")
    SparkR.DF$sourcebytes <- cast(alias(regexp_replace(SparkR.DF$sourcebytes, ",", ""), "sourcebytes"), "int")
    SparkR.DF$firstpackettimemod <- SparkR.DF$firstpackettime/1000
    SparkR.DF$firstpackettimemod <- from_unixtime(SparkR.DF$firstpackettimemod, 'yyyy-MM-dd HH')
    #nrow(SparkR.DF)
    #head(SparkR.DF)
    SparkR.DF <- filter(SparkR.DF, SparkR.DF$firstpackettimemod >= "2018-02-10 00" & SparkR.DF$firstpackettimemod<= "2018-02-20 00")
    
    SparkR.DF$'total_bytes' <- SparkR.DF$sourcebytes + SparkR.DF$destinationbytes
    
    hourly_geo_source_df <-summarize(groupBy(SparkR.DF, SparkR.DF$firstpackettimemod, SparkR.DF$sourcesubnet), total_bytes=sum(SparkR.DF$'total_bytes'))
    hourly_geo_dest_df <-summarize(groupBy(SparkR.DF, SparkR.DF$firstpackettimemod, SparkR.DF$destinationsubnet), total_bytes=sum(SparkR.DF$'total_bytes'))
    
    hourly_geo_source_df_2 <- collect (hourly_geo_source_df)
    hourly_geo_dest_df_2 <- collect (hourly_geo_dest_df)
    
    cat(file.names[i], file = logFile,append = TRUE, sep ="\n" )

    write.table(hourly_geo_source_df_2, append=TRUE, col.names=F, row.names=F, file = dataFileGeoSource, sep = ",")
    write.table(hourly_geo_dest_df_2, append=TRUE, col.names=F, row.names=F, file = dataFileGeoDest, sep = ",")
  }
}

Sys.time()
sparkR.session.stop()
