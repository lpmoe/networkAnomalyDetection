#https://bookdown.org/alizaidi/mrs-spark-ml/starting-your-machine-learning-pipeline.html#creating-a-spark-context
#https://hortonworks.com/blog/integrate-sparkr-and-r-for-better-data-science-workflow/

library('GGally')
library('network')
library('SparkR')
library('yaml')

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

  logFile <- paste("/dev/shm/test", container.names[j], ".log", sep="")
  #dataFile <- paste("swift://networkdata.sahara/totalBytesFile",container.names[j], ".csv", sep="")
  dataFile <- paste("/dev/shm/totalBytesFile",container.names[j], ".csv", sep="")
  dataFileSourceDestination <- paste("/dev/shm/SourceDestination",container.names[j], ".csv", sep="")
  dataFileDistinctMachines <- paste("/dev/shm/DistinctMachines",container.names[j], ".csv", sep="")
  dataFileSpecificPort <- paste("/dev/shm/SpecificPort",container.names[j], ".csv", sep="")
  dataFileSpecificMachine <- paste("/dev/shm/SpecificMachine",container.names[j], ".csv", sep="")
  
  #length(file.names)
  for(i in 1:length(file.names)){
    file =  paste("swift://", container.names[j], ".sahara/", file.names[i], sep="")
    SparkR.DF <- myWhileLoop(file)
   
    browser()
    #SparkR.DF <- union(SparkR.DataFrame1, SparkR.DataFrame2)
    #persist(SparkR.DF, "MEMORY_ONLY")
    
    #SparkR.DF$'Total Bytes' <-  cast(gsub(",", "",SparkR.DF$'Total Bytes'))
    SparkR.DF$destinationbytes <- cast(alias(regexp_replace(SparkR.DF$destinationbytes, ",", ""), "destinationbytes"), "int")
    SparkR.DF$sourcebytes <- cast(alias(regexp_replace(SparkR.DF$sourcebytes, ",", ""), "sourcebytes"), "int")
    SparkR.DF$firstpackettimemod <- SparkR.DF$firstpackettime/1000
    #SparkR.DF$lastpackettimemod <- SparkR.DF$lastpackettime/1000
    #SparkR.DF$firstpackettimemod <- from_unixtime(SparkR.DF$firstpackettime, 'yyyy-MM-dd HH')
    SparkR.DF$firstpackettimemod <- from_unixtime(SparkR.DF$firstpackettimemod)
    #SparkR.DF$lastpackettimemod <- from_unixtime(SparkR.DF$lastpackettimemod)
    #nrow(SparkR.DF)
   
    SparkR.DF$'total_bytes' <- SparkR.DF$sourcebytes  +  SparkR.DF$destinationbytes
    #str(SparkR.DF)
    #SD3_S <-(select(SparkR.DF, SparkR.DF$firstpackettimemod, SparkR.DF$lastpackettime, SparkR.DF$firstpackettime,SparkR.DF$sourceip, SparkR.DF$destinationip, SparkR.DF$`total_bytes`, SparkR.DF$protocol, SparkR.DF$destinationport)) 
    
    #distinct_machines_df <- (select(SparkR.DF,SparkR.DF$sourceip, SparkR.DF$destinationip))
    #distinct_machines_df <- distinct(distinct_machines_df)
    #nrow(distinct_machines_df)
    
    #count_df <-summarize(groupBy(SparkR.DF, SparkR.DF$sourceip), count=n(SparkR.DF$sourceip))
    #count_df_arranged<-arrange(count_df, desc(count_df$count))
    #head(count_df_arranged)
    
    #hourly_data_df <-summarize(groupBy(SparkR.DF, SparkR.DF$firstpackettimemod), total_bytes=sum(SparkR.DF$`total_bytes`))
    hourly_data_sourcedestination_df <-summarize(groupBy(SparkR.DF, SparkR.DF$firstpackettimemod), source_bytes=sum(SparkR.DF$sourcebytes) , destination_bytes=sum(SparkR.DF$destinationbytes))
    data_port_df <- subset(SparkR.DF, SparkR.DF$destinationport == "22")
    hourly_data_port_df <-summarize(groupBy(data_port_df,data_port_df$firstpackettimemod), port_total_bytes=sum(data_port_df$'total_bytes'))
    data_machine_df <- subset(SparkR.DF, SparkR.DF$sourceip == "SOURCE_IP")
    hourly_data_machine_df <-summarize(groupBy(data_machine_df,data_machine_df$firstpackettimemod), machine_total_bytes=sum(data_machine_df$'total_bytes'))
    

    #hourly_data_df <- arrange(hourly_data_df , hourly_data_df$firstpackettimemod)
    hourly_data_sourcedestination_df <- arrange(hourly_data_sourcedestination_df , hourly_data_sourcedestination_df$firstpackettimemod)
    hourly_data_port_df <- arrange(hourly_data_port_df , hourly_data_port_df$firstpackettimemod)
    hourly_data_machine_df <- arrange(hourly_data_machine_df , hourly_data_machine_df$firstpackettimemod)
    
    #hourly_data_df_2 <- collect(hourly_data_df)
    hourly_data_sourcedestination_df_2 <- collect(hourly_data_sourcedestination_df)
    hourly_data_port_df_2 <- collect(hourly_data_port_df)
    hourly_data_machine_df_2 <- collect(hourly_data_machine_df)
    #distinct_machines_df_2 <- collect(distinct_machines_df)
    
    #class(hourly_data_df_2$firstpackettimemod) = c('POSIXt','POSIXct')
    #hourly_data_df_2$FirstPacketTimeMod2 <- as.POSIXct(strptime(hourly_data_df_2$firstpackettime, format="%b %d, %Y, %H:%M:%S %t"))
    #hourly_data_df_2 <- hourly_data_df[order(hourly_data_df_2$firstpackettimemod),]
    #hourly_data_df_2 <- na.omit(hourly_data_df_2)
    #summary(hourly_data_df_2$total_bytes)
    #quantile(hourly_data_df_2$total_bytes, c(.32, .57, .95)) 
    
    #colnames(hourly_data_df)[which(names(hourly_data_df) == "total_bytes")] <- "data"
    #View(hourly_data_df)
    #logFile <- file("/dev/shm/test.log", "w")
    #totalBytesFile <- file("/dev/shm/totalBytesFile.csv", "w")
    
    cat(file.names[i], file = logFile,append = TRUE, sep ="\n" )
    
    #write.df(hourly_data_df_2, path = dataFile, source =  "csv", mode = "append")
    #write.table(hourly_data_df_2, append=TRUE, col.names=F, row.names=F,file = dataFile, sep = ",")
    write.table(hourly_data_sourcedestination_df_2, append=TRUE, col.names=F, row.names=F, file = dataFileSourceDestination, sep = ",")
    write.table(hourly_data_port_df_2, append=TRUE, col.names=F, row.names=F,  file = dataFileSpecificPort, sep = ",")
    write.table(hourly_data_machine_df_2, append=TRUE, col.names=F,row.names=F, file = dataFileSpecificMachine, sep = ",")
    #write.table(distinct_machines_df_2, append=TRUE, col.names=F, row.names=F, file = dataFileDistinctMachines, sep = ",")
    
  }
}
Sys.time()
sparkR.session.stop()
