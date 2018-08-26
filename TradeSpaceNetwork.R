matx <- NULL
algorithms <- c("EN-ARIMA-ETS","ARIMA","ETS","Seasonal ARIMA","Seasonal ETS","TBATS","DS-Holt-Winters","Anomaly Detection","LSTM","Network")
parallelProcessing <- c("Hadoop","Spark")
dataStorage <- c("Amazon S3","OpenStack Swift")
cloudPlatform <- c("OpenStack","Amazon AWS","Databricks")
IDENoteBook <- c("RStudio","Zeppelin","SparkNoteBook","DatabricksNoteBook")
clusterArrangement <- c("2Masters-8Workers", "2Masters-4Workers", "1Master-4Workers", "1Master-2Workers")
progLanguage <- c("R","Python","Scala","Java")


matx <- expand.grid(algorithms, parallelProcessing, dataStorage, cloudPlatform, IDENoteBook, clusterArrangement, progLanguage)
#atx <- expand.grid(parallelProcessing, dataStorage)
colnames(matx) <- c("algorithms","parallelProcessing","dataStorage","cloudPlatform","IDENoteBook","clusterArrangement","progLanguage")
#colnames(matx) <- c("parallelProcessing","dataStorage")
matx

#algorithms <- c("EN-ARIMA-ETS","ARIMA","ETS","Seasonal ARIMA","Seasonal ETS","TBATS","DS-Holt-Winters","Classification","LSTM","Network")
calc.algorithms.wt <- function(algorithms) {
  #print(algorithms)
  algorithms_wt <- 0.5
  if(algorithms == "EN-ARIMA-ETS") {
    algorithms_wt <- 0.5
  }
  if(algorithms == "ARIMA") {
    algorithms_wt <- 0.1
  }
  if(algorithms == "ETS") {
    algorithms_wt <- 0.2
  }
  if(algorithms == "Seasonal ARIMA") {
    algorithms_wt <- 0.2
  }
  if(algorithms == "Seasonal ETS") {
    algorithms_wt <- 0.7
  }
  if(algorithms == "TBATS") {
    algorithms_wt <- 0.6
  }
  if(algorithms == "DS-Holt-Winters") {
    algorithms_wt <- 0.9
  }

  if(algorithms == "Anomaly Detection") {
    algorithms_wt <- 0.6
  }
  if(algorithms == "LSTM") {
    algorithms_wt <- 0.6
  }
  if(algorithms == "Network") {
    algorithms_wt <- 0.7
  }
    algorithms_wt
}

calc.algorithms.cost <- function(algorithms) {
  #print(algorithms)
  
  algorithms_cost <- 50
  if(algorithms == "EN-ARIMA-ETS") {
    algorithms_cost <- 50
  }
  if(algorithms == "ARIMA") {
    algorithms_cost <- 10
  }
  if(algorithms == "ETS") {
    algorithms_cost <- 20
  }
  if(algorithms == "Seasonal ARIMA") {
    algorithms_cost <- 20
  }
  if(algorithms == "Seasonal ETS") {
    algorithms_cost <- 20
  }
  if(algorithms == "TBATS") {
    algorithms_cost <- 50
  }
  if(algorithms == "DS-Holt-Winters") {
    algorithms_cost <- 50
  }
  
  if(algorithms == "Anomaly Detection") {
    algorithms_cost <- 70
  }
  if(algorithms == "LSTM") {
    algorithms_cost <- 90
  }
  if(algorithms == "Network") {
    algorithms_cost <- 70
  }
  algorithms_cost
}

matx$algorithmswt <- mapply(calc.algorithms.wt,matx$algorithms)
matx$algorithmscost <- mapply(calc.algorithms.cost,matx$algorithms)
#parallelProcessing <- c("Hadoop","Spark")
#parallelProcessing_wt   <- c(0.5, 0.7)
#parallelProcessing_cost <- c(0.5, 0.6)

calc.parallelProcessing.wt <- function(parallelProcessing) {
  #printparallelProcessing)
  
  if(parallelProcessing == "Hadoop") {
    parallelProcessing_wt <- 0.5
  }
  if(parallelProcessing == "Spark") {
    parallelProcessing_wt <- 0.7
    
  }
    parallelProcessing_wt
    # return value 
}
calc.parallelProcessing.cost <- function(parallelProcessing) {
  #printparallelProcessing)
  
  if(parallelProcessing == "Hadoop") {
    parallelProcessing_cost <- 50
  }
  if(parallelProcessing == "Spark") {
    parallelProcessing_cost <- 60
    
  }
  parallelProcessing_cost
  # return value 
}

matx$parallelProcessingwt <- mapply(calc.parallelProcessing.wt,matx$parallelProcessing)
matx$parallelProcessingcost <- mapply(calc.parallelProcessing.cost,matx$parallelProcessing)

#dataStorage <- c("Amazon S3","OpenStack Swift")
#dataStorage_wt <- c(0.7, 0.4)
#dataStorage_cost    <- c(0.7, 0.3)

calc.dataStorage.wt <- function(dataStorage) {
  #printdataStorage)
  
  if(dataStorage == "Amazon S3") {
    dataStorage_wt <- 0.7
  }
  if(dataStorage == "OpenStack Swift") {
    dataStorage_wt <- 0.4
    
  }
  dataStorage_wt
}
calc.dataStorage.cost <- function(dataStorage) {
  #printdataStorage)
  
  if(dataStorage == "Amazon S3") {
    dataStorage_cost <- 70
  }
  if(dataStorage == "OpenStack Swift") {
    dataStorage_cost <- 30
    
  }
  dataStorage_cost
}

matx$dataStoragewt <- mapply(calc.dataStorage.wt,matx$dataStorage)
matx$dataStoragecost <- mapply(calc.dataStorage.cost,matx$dataStorage)

#cloudPlatform <- c("OpenStack","AWS","Databricks")
#cloudPlatform_utility <- c(0.4, 0.6, 0.7)
#cloudPlatform_cost    <- c(0.3, 0.7, 0.6)

calc.cloudPlatform.wt <- function(cloudPlatform) {
  #printcloudPlatform)
  
  if(cloudPlatform == "OpenStack") {
    cloudPlatform_wt <- 0.4
  }
  if(cloudPlatform == "Amazon AWS") {
    cloudPlatform_wt <- 0.6
  }
  if(cloudPlatform == "Databricks") {
    cloudPlatform_wt <- 0.7
  }
  cloudPlatform_wt
}
calc.cloudPlatform.cost <- function(cloudPlatform) {
  #printcloudPlatform)
  
  if(cloudPlatform == "OpenStack") {
    cloudPlatform_cost <- 30
  }
  if(cloudPlatform == "Amazon AWS") {
    cloudPlatform_cost <- 70
  }
  if(cloudPlatform == "Databricks") {
    cloudPlatform_cost <- 60
  }
  cloudPlatform_cost
}

matx$cloudPlatformwt <- mapply(calc.cloudPlatform.wt,matx$cloudPlatform)
matx$cloudPlatformcost <- mapply(calc.cloudPlatform.cost,matx$cloudPlatform)


#IDENoteBook <- c("RStudio","Zeppelin","SparkNoteBook","DatabricksNoteBook")
#IDENoteBook_utility <- c(0.7, 0.5, 0.3, 0.7)
#IDENoteBook_cost    <- c(0.6, 0.5, 0.5, 0.7)

calc.IDENoteBook.wt <- function(IDENoteBook) {
  #printIDENoteBook)
  
  if(IDENoteBook == "RStudio") {
    IDENoteBook_wt <- 0.7
  }
  if(IDENoteBook == "Zeppelin") {
    IDENoteBook_wt <- 0.5
  }
  if(IDENoteBook == "SparkNoteBook") {
    IDENoteBook_wt <- 0.3
  }
  if(IDENoteBook == "DatabricksNoteBook") {
    IDENoteBook_wt <- 0.7
  }
  IDENoteBook_wt
}
calc.IDENoteBook.cost <- function(IDENoteBook) {
  #printIDENoteBook)
  
  if(IDENoteBook == "RStudio") {
    IDENoteBook_cost <- 50
  }
  if(IDENoteBook == "Zeppelin") {
    IDENoteBook_cost <- 50
  }
  if(IDENoteBook == "SparkNoteBook") {
    IDENoteBook_cost <- 50
  }
  if(IDENoteBook == "DatabricksNoteBook") {
    IDENoteBook_cost <- 70
  }
  IDENoteBook_cost
}

matx$IDENoteBookwt <- mapply(calc.IDENoteBook.wt,matx$IDENoteBook)
matx$IDENoteBookcost <- mapply(calc.IDENoteBook.cost,matx$IDENoteBook)
  
#clusterArrangement <- c("2Masters-6Slaves", "2Masters-4Slaves", "1Master-2Slaves", "1Master-4Slaves")
#clusterArrangement_utility <- c(0.7, 0.5, 0.5, 0.8)
#clusterArrangement_cost    <- c(0.8, 0.6, 0.4, 0.3)

calc.clusterArrangement.wt <- function(clusterArrangement) {
  #printclusterArrangement)
  
  if(clusterArrangement == "2Masters-8Workers") {
    clusterArrangement_wt <- 0.7
  }
  if(clusterArrangement == "2Masters-4Workers") {
    clusterArrangement_wt <- 0.5
  }
  if(clusterArrangement == "1Master-4Workers") {
    clusterArrangement_wt <- 0.8
  }
  if(clusterArrangement == "1Master-2Workers") {
    clusterArrangement_wt <- 0.5
  }

  clusterArrangement_wt
}
calc.clusterArrangement.cost <- function(clusterArrangement) {
  #printclusterArrangement)
  
  if(clusterArrangement == "2Masters-8Workers") {
    clusterArrangement_cost <- 80
  }
  if(clusterArrangement == "2Masters-4Workers") {
    clusterArrangement_cost <- 60
  }
  if(clusterArrangement == "1Master-4Workers") {
    clusterArrangement_cost <- 40
  }
  if(clusterArrangement == "1Master-2Workers") {
    clusterArrangement_cost <- 30
  }

  clusterArrangement_cost
}

matx$clusterArrangementwt <- mapply(calc.clusterArrangement.wt,matx$clusterArrangement)
matx$clusterArrangementcost <- mapply(calc.clusterArrangement.cost,matx$clusterArrangement)


#progLanguage <- c("R","Python","Scala","Java")
#progLanguage_utility <- c(0.7, 0.8, 0.5,0.6)
#progLanguage_cost    <- c(0.5, 0.7, 0.8,0.4)

calc.progLanguage.wt <- function(progLanguage) {
  #printprogLanguage)
  
  if(progLanguage == "R") {
    progLanguage_wt <- 0.7
  }
  if(progLanguage == "Python") {
    progLanguage_wt <- 0.8
  }
  if(progLanguage == "Scala") {
    progLanguage_wt <- 0.5
  }
  if(progLanguage == "Java") {
    progLanguage_wt <- 0.6
  }
  progLanguage_wt
}

calc.progLanguage.cost <- function(progLanguage) {
  #printprogLanguage)
  if(progLanguage == "R") {
    progLanguage_cost <- 50
  }
  if(progLanguage == "Python") {
    progLanguage_cost <- 70
  }
  if(progLanguage == "Scala") {
    progLanguage_cost <- 80
  }
  if(progLanguage == "Java") {
    progLanguage_cost <- 40
  }
  progLanguage_cost
}

matx$progLanguagewt <- mapply(calc.progLanguage.wt,matx$progLanguage)
matx$progLanguagecost <- mapply(calc.progLanguage.cost,matx$progLanguage)


library(ggplot2)

par(mar=c(1,1,1,1))
matx$wt_sum   <- matx$algorithmswt + matx$parallelProcessingwt + matx$dataStoragewt + matx$cloudPlatformwt + matx$IDENoteBookwt + matx$clusterArrangementwt + matx$progLanguagewt
matx$cost_sum <- matx$algorithmscost+  matx$parallelProcessingcost + matx$dataStoragecost + matx$cloudPlatformcost + matx$IDENoteBookcost + matx$clusterArrangementcost + matx$progLanguagecost


ggplot(matx, aes(x=cost_sum, y=wt_sum)) + geom_point(aes(color=matx$algorithms),size=3) + geom_point(aes(shape=matx$clusterArrangement), size=1) + xlab("cost") + ylab("weight") + ggtitle("Architecture-Design Trade Space Diagram")

ggplot(matx, aes(x=wt_sum, y=cost_sum)) + geom_point()

