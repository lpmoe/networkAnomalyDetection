#https://stats.stackexchange.com/questions/7268/how-to-aggregate-by-minute-data-for-a-week-into-hourly-means
# Lwin Moe, 

install.packages("forecast")
install.packages("tseries")
install.packages("igraph")
install.packages("GGally")
install.packages("network")
install.packages("sna")
install.packages("networkD3") 
install.packages("SparkR")
install.packages("tidyverse")
install.packages("anomalyDetection")

library(networkD3)
library('ggplot2')
library('forecast')
library('tseries')
library('igraph')
library('GGally')
library('network')
#library(sna)
library(SparkR)
library(caret)
library(class)
library('e1071')
library(scales)
library(dplyr)
#library(tidyverse)

################################tsclean() to remove outliers##################################
setwd("results_foldertotalBytes") 
dataset_meanbytes = read.csv('totalbytes10dayshourlyquadruple.csv', header=TRUE, stringsAsFactors = FALSE)
#dataset_meanbytes = read.csv('totalbytes10dayshourly.csv', header=TRUE, stringsAsFactors = FALSE)

str(dataset_meanbytes)
names(dataset_meanbytes) <- c("time", "index", "data")
str(dataset_meanbytes)

dataset_meanbytes$index<- as.POSIXct(strptime(dataset_meanbytes$index, format="%m/%d/%Y %H:%M"))
dataset_meanbytes$data <- dataset_meanbytes$data/(1000 * 1000 * 1000)
str(dataset_meanbytes)
count_ts <- ts(dataset_meanbytes[,c('data')])
dataset_meanbytes$clean_count = tsclean(count_ts)

ggplot(dataset_meanbytes, aes(index, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time")  #+ ylim(c(0, 500000))

ggplot(dataset_meanbytes, aes(index, clean_count)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

#####################minutes and hourly count##################################################
dataset_meanbytes$count_ma = ma(dataset_meanbytes$clean_count, order=24) # using the clean count with no outliers
ggplot() +
  geom_line(data = dataset_meanbytes, aes(x = index, y = clean_count, colour = "Volume (GB)")) +
  geom_line(data = dataset_meanbytes, aes(x = index, y = count_ma,   colour = "Moving Average"))  +
  ylab('Volume (GB)') + xlab('Time')


######################### stl()decomposing and forecasting#############################
count_ma2 = ts(na.omit(dataset_meanbytes$clean_count), frequency=24)
decomp = stl(count_ma2, s.window="periodic")
deseasonal_count <- seasadj(decomp)
plot(decomp)

#################MS code Common Function##################

masefun <- function(observed, predicted){
  error = 0;
  if (length(observed) != length(predicted)) {
    return (NA);
  } else if (length(observed) == 0 || length(predicted) == 0) {
    return (NA);
  }
  else {
    denom = (sum(abs(observed[2:length(observed)] - observed[1:(length(observed) - 1)])))/(length(observed) - 1)
    error = sum((abs(observed-predicted)) / denom)/length(observed);
  }
  return (error);
}

smape <- function(observed, predicted){
  error = 0;
  if (length(observed) != length(predicted)) {
    return (NA);
  } else if (length(observed) == 0 || length(predicted) == 0) {
    return (NA);
  }
  else {
    error = sum((abs(observed-predicted)) / (observed+predicted))/length(observed);
    # denom = (sum(abs(observed[2:length(observed)] - observed[1:(length(observed) - 1)])))/(length(observed) - 1)
    #  error = sum((abs(observed-predicted)) / denom)/length(observed);
  }
  return (100.0*error);
}

#########################Seasonal ETS - ############################################################
str(dataset_meanbytes)

#dataset1 <- dataset_meanbytes[1:39, ]
#dataset1 <- dataset_meanbytes[49:144, ]
dataset1 <- dataset_meanbytes[1:672, ]
dim(dataset1)
str(dataset1)
#dataset2 <-dataset_meanbytes[40:43, ]
#dataset2 <-dataset_meanbytes[145:168, ]
dataset2 <-dataset_meanbytes[673:744, ]
dim(dataset2)
head(dataset2)

dataset_combined <- rbind.data.frame(dataset1, dataset2)
nrow(dataset_combined)
#labels <- as.numeric(dataset1$data)
labels <- as.numeric(dataset1$clean_count)

seasonality<- 24#168
timeseries <- ts(labels,frequency=seasonality)
plot(stl(timeseries, "periodic"))
model <- ets(timeseries)
#model <- ets(timeseries, model="MAA", damped=NULL, alpha=NULL, beta=NULL, gamma=NULL, phi=NULL)
#model <-  HoltWinters(timeseries, gamma=FALSE)

timeseries <- msts(labels, seasonal.periods=c(28,168))
plot(stl(timeseries,"periodic"))
model <- tbats(timeseries)
plot(model)
plot(forecast(model))


timeseries<-ts(labels)
plot(stl(timeseries, "periodic"))
model <- dshw(timeseries, 168,24)
plot(model)
plot(forecast(model))

numPeriodsToForecast <- 72#72#ceiling(max(dataset2$time)) - ceiling(max(dataset1$time))
numPeriodsToForecast <- max(numPeriodsToForecast, 0)
forecastedData <- forecast(model, h=numPeriodsToForecast)
forecastedData <- as.numeric(forecastedData$mean)

output <- data.frame(cbind(dataset2,forecastedData))
#output <- data.frame(time=dataset2$time,forecast=forecastedData)
dataset4 <- NULL
dataset4 <- output

str(dataset4)
str(dataset_combined)
train <- as.data.frame(as.numeric(model$fitted))
valid <- as.data.frame(as.numeric(dataset4$forecastedData))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<- rbind(train,valid)
dataset_combined$forecastedData <- temp$forecastedValues

#time <- as.numeric(cbind(dataset1$time,dataset3$time))
time <- as.numeric(dataset_combined$index)
time2<- as.numeric(dataset4$index)
#observed_data <- as.numeric(dataset_combined$data)
observed_data <- as.numeric(dataset4$data)
forecast <- as.numeric(dataset4$forecastedData)
plot(dataset4$index,observed_data,type="l",col="royalblue" ,xlab="Time",ylab="Volume (GB)",lwd=2)
#plot(dataset_combined$index,observed_data,type="l",col="royalblue",xlab="Time",ylab="Volume (GB)",lwd=2)
lines(time2,forecast, col="red", lty=4, lwd=2)
#lines(dataset_combined$index,dataset_combined$forecastedData, col="red", lty=4, lwd=2)
legend("topleft",legend = c("Original Data","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("royalblue","red"), lwd=2)
ggtsdisplay(residuals(model), main="Double Seasonal Holt-Winters Residuals")
hist(model$residuals, nclass="FD", main="Histogram of residuals")
ggplot(err, aes(x = Set, y = model$residuals)) + geom_boxplot()
# produce linear trend model
#network.lm <- tslm(timeseries~ trend)
#plot(timeseries, xlab = "Time", ylab = "Volume (Bytes)",  bty = "l")
#lines(network.lm$fitted, lwd = 2)

forecast_data_testwindow <- as.numeric(forecast[(which(!is.na(forecast)))])
actual_data_testwindow <- as.numeric(observed_data[(which(!is.na(forecast)))])
mase <- masefun(actual_data_testwindow,forecast_data_testwindow)
smape <- smape(actual_data_testwindow,forecast_data_testwindow)
arima_acc <- data.frame(Method=as.character("Double Seasonal Holt-Winters Results"),accuracy(forecast_data_testwindow,actual_data_testwindow),MASE=mase,sMAPE=smape)
arima_acc$Method <- as.character(arima_acc$Method)
data.set <- arima_acc

data.set

####################anomaly deletion - keep###################

train.err <- as.data.frame(as.numeric(model$fitted - dataset1$data))
valid.err <- as.data.frame(as.numeric(dataset4$forecastedData - dataset4$data))
names(train.err)<-c('err')
names(valid.err)<-c('err')
errtemp <- rbind(train.err,valid.err)
dataset_combined$err <- errtemp$err
str(dataset_combined)
points_df <- dataset_combined[,c("index","data")]
plot(dataset_combined[,c("index","data")],type="l", col="light blue" ,xlab="Time",ylab="Volume (GB)",lwd=2)
lines(dataset_combined[,c("index","forecastedData")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data","Double Seasonal Holt-Winters Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecastedData"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecastedData"] > .5){
    points(x=dataset_combined[row, "index"], y=dataset_combined[row, "data"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
  }
 
}

write.csv(dataset_combined,file="totalbytes10dayshourlyquadruple_supervised.csv",row.names = FALSE)
####################anomaly deletion - delete###################
train.err2 <- as.numeric(model$fitted - dataset1$data)
valid.err2 <- as.numeric(dataset4$forecastedData - dataset4$data)

err <- data.frame(Error = c( valid.err2), 
                   Set = c(rep("Validation", length(valid.err2))))
# err <- data.frame(Error = c(train.err2, valid.err2), 
#                   Set = c(rep("Training", length(train.err2)),
#                           rep("Validation", length(valid.err2))))
ggplot(err, aes(x = Set, y = Error)) + geom_boxplot()
plot(dataset4$index, valid.err2, pch=16,col="red", xlab = "Time", ylab = "Error - Volume (GB)")




#################################Seasonal Arima########################################
# partition
names(dataset_meanbytes) <- c("index", "time", "data")
str(dataset_meanbytes)
dataset_meanbytes$time<- as.POSIXct(strptime(dataset_meanbytes$time, format="%Y-%m-%d %H:%M:%S"))
str(dataset_meanbytes)

dataset1 <- dataset_meanbytes[1:168, ]
dim(dataset1)
str(dataset1)
dataset2 <-dataset_meanbytes[169:240, ]
dim(dataset2)
head(dataset2)

seasonality<- 1
labels <- as.numeric(dataset1$data)
timeseries <- ts(labels,frequency=seasonality)
model <- auto.arima(timeseries)
numPeriodsToForecast <-72# ceiling(max(dataset2$time)) - ceiling(max(dataset1$time))
numPeriodsToForecast
numPeriodsToForecast <- max(numPeriodsToForecast, 0)
forecastedData <- forecast(model, h=numPeriodsToForecast)
forecastedData <- as.numeric(forecastedData$mean)

output <- data.frame(cbind(dataset2,forecastedData))
#output <- data.frame(time=dataset2$time,forecast=forecastedData)
dataset3 <- output
str(dataset3)

#time <- as.numeric(cbind(dataset1$time,dataset3$time))
time <- as.numeric(dataset_meanbytes$time)
time2<- as.numeric(dataset3$time)
#observed_data <- as.numeric(cbind(dataset1$data,dataset3$data))
observed_data <- as.numeric(dataset_meanbytes$data)
forecast <- as.numeric(dataset3$forecastedData)
plot(time,observed_data,type="l",col="blue",xlab="Time",ylab="Data",lwd=1.5)
lines(time2,forecast,col="red",pch=22, lwd=1.5)

legend("topleft",legend = c("Original Data","Seasonal ARIMA Forecast"),bty=c("n","n"),lty=c(1,1),pch=16,col=c("blue","red"))

#network.lm <- tslm(timeseries ~ trend + I(trend^2))
#lines(network.lm$fitted, lwd = 2)

# produce linear trend model
network.lm <- tslm(timeseries~ trend)
# plot the series
plot(timeseries, xlab = "Time", ylab = "Volume (Bytes)",  bty = "l")
lines(network.lm$fitted, lwd = 2)
summary(network.lm)

forecast_data_testwindow <- as.numeric(forecast[(which(!is.na(forecast)))])
actual_data_testwindow <- as.numeric(observed_data[(which(!is.na(forecast)))])
mase <- masefun(actual_data_testwindow,forecast_data_testwindow)
smape <- smape(actual_data_testwindow,forecast_data_testwindow)
arima_acc <- data.frame(Method=as.character("seasonal arima"),accuracy(forecast_data_testwindow,actual_data_testwindow),MASE=mase,sMAPE=smape)
arima_acc$Method <- as.character(arima_acc$Method)
data.set <- arima_acc

lapply(data.set,class)
data.set
#########################Arima and Seasonal ETS############################################

output_forecast = (dataset3$forecastedData + dataset4$forecastedData)/2;
output_forecast
output_frame = data.frame(cbind(dataset3[,1],output_forecast))
dim(output_frame)
str(output_frame)
colnames(output_frame) <- c("time","forecast");

# Sample operation
data.set = output_frame;

str(data.set)
dim(data.set)
dim(dataset2)
time2 <- as.numeric(dataset_meanbytes$time)
time <- as.numeric(data.set$time)
observed_data <- as.numeric(dataset2$data)
forecast <- as.numeric(data.set$forecast)
plot(time,observed_data,type="l",col="blue",xlab="Time",ylab="Data",lwd=1.5)
lines(time,forecast,col="red",lwd=1.5)
legend("topleft",legend = c("Original Data","Average Seasonal ETS and ARIMA Forecast"),bty=c("n","n"),lty=c(1,1),pch=16,col=c("blue","red"))

forecast_data_testwindow <- as.numeric(forecast[(which(!is.na(forecast)))])
actual_data_testwindow <- as.numeric(observed_data[(which(!is.na(forecast)))])
mase <- masefun(actual_data_testwindow,forecast_data_testwindow)
smape <- smape(actual_data_testwindow,forecast_data_testwindow)
arima_acc <- data.frame(Method=as.character("average seasonal arima & ets"),accuracy(forecast_data_testwindow,actual_data_testwindow),MASE=mase,sMAPE=smape)
arima_acc$Method <- as.character(arima_acc$Method)
data.set <- arima_acc

lapply(data.set,class)
data.set
######################### statistical ADF test for stationary#############################
adf.test(count_ma, alternative = "stationary")


#########################auto correlation and choosing model order#############################
Acf(count_ma, main='')

Pacf(count_ma, main='')


#########################The augmented Dickey-Fuller test ######################################
count_d1 = diff(deseasonal_cnt, differences = 1)
plot(count_d1)
adf.test(count_d1, alternative = "stationary")


#############################ACF for Differenced Series###############################
Acf(count_d1, main='ACF for Differenced Series')
Pacf(count_d1, main='PACF for Differenced Series')


##########################network graph#################################################

#Eliminate dups
network_data<-network_dataoriginal
dim(network_data)
df_dups <- network_data[c(4,6)]
dim(network_data)
test<-network_data[!duplicated(df_dups),]
test<- test[,c(4,6)]
str(test)

test2<-test[1:200,]
graph.edges <- as.matrix((test2[,c(1,2)]))
g <- graph.edgelist(graph.edges, directed = TRUE)
plot(g)
degree(graph.edges)
eigen_centrality(g)

diameter(g, directed=F,weights=NA)
diam <- get_diameter(g, directed=T)
diam
edge_density(g, loops=F)
#http://kateto.net/networks-r-igraph

class(g)
deg <- degree(graph.edges)
#plot(net, vertex.size=deg*3)
hist(deg, breaks=1:vcount(g)-1, main="Histogram of node degree")

graph_from_data_frame(d, directed = TRUE, vertices = NULL)
simpleNetwork(test2)


#graph.edges <- as.matrix(network_data[,c(4,6)])
#g <- graph.edgelist(graph.edges, directed = FALSE)
#g<-simplify(g, remove.multiple = T, remove.loops = T)
#plot(g, vertex.label = NA)

