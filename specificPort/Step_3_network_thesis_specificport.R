#https://stats.stackexchange.com/questions/7268/how-to-aggregate-by-minute-data-for-a-week-into-hourly-means
# Lwin Moe, 

library(networkD3)
library('ggplot2')
library('forecast')
library('tseries')
library('igraph')
library('GGally')
library('network')
library(SparkR)
library(caret)
library(class)
library('e1071')
library(scales)
library(dplyr)

################################tsclean() to remove outliers##################################
setwd("results_folderspecificPort") 
dataset_bytes = read.csv('specificport10dayshourly.csv', header=TRUE, stringsAsFactors = FALSE)
#dataset_bytes = read.csv('totalbytes10dayshourly.csv', header=TRUE, stringsAsFactors = FALSE)

str(dataset_bytes)
names(dataset_bytes) <- c("index", "time", "data")
str(dataset_bytes)

dataset_bytes$time<- as.POSIXct(strptime(dataset_bytes$time, format="%Y-%m-%d %H:%M:%S"))
dataset_bytes$data <- dataset_bytes$data/(1000 * 1000 * 1000)
str(dataset_bytes)
count_ts <- ts(dataset_bytes[,c('data')])
dataset_bytes$clean_count = tsclean(count_ts)
str(dataset_bytes)

ggplot(dataset_bytes, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time")  #+ ylim(c(0, 500000))

ggplot(dataset_bytes, aes(time, clean_count)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

#####################minutes and hourly count##################################################
dataset_bytes$count_ma = ma(dataset_bytes$clean_count, order=24) # using the clean count with no outliers
ggplot() +
  geom_line(data = dataset_bytes, aes(x = index, y = clean_count, colour = "Volume (GB)")) +
  geom_line(data = dataset_bytes, aes(x = index, y = count_ma,   colour = "Moving Average"))  +
  ylab('Volume (GB)') + xlab('Time')


######################### stl()decomposing and forecasting#############################
count_ma2 = ts(na.omit(dataset_bytes$clean_count), frequency=24)
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
str(dataset_bytes)

#dataset1 <- dataset_bytes[1:39, ]
dataset1 <- dataset_bytes[49:144, ]
#dataset1 <- dataset_bytes[1:672, ]
dim(dataset1)
str(dataset1)
#dataset2 <-dataset_bytes[40:43, ]
dataset2 <-dataset_bytes[145:168, ]
#dataset2 <-dataset_bytes[673:744, ]
dim(dataset2)

dataset_combined <- rbind.data.frame(dataset1, dataset2)
nrow(dataset_combined)
#labels <- as.numeric(dataset1$data)
labels <- as.numeric(dataset1$clean_count)

seasonality<- 24#168
timeseries <- ts(labels,frequency=seasonality)
plot(stl(timeseries, "periodic"))
model <- auto.arima(timeseries)
#model <- ets(timeseries, model="MAA", damped=NULL, alpha=NULL, beta=NULL, gamma=NULL, phi=NULL)
#model <-  HoltWinters(timeseries, gamma=FALSE)


timeseries<-ts(labels)
plot(stl(timeseries, "periodic"))
model <- dshw(timeseries, 12,4)
plot(model)
plot(forecast(model))

numPeriodsToForecast <- 24#72#ceiling(max(dataset2$time)) - ceiling(max(dataset1$time))
numPeriodsToForecast <- max(numPeriodsToForecast, 0)
forecastedData <- forecast(model, h=numPeriodsToForecast)
forecastedData <- as.numeric(forecastedData$mean)
output <- data.frame(cbind(dataset2,forecastedData))
#output <- data.frame(time=dataset2$time,forecast=forecastedData)
dataset4 <- NULL
dataset4 <- output

str(dataset_combined)
train <- as.data.frame(as.numeric(model$fitted))
valid <- as.data.frame(as.numeric(dataset4$forecastedData))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<- rbind(train,valid)
dataset_combined$forecastedData <- temp$forecastedValues

#time <- as.numeric(cbind(dataset1$time,dataset3$time))
time <- as.numeric(dataset_combined$time)
time2<- as.numeric(dataset4$time)
#observed_data <- as.numeric(dataset_combined$data)
observed_data <- as.numeric(dataset4$clean_count)
forecast <- as.numeric(dataset4$forecastedData)
plot(dataset4$time,observed_data,type="l",col="royalblue" ,xlab="Time",ylab="Volume (GB)",lwd=2, ylim=c(2,15))
#plot(dataset_combined$time,dataset_combined$clean_count,type="l",col="royalblue",xlab="Time",ylab="Volume (GB)",lwd=2)
lines(dataset4$time,forecast, col="red", lty=4, lwd=2)
#lines(dataset_combined$time,dataset_combined$forecastedData, col="red", lty=4, lwd=2)
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
points_df <- dataset_combined[,c("time","data")]
plot(dataset_combined[,c("time","data")],type="l", col="light blue" ,xlab="Time",ylab="Volume (GB)",lwd=2)
lines(dataset_combined[,c("time","forecastedData")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data","Double Seasonal Holt-Winters Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecastedData"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecastedData"] > 1.2){
    points(x=dataset_combined[row, "time"], y=dataset_combined[row, "data"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
    print(row)
  }
 
}

write.csv(dataset_combined,file="specificport10dayshourly_supervised.csv",row.names = FALSE)
