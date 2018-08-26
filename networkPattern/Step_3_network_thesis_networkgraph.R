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
setwd("results_foldernetworkGraph") 
dataset_bytes = read.csv('networkGraphSummary.csv', header=TRUE, stringsAsFactors = FALSE)

str(dataset_bytes)
dataset_bytes$time<- as.POSIXct(strptime(dataset_bytes$timeoriginal, format="%Y-%m-%d %H"))
str(dataset_bytes)
ecount_ts <- ts(dataset_bytes[,c('ecount')])
dataset_bytes$e_clean_count = tsclean(ecount_ts)

vcount_ts <- ts(dataset_bytes[,c('vcount')])
dataset_bytes$v_clean_count = tsclean(vcount_ts)

maxdegree_ts <- ts(dataset_bytes[,c('max_degree')])
dataset_bytes$maxdegree_clean_count = tsclean(maxdegree_ts)

density_ts <- ts(dataset_bytes[,c('graph_density')])
dataset_bytes$desity_clean_count = tsclean(density_ts)

str(dataset_bytes)
ggplot(dataset_bytes, aes(time, ecount)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time")  #+ ylim(c(0, 500000))

ggplot(dataset_bytes, aes(time, vcount)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

ggplot(dataset_bytes, aes(time, max_degree)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

ggplot(dataset_bytes, aes(time, graph_density)) + geom_line() + scale_x_datetime('Time') + 
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

#########################googleVis Map - ############################################################

library(googleVis)
geo = gvisGeoChart(Exports, locationvar = "Country", colorvar="Profit", options=list(Projection = "kavrayskiy-vii"))
print(geo, tag = 'chart')
################################Seasonal ETS ######################################################


str(dataset_bytes)

dataset1 <- dataset_bytes[1:72, ]
dim(dataset1)
str(dataset1)
dataset2 <-dataset_bytes[73:96, ]
dim(dataset2)

dataset_combined <- rbind.data.frame(dataset1, dataset2)
nrow(dataset_combined)
labels_ecount <- as.numeric(dataset1$e_clean_count)
labels_vcount <- as.numeric(dataset1$v_clean_count)
labels_maxdegree <- as.numeric(dataset1$max_degree)
labels_density <- as.numeric(dataset1$desity_clean_count)

seasonality<- 24
timeseries <- ts(labels_ecount,frequency=seasonality)
#model_ecount <- auto.arima(timeseries)
model_ecount <- dshw(timeseries, 24,8)

timeseries <- ts(labels_vcount,frequency=seasonality)
#model_vcount <- ets(timeseries)
model_vcount <- dshw(timeseries, 24,8)

timeseries <- ts(labels_maxdegree,frequency=seasonality)
#model_maxdegree <- ets(timeseries)
model_maxdegree <- dshw(timeseries, 24,8)

timeseries <- ts(labels_density,frequency=seasonality)
#model_density <- ets(timeseries)
model_density <- dshw(timeseries, 24,8)
#model <- ets(timeseries, model="MAA", damped=NULL, alpha=NULL, beta=NULL, gamma=NULL, phi=NULL)
#model <-  HoltWinters(timeseries, gamma=FALSE)

# timeseries <- msts(labels, seasonal.periods=c(28,168))
# plot(stl(timeseries,"periodic"))
# model <- tbats(timeseries)
# plot(model)
# plot(forecast(model))

# timeseries<-ts(labels_ecount)
# plot(stl(timeseries, "periodic"))
# #model <- dshw(timeseries, 168,24)
# model_ecount <- dshw(timeseries, 24,8)
# plot(model_ecount)
# plot(forecast(model_ecount))

numPeriodsToForecast <- 24#72#ceiling(max(dataset2$time)) - ceiling(max(dataset1$time))
numPeriodsToForecast <- max(numPeriodsToForecast, 0)

forecasted_ecount <- forecast(model_ecount, h=numPeriodsToForecast)
forecasted_ecount <- as.numeric(forecasted_ecount$mean)
dataset_ecount<-data.frame(cbind(dataset2,forecasted_ecount))

forecasted_vcount <- forecast(model_vcount, h=numPeriodsToForecast)
forecasted_vcount <- as.numeric(forecasted_vcount$mean)
dataset_vcount<- data.frame(cbind(dataset2,forecasted_vcount))

forecasted_maxdegree <- forecast(model_maxdegree, h=numPeriodsToForecast)
forecasted_maxdegree <- as.numeric(forecasted_maxdegree$mean)
dataset_maxdegree<- data.frame(cbind(dataset2,forecasted_maxdegree))

forecasted_density <- forecast(model_density, h=numPeriodsToForecast)
forecasted_density <- as.numeric(forecasted_density$mean)
dataset_density <- data.frame(cbind(dataset2,forecasted_density))

str(dataset_combined)
train <- as.data.frame(as.numeric(model_ecount$fitted))
valid <- as.data.frame(as.numeric(dataset_ecount$forecasted_ecount))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
dataset_combined$forecasted_ecount <- temp$forecastedValues

train <- as.data.frame(as.numeric(model_vcount$fitted))
valid <- as.data.frame(as.numeric(dataset_vcount$forecasted_vcount))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp <-NULL
temp<- rbind(train,valid)
dataset_combined$forecasted_vcount <- temp$forecastedValues

train <- as.data.frame(as.numeric(model_maxdegree$fitted))
valid <- as.data.frame(as.numeric(dataset_maxdegree$forecasted_maxdegree))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<- NULL
temp<- rbind(train,valid)
dataset_combined$forecasted_maxdegree <- temp$forecastedValues

train <- as.data.frame(as.numeric(model_density$fitted))
valid <- as.data.frame(as.numeric(dataset_density$forecasted_density))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
dataset_combined$forecasted_density <- temp$forecastedValues
str(dataset_combined)

str(dataset_bytes)
plot(dataset_combined$time, dataset_combined$ecount,type="l",col="royalblue",xlab="Time",ylab="count",lwd=2, ylim=c(10000,2000000))
lines(dataset_combined$time,dataset_combined$forecasted_ecount, col="red", lty=4, lwd=2)
legend("topleft",legend = c("Original Data-Num of Edges","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("royalblue","red"), lwd=2)

lines(dataset_combined$time,dataset_combined$vcount,  col="dark green", lwd=2)
lines(dataset_combined$time,dataset_combined$forecasted_vcount, col="red", lty=4, lwd=2)
legend("topright",legend = c("Original Data-Num of Nodes","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("dark green","red"), lwd=2)

plot(dataset_combined$time, dataset_combined$max_degree,type="l",col="royalblue",xlab="Time",ylab="count",lwd=2)
lines(dataset_combined$time,dataset_combined$forecasted_maxdegree, col="red", lty=4, lwd=2)
legend("topleft",legend = c("Original Data-Max Degree","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("royalblue","red"), lwd=2)

plot(dataset_combined$time, dataset_combined$graph_density,type="l",col="royalblue",xlab="Time",ylab="count",lwd=2)
lines(dataset_combined$time,dataset_combined$forecasted_density, col="red", lty=4, lwd=2)
legend("topleft",legend = c("Original Data-Graph Density","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("royalblue","red"), lwd=2)

ggtsdisplay(residuals(model), main="Double Seasonal Holt-Winters Forecast Residuals")
hist(model$residuals, nclass="FD", main="Histogram of residuals")
ggplot(err, aes(x = Set, y = model$residuals)) + geom_boxplot()

observed_data <- as.numeric(dataset_ecount$e_clean_count)
forecast <- as.numeric(dataset_ecount$forecasted_ecount)

observed_data <- as.numeric(dataset_vcount$v_clean_count)
forecast <- as.numeric(dataset_vcount$forecasted_vcount)

observed_data <- as.numeric(dataset_maxdegree$maxdegree_clean_count)
forecast <- as.numeric(dataset_maxdegree$forecasted_maxdegree)

observed_data <- as.numeric(dataset_density$desity_clean_count)
forecast <- as.numeric(dataset_density$forecasted_density)
  
forecast_data_testwindow <- as.numeric(forecast[(which(!is.na(forecast)))])
actual_data_testwindow <- as.numeric(observed_data[(which(!is.na(forecast)))])
mase <- masefun(actual_data_testwindow,forecast_data_testwindow)
smape <- smape(actual_data_testwindow,forecast_data_testwindow)
arima_acc <- data.frame(Method=as.character("Double Seasonal Holt-Winters Forecast Results"),accuracy(forecast_data_testwindow,actual_data_testwindow),MASE=mase,sMAPE=smape)
arima_acc$Method <- as.character(arima_acc$Method)
data.set <- arima_acc

data.set
####################anomaly deletion - e-count###################

train.err <- as.data.frame(as.numeric(model_ecount$fitted - dataset1$ecount))
valid.err <- as.data.frame(as.numeric(dataset_ecount$forecasted_ecount- dataset_ecount$ecount))
names(train.err)<-c('err')
names(valid.err)<-c('err')
errtemp <- rbind(train.err,valid.err)
dataset_combined$err <- errtemp$err
str(dataset_combined)
points_df <- dataset_combined[,c("time","graph_density")]
plot(dataset_combined[,c("time","ecount")],type="l", col="light blue" ,xlab="Time",ylab="Count",lwd=2)
lines(dataset_combined[,c("time","forecasted_ecount")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data-Num of Edges","Double Seasonal Holt-Winters Forecast Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_ecount"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_ecount"] > .5){
    points(x=dataset_combined[row, "time"], y=dataset_combined[row, "ecount"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
  }
  
}

#write.csv(dataset_combined,file="totalbytes10dayshourlyquadruple_supervised.csv",row.names = FALSE)

####################anomaly deletion - v-count###################

train.err <- as.data.frame(as.numeric(model_vcount$fitted - dataset1$vcount))
valid.err <- as.data.frame(as.numeric(dataset_vcount$forecasted_vcount- dataset_vcount$vcount))
names(train.err)<-c('err')
names(valid.err)<-c('err')
errtemp <- rbind(train.err,valid.err)
dataset_combined$err <- errtemp$err
str(dataset_combined)
points_df <- dataset_combined[,c("time","graph_density")]
plot(dataset_combined[,c("time","vcount")],type="l", col="light blue" ,xlab="Time",ylab="Count",lwd=2)
lines(dataset_combined[,c("time","forecasted_vcount")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data-Num of Nodes","Double Seasonal Holt-Winters Forecast Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_vcount"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_vcount"] > .5){
    points(x=dataset_combined[row, "time"], y=dataset_combined[row, "vcount"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
  }
  
}

#write.csv(dataset_combined,file="totalbytes10dayshourlyquadruple_supervised.csv",row.names = FALSE)

####################anomaly deletion - Max degree###################

train.err <- as.data.frame(as.numeric(model_maxdegree$fitted - dataset1$max_degree))
valid.err <- as.data.frame(as.numeric(dataset_maxdegree$forecasted_maxdegree- dataset_maxdegree$max_degree))
names(train.err)<-c('err')
names(valid.err)<-c('err')
errtemp <- rbind(train.err,valid.err)
dataset_combined$err <- errtemp$err
str(dataset_combined)
points_df <- dataset_combined[,c("time","max_degree")]
plot(dataset_combined[,c("time","max_degree")],type="l", col="light blue" ,xlab="Time",ylab="Count",lwd=2)
lines(dataset_combined[,c("time","forecasted_maxdegree")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data-Max degree","Double Seasonal Holt-Winters Forecast Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_maxdegree"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_maxdegree"] > .5){
    points(x=dataset_combined[row, "time"], y=dataset_combined[row, "max_degree"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
  }
  
}

#write.csv(dataset_combined,file="totalbytes10dayshourlyquadruple_supervised.csv",row.names = FALSE)


####################anomaly deletion - Density###################

train.err <- as.data.frame(as.numeric(model_density$fitted - dataset1$graph_density))
valid.err <- as.data.frame(as.numeric(dataset_density$forecasted_density- dataset_density$graph_density))
names(train.err)<-c('err')
names(valid.err)<-c('err')
errtemp <- rbind(train.err,valid.err)
dataset_combined$err <- errtemp$err
str(dataset_combined)
points_df <- dataset_combined[,c("time","graph_density")]
plot(dataset_combined[,c("time","graph_density")],type="l", col="light blue" ,xlab="Time",ylab="Count",lwd=2)
lines(dataset_combined[,c("time","forecasted_density")], col="red", lty=4, lwd=1.5)
legend("topleft",legend = c("Original Data-Graph Density","Double Seasonal Holt-Winters Forecast Forecast", "Anomalies"),bty=c("n","n"),pch=c(16,16,16),lty=c(1,4,0),col=c("light blue1","red","coral1"), lwd=2)

#View(dataset_combined)
for (row in 1:nrow(dataset_combined)) {
  dataset_combined[row, "anomaly"] <- 0
  print( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_density"])
  if ( abs(dataset_combined[row, "err"]) /dataset_combined[row, "forecasted_density"] > .5){
    points(x=dataset_combined[row, "time"], y=dataset_combined[row, "graph_density"], pch=16, col="coral1")
    dataset_combined[row, "anomaly"] <- 1
  }
 
}

#write.csv(dataset_combined,file="totalbytes10dayshourlyquadruple_supervised.csv",row.names = FALSE)
