#https://stats.stackexchange.com/questions/7268/how-to-aggregate-by-minute-data-for-a-week-into-hourly-means
# Lwin Moe, 
install.packages("googleVis")
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
setwd("results_foldergeoSourceDest/moddata") 
dataset_bytes = read.csv('geoDataFinal.csv', header=TRUE, stringsAsFactors = FALSE)
str(dataset_bytes)
#View(dataset_bytes)

dataset_bytes  <- aggregate(totalbytes ~ timeoriginal + geolocation , data=dataset_bytes, FUN=sum)
str(dataset_bytes)
#names(dataset_bytes) <- c("time", "index", "data")

# geo_distinct <- dataset_bytes[c(2)]
# geo_distinct <- distinct(geo_distinct)
# View(geo_distinct)
# res2 <- aggregate(totalbytes ~ geolocation , data=dataset_bytes, FUN=sum)
# View(res2)

dataset_bytes$time <- as.POSIXct(strptime(dataset_bytes$timeoriginal, format="%Y-%m-%d %H"))
dataset_bytes$data <- dataset_bytes$totalbytes/(1000 * 1000)
str(dataset_bytes)

geoAsiaPacificAfrica_df<- dataset_bytes[with(dataset_bytes, geolocation == "Asia Pacific & Africa"),]
geoEurope_df <- dataset_bytes[with(dataset_bytes, geolocation == "Europe"),]
geoNorthAmerica_df <- dataset_bytes[with(dataset_bytes, geolocation == "North America"),]
geoSouthAmerica_df <- dataset_bytes[with(dataset_bytes, geolocation == "South America"),]
geoExNorthAmerica_df <- dataset_bytes[with(dataset_bytes, geolocation == "External - North America"),]
geoNotExNorthAmerica_df <- dataset_bytes[with(dataset_bytes, geolocation == "External - not North America"),]
geoGlobalTransport_df <- dataset_bytes[with(dataset_bytes, geolocation == "Global Transport"),]
str(geoAsiaPacificAfrica_df)
#View(geoAsiaPacificAfrica_df)
str(geoEurope_df)
str(geoNorthAmerica_df)
str(geoSouthAmerica_df)
str(geoExNorthAmerica_df)
str(geoNotExNorthAmerica_df)
str(geoGlobalTransport_df)

#count_ts <- ts(dataset_bytes[,c('data')])
#dataset_bytes$clean_count = tsclean(count_ts)

ggplot(geoAsiaPacificAfrica_df, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (MB)") + xlab("Time")  #+ ylim(c(0, 500000))

ggplot(geoEurope_df, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

ggplot(geoNorthAmerica_df, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

ggplot(geoSouthAmerica_df, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))

ggplot(geoGlobalTransport_df, aes(time, data)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (GB)") + xlab("Time") #+ ylim(c(0, 500000))


#####################googleVis -Map##################################################

library(googleVis)
??googleVis
geo = gvisGeoChart(dataset_bytes , locationvar = "geolocation", colorvar="Profit", options=list(Projection = "kavrayskiy-vii"))
print(geo, tag = 'chart')

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

str(geoAsiaPacificAfrica_df)
geoAsiaPacificAfrica_dataset1 <- geoAsiaPacificAfrica_df[49:120,]
dim(geoAsiaPacificAfrica_dataset1)
geoAsiaPacificAfrica_dataset2 <- geoAsiaPacificAfrica_df[121:144,]
dim(geoAsiaPacificAfrica_dataset2)
geoAsiaPacificAfrica_combined <- rbind.data.frame(geoAsiaPacificAfrica_dataset1, geoAsiaPacificAfrica_dataset2)
nrow(geoAsiaPacificAfrica_combined)

str(geoEurope_df)
geoEurope_dataset1 <- geoEurope_df[49:120,]
dim(geoEurope_dataset1)
geoEurope_dataset2 <- geoEurope_df[121:144,]
dim(geoEurope_dataset2)
geoEurope_combined <- rbind.data.frame(geoEurope_dataset1, geoEurope_dataset2)
nrow(geoEurope_combined)

str(geoNorthAmerica_df)
geoNorthAmerica_dataset1 <- geoNorthAmerica_df[49:120,]
dim(geoNorthAmerica_dataset1)
geoNorthAmerica_dataset2 <- geoNorthAmerica_df[121:144,]
dim(geoNorthAmerica_dataset2)
geoNorthAmerica_combined <- rbind.data.frame(geoNorthAmerica_dataset1, geoNorthAmerica_dataset2)
nrow(geoNorthAmerica_combined)

str(geoSouthAmerica_df)
geoSouthAmerica_dataset1 <- geoSouthAmerica_df[49:120,]
dim(geoSouthAmerica_dataset1)
geoSouthAmerica_dataset2 <- geoSouthAmerica_df[121:144,]
dim(geoSouthAmerica_dataset2)
geoSouthAmerica_combined <- rbind.data.frame(geoSouthAmerica_dataset1, geoSouthAmerica_dataset2)
nrow(geoSouthAmerica_combined)

str(geoGlobalTransport_df)
geoGlobalTransport_dataset1 <- geoGlobalTransport_df[49:120,]
dim(geoGlobalTransport_dataset1)
geoGlobalTransport_dataset2 <- geoGlobalTransport_df[121:144,]
dim(geoGlobalTransport_dataset2)
geoGlobalTransport_combined <- rbind.data.frame(geoGlobalTransport_dataset1, geoGlobalTransport_dataset2)
nrow(geoGlobalTransport_combined)

labels_geoAsiaPacificAfrica <- as.numeric(geoAsiaPacificAfrica_dataset1$data)
labels_geoEurope <- as.numeric(geoEurope_dataset1$data)
labels_geoNorthAmerica <- as.numeric(geoNorthAmerica_dataset1$data)
labels_geoSouthAmerica <- as.numeric(geoSouthAmerica_dataset1$data)
labels_geoGlobalTransport <- as.numeric(geoGlobalTransport_dataset1$data)

seasonality<- 24
timeseries <- ts(labels_geoAsiaPacificAfrica,frequency=seasonality)
model_geoAsiaPacificAfrica <- dshw(timeseries, 24,8)

seasonality<- 24
timeseries <- ts(labels_geoEurope,frequency=seasonality)
model_geoEurope <- dshw(timeseries, 24,8)

seasonality<- 24
timeseries <- ts(labels_geoNorthAmerica,frequency=seasonality)
model_geoNorthAmerica <- dshw(timeseries, 24,8)

seasonality<- 24
timeseries <- ts(labels_geoSouthAmerica,frequency=seasonality)
model_geoSouthAmerica <- dshw(timeseries, 24,8)

seasonality<- 24
timeseries <- ts(labels_geoGlobalTransport,frequency=seasonality)
model_geoGlobalTransport <- dshw(timeseries, 24,8)

numPeriodsToForecast <- 24#72#ceiling(max(dataset2$time)) - ceiling(max(dataset1$time))
numPeriodsToForecast <- max(numPeriodsToForecast, 0)

forecasted_geoAsiaPacificAfrica <- forecast(model_geoAsiaPacificAfrica, h=numPeriodsToForecast)
forecasted_geoAsiaPacificAfrica<- as.numeric(forecasted_geoAsiaPacificAfrica$mean)
dataset_geoAsiaPacificAfrica<-data.frame(cbind(geoAsiaPacificAfrica_dataset2,forecasted_geoAsiaPacificAfrica))

forecasted_geoEurope <- forecast(model_geoEurope, h=numPeriodsToForecast)
forecasted_geoEurope<- as.numeric(forecasted_geoEurope$mean)
dataset_geoEurope<-data.frame(cbind(geoEurope_dataset2,forecasted_geoEurope))

forecasted_geoNorthAmerica <- forecast(model_geoNorthAmerica, h=numPeriodsToForecast)
forecasted_geoNorthAmerica<- as.numeric(forecasted_geoNorthAmerica$mean)
dataset_geoNorthAmerica<-data.frame(cbind(geoNorthAmerica_dataset2,forecasted_geoNorthAmerica))

forecasted_geoSouthAmerica <- forecast(model_geoSouthAmerica, h=numPeriodsToForecast)
forecasted_geoSouthAmerica<- as.numeric(forecasted_geoSouthAmerica$mean)
dataset_geoSouthAmerica<-data.frame(cbind(geoSouthAmerica_dataset2,forecasted_geoSouthAmerica))

forecasted_geoGlobalTransport <- forecast(model_geoGlobalTransport, h=numPeriodsToForecast)
forecasted_geoGlobalTransport<- as.numeric(forecasted_geoGlobalTransport$mean)
dataset_geoGlobalTransport<-data.frame(cbind(geoGlobalTransport_dataset2,forecasted_geoGlobalTransport))

str(geoAsiaPacificAfrica_combined)
train <- as.data.frame(as.numeric(model_geoAsiaPacificAfrica$fitted))
valid <- as.data.frame(as.numeric(dataset_geoAsiaPacificAfrica$forecasted_geoAsiaPacificAfrica))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
geoAsiaPacificAfrica_combined$forecasted <- temp$forecastedValues

str(geoEurope_combined)
train <- as.data.frame(as.numeric(model_geoEurope$fitted))
valid <- as.data.frame(as.numeric(dataset_geoEurope$forecasted_geoEurope))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
geoEurope_combined$forecasted <- temp$forecastedValues

str(geoNorthAmerica_combined)
train <- as.data.frame(as.numeric(model_geoNorthAmerica$fitted))
valid <- as.data.frame(as.numeric(dataset_geoNorthAmerica$forecasted_geoNorthAmerica))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
geoNorthAmerica_combined$forecasted <- temp$forecastedValues

str(geoSouthAmerica_combined)
train <- as.data.frame(as.numeric(model_geoSouthAmerica$fitted))
valid <- as.data.frame(as.numeric(dataset_geoSouthAmerica$forecasted_geoSouthAmerica))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
geoSouthAmerica_combined$forecasted <- temp$forecastedValues

str(geoGlobalTransport_combined)
train <- as.data.frame(as.numeric(model_geoGlobalTransport$fitted))
valid <- as.data.frame(as.numeric(dataset_geoGlobalTransport$forecasted_geoGlobalTransport))
names(train)<-c('forecastedValues')
names(valid)<-c('forecastedValues')
temp<-NULL
temp<- rbind(train,valid)
geoGlobalTransport_combined$forecasted <- temp$forecastedValues

plot(geoAsiaPacificAfrica_combined$time, geoAsiaPacificAfrica_combined$data,type="l",col="royalblue",xlab="Time",ylab="Volume (MB)",lwd=2, ylim=c(100,3000000))
#lines(geoAsiaPacificAfrica_combined$time,geoAsiaPacificAfrica_combined$forecasted, col="red", lty=4, lwd=2)
#legend("topleft",legend = c("Original Data-Asia Pacific & Africa","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("royalblue","red"), lwd=2)

lines(geoEurope_combined$time,geoEurope_combined$data,  col="dark green", lwd=2)
#lines(geoEurope_combined$time,geoEurope_combined$forecasted, col="red", lty=4, lwd=2)
#legend("topright",legend = c("Original Data-Europe","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("dark green","red"), lwd=2)

lines(geoNorthAmerica_combined$time,geoNorthAmerica_combined$data,  col="pink", lwd=2)
#lines(geoNorthAmerica_combined$time,geoNorthAmerica_combined$forecasted, col="red", lty=4, lwd=2)
#legend("topright",legend = c("Original Data-Europe","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("dark green","red"), lwd=2)

lines(geoSouthAmerica_combined$time,geoSouthAmerica_combined$data,  col="red", lwd=2)
#lines(geoSouthAmerica_combined$time,geoSouthAmerica_combined$forecasted, col="red", lty=4, lwd=2)
#legend("topright",legend = c("Original Data-Europe","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("dark green","red"), lwd=2)

lines(geoGlobalTransport_combined$time,geoGlobalTransport_combined$data,  col="purple", lwd=2)
#lines(geoGlobalTransport_combined$time,geoGlobalTransport_combined$forecasted, col="red", lty=4, lwd=2)
#legend("topright",legend = c("Original Data-Europe","Double Seasonal Holt-Winters Forecast"),bty=c("n","n"),lty=c(1,4),col=c("dark green","red"), lwd=2)
legend("topright",legend = c("Asia, Pacific & Africa","Europe", "North America", "South America", "Global Transport"),
                              bty=c("n","n","n","n","n"),lty=c(1,1,1,1,1),col=c("royalblue","dark green","pink","red","purple"), lwd=2)



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

