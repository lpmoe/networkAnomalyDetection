install.packages("devtools")
devtools::install_github("twitter/AnomalyDetection")
library(AnomalyDetection)
Sys.getenv("R_LIBS_USER")
setwd("results_folder") 
help(AnomalyDetectionTs)
help(AnomalyDetectionVec)


data(raw_data)
View(raw_data)
str(raw_data)
res = AnomalyDetectionTs(raw_data, max_anoms=0.02, direction='both', plot=TRUE)
res = AnomalyDetectionVec(raw_data, max_anoms=0.02, direction='both', plot=TRUE)
res$plot

####################################testing twitter with network data###########################

dataset_meanbytes_index=read.csv('totalbytes10dayminutes.csv', header=TRUE, stringsAsFactors = FALSE)
str(dataset_meanbytes_index)
#names(dataset_meanbytes_index) <- c("index", "timestamp", "count")

#str(dataset_meanbytes_index)
#dataset_meanbytes_index$timestamp<- as.POSIXlt(strptime(dataset_meanbytes_index$timestamp, format="%m/%d/%Y %H:%M"))
#str(dataset_meanbytes_index)

dataset_meanbytes_index_final <- dataset_meanbytes_index[,c(2,3)]


#dataset_meanbytes_index_final <- dataset_meanbytes_index[,c(3,4)]
names(dataset_meanbytes_index_final) <- c("timestamp", "count")
str(dataset_meanbytes_index_final)
head(dataset_meanbytes_index_final)
dataset_meanbytes_index_final$timestamp<- as.POSIXlt(strptime(dataset_meanbytes_index_final$timestamp, format="%Y-%m-%d %H:%M:%S"))

test<-dataset_meanbytes_index_final
#test<-dataset_meanbytes_index_final[with(dataset_meanbytes_index_final, timestamp >= "2018-02-10 00:00:00" & timestamp<= "2018-02-20 15:02:00"),]
str(test)
#plot(test)
test$count <- log(test$count, base = exp(1))
res = AnomalyDetectionTs(test, max_anoms=0.2, direction='both', plot=TRUE, na.rm=TRUE)#,alpha = 0.4, longterm=TRUE)
res$plot

res = AnomalyDetectionVec(dataset_meanbytes3, max_anoms=0.02, direction='both', plot=TRUE)
res$plot

#############################Facebook Prophet############################
install.packages('prophet')
library(prophet)
library(dplyr)

names(dataset_meanbytes3) <- c("ds", "y")
dataset_meanbytes3$ds <- as.factor(dataset_meanbytes3$ds)
str(dataset_meanbytes3)
View(dataset_meanbytes3)
#dataset_meanbytes3$ds<- as.POSIXct(strptime(dataset_meanbytes3$ds, format="%d/%m/%Y %H:%M"))
# R
df2 <- read.csv('example_wp_peyton_manning.csv') %>% mutate(y = log(y))
str(df2)
View(df2)
df <- dataset_meanbytes3 %>%
  mutate(y = log(y))
str(df)
m<- prophet(df)
# R
future <- make_future_dataframe(m, periods = 365)
tail(future)

forecast <- predict(m, future)
tail(forecast[c('ds', 'yhat', 'yhat_lower', 'yhat_upper')])

plot(m, forecast)
prophet_plot_components(m, forecast)


###################################################test###############################################3333

network_dataoriginal = read.csv('totalbytes10dayshourly.csv', header=TRUE, stringsAsFactors = FALSE)
View(network_dataoriginal)
network_data<- network_dataoriginal
str(network_data)

network_data$Total.Bytes <- as.numeric(gsub(",", "",network_data$Total.Bytes))
#network_data$Storage.Time <- strptime(as.character(network_data$Storage.Time), format = "%m/%d/%Y %H:%M")

#means2 <- aggregate(Total.Bytes~First.Packet.Time,network_data,mean)
meanbytes <- aggregate(Total.Bytes~Storage.Time,network_data, mean)
#meanbytes <- aggregate(Total.Bytes~Storage.Time,network_data, sum)
port <- aggregate(Total.Bytes~Destination.Port,network_data,sum)
protocol <- aggregate(Total.Bytes~Protocol,network_data,sum)
head(port)
head(protocol)
port[order(port$Total.Bytes),]
protocol[order(protocol$Total.Bytes),]

str(protocol)
barplot(protocol$Total.Bytes, names.arg=protocol$Protocol,border="blue",main="Protocols vs. Total Bytes", xlab="Protocol",  
        ylab="Total Bytes")

#means2$First.Packet.TimeMod <- as.POSIXct(strptime(means2$First.Packet.Time, format="%b %d, %Y, %H:%M:%S %t"))
meanbytes$Storage.TimeMod <- as.POSIXct(strptime(meanbytes$Storage.Time, format="%b %d, %Y, %H:%M:%S %t"))
meanbytes[order(meanbytes$Storage.TimeMod),c(1,2,3)]
head(meanbytes)

ggplot(meanbytes, aes(Storage.TimeMod, Total.Bytes)) + geom_line() + scale_x_datetime('Time') + 
  ylab("Volume (Bytes)") + xlab("") #+ ylim(c(0, 8000))

colnames(meanbytes)[which(names(meanbytes) == "Total.Bytes")] <- "data"
write.csv(meanbytes, file = "meanbytes.csv")
write.csv(port, file ="port.csv")
write.csv(protocol, file = "protocol.csv")

#dataset_meanbytes = read.csv('meanbytes.csv', header=TRUE, stringsAsFactors = FALSE)
dataset_meanbytes = read.csv('totalbytes10dayshourlydouble.csv', header=TRUE, stringsAsFactors = FALSE)
str(dataset_meanbytes)
names(dataset_meanbytes)
