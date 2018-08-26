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
library(SparkR)
library(caret)
library(class)
library('e1071')
library(scales)
library(dplyr)

#Setting directory
setwd("results_folder") 
getwd()

#network_dataoriginal = read.csv('output.csv', header=TRUE, stringsAsFactors = FALSE)

file.names <- list.files(getwd(), pattern="SourceDestinationbatch*", all.files=FALSE, full.names=FALSE)
length(file.names)
file.names
test <- NULL
for(i in 1:length(file.names)){
  file.names[i]
  temptest <-NULL
  temptest <- read.csv(file.names[i], header=FALSE, stringsAsFactors = FALSE)
  test <- rbind(test, temptest)
}
nrow(test)

test2 <- na.omit(test)
nrow(test2)
str(test2)
names(test2) <- c("index", "timeoriginal", "data")
test2[order(test2$timeoriginal),]
test2$time <- seq.int(nrow(test2))
nrow(test2)
test2$timemod <- as.POSIXct(strptime(test2$timeoriginal, format="%Y-%m-%d %H:%M:%S"))

#summary(test$data)
#quantile(test$data, c(.32, .57, .95)) 

#df1[with(df1, timeStamp >= "2015-01-05 15:01:00" & timeStamp <= "2015-01-05 15:02:00"), ]
test3<-test2[with(test2, timemod >= "2018-02-10 00:00:00" & timemod<= "2018-02-20 15:02:00"),]
#nrow(test3)
test4<-test2[with(test2, timemod >= "2018-02-12 00:00:00" & timemod<= "2018-02-20 15:02:00"),]
nrow(test4)

test3$hFac <- droplevels(cut(test3$timemod, breaks="hour"))
str(test3)
test4$hFac <- droplevels(cut(test4$timemod, breaks="min"))
str(test4)
nrow(test4)

#res2 <- aggregate(cbind(x1, x2) ~ hFac + hosts, data=Data, FUN=mean)
res2 <- aggregate(data ~ hFac , data=test3, FUN=sum)
head(res2)
res3 <- aggregate(data ~ hFac , data=test4, FUN=sum)
head(res3)
nrow(res3)

barplot(res2$data, names.arg=res2$hFac,border="blue",main="Time vs. Total Bytes", xlab="Time",  
        ylab="Total Bytes") #+ ylim(c(0, 115364100))

ggplot(test2, aes(timemod,data)) + geom_point() + theme(axis.text.x = element_text(angle = 90)) +
  scale_x_datetime(labels = date_format("%Y-%m-%d %H:%M:%S")) +
  ylab("Volume (Bytes)") + xlab("Time") + scale_y_continuous(limits=c(0,115364100)) #+ ylim(c(0, 8000))

#str(test2)
#write.csv(test2, file = "totalbytes10days.csv")
#str(res2)
#write.csv(res2, file = "totalbytes10dayshourly.csv")
str(res3)
write.csv(res3, file = "totalbytes10dayminutes.csv")
