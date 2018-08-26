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
library(data.table)

#Setting directory
setwd("results_folder/geoSourceDest") 
getwd()

memory.limit()
memory.limit(46217)

geolookup_df <- read.table(file='lookupgeo.csv',header=FALSE,
                     sep=',',col.names=c('Key','Value'))
str(geolookup_df)
geo_tb<- data.table(geolookup_df,key='Key')
geo_tb['136.1.0.0/16']

file.names <- list.files(getwd(), pattern="*batch*", all.files=FALSE, full.names=FALSE)
file.names[4]
file.names

for(i in 4:4){
  file.names[i]
  dataFileGeo <- paste("moddata/",file.names[i],"mod",sep="")
  df1 <- list("timeoriginal","subnet", "totalbytes","geolocation")
  write.table(df1, append=TRUE, col.names=F, row.names=F, file = dataFileGeo, sep = ",")
  
  geodata_original <- read.csv(file.names[i], header=FALSE, stringsAsFactors = FALSE)
  str(geodata_original)
  names(geodata_original) <-c("timeoriginal","subnet", "totalbytes")
  geodata_df <- geodata_original[order(geodata_original$timeoriginal),]
  str(geodata_df)
  
  for (row in 1:nrow(geodata_df)) {
    subnet_string <-  paste(geodata_df[row,"subnet"],"", sep="")
    df <- list(geodata_df[row,"timeoriginal"], geodata_df[row,"subnet"], geodata_df[row,"totalbytes"],
               geo_tb[subnet_string] )
    write.table(df, append=TRUE, col.names=F, row.names=F, file = dataFileGeo, sep = ",")
  }
  
}


#######################create files to make geo graphy #############################################

#Setting directory
setwd("results_foldergeoSourceDest/moddata") 
getwd()

memory.limit()
memory.limit(46217)

#geolookup_df <- read.table(file='lookupgeo.csv',header=FALSE,sep=',',col.names=c('Key','Value'))


file.names <- list.files(getwd(), pattern="geo*", all.files=FALSE, full.names=FALSE)
file.names

dataFileGeo <- paste("geoData","Final.csv",sep="")
df1 <- list("timeoriginal", "totalbytes","geolocation")
write.table(df1, append=TRUE, col.names=F, row.names=F, file = dataFileGeo, sep = ",")

for(i in 1:4){
  file.names[i]

  geodata_original <- read.csv(file.names[i], header=TRUE, stringsAsFactors = FALSE)
  str(geodata_original)
  #names(geodata_original) <-c("timeoriginal","subnet", "totalbytes")
  #geodata_df <- geodata_original[order(geodata_original$timeoriginal),]
  #str(geodata_df)

  #geodata_original$timeoriginal <- as.POSIXct(strptime(geodata_original$timeoriginal, format="%Y-%m-%d %H"))
  #df1[with(df1, timeStamp >= "2015-01-05 15:01:00" & timeStamp <= "2015-01-05 15:02:00"), ]
  #geodata_df<-geodata_original[with(geodata_original, timeoriginal >= "2018-02-10 00" & timeoriginal<= "2018-02-20 15:02:00"),]
  #nrow(geodata_df)
  
  #test3$hFac <- droplevels(cut(geodata_original$timemod, breaks="hour"))

  #res2 <- aggregate(cbind(x1, x2) ~ hFac + hosts, data=Data, FUN=mean)
  res2 <- aggregate(totalbytes ~ timeoriginal + geolocation , data=geodata_original, FUN=sum)
  head(res2)
  nrow(res2)
  write.table(res2, append=TRUE, col.names=F, row.names=F, file = dataFileGeo, sep = ",")
}
