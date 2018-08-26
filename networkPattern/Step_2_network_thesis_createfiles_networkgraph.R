#https://stats.stackexchange.com/questions/7268/how-to-aggregate-by-minute-data-for-a-week-into-hourly-means
# Lwin Moe, 

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
#library(anomalyDetection)

#Setting directory
setwd("results_folder/networkGraph") 
getwd()



memory.limit()
memory.limit(46217)
test = read.csv('batch3ab', header=TRUE, stringsAsFactors = FALSE)

#nrow(test)
#str(test)
test2 <- na.omit(test)
#nrow(test2)
#head(test2)
names(test2) <- c("timeoriginal", "sourceip", "destinationip")
#test2[order(test2$timeoriginal),]
#test2$time <- seq.int(nrow(test2))
#nrow(test2)
#test2$timemod <- as.POSIXct(strptime(test2$timeoriginal, format="%Y-%m-%d %H"))

#summary(test$data)
#quantile(test$data, c(.32, .57, .95)) 

#SparkRNetwork_DF<-test2[with(test2, timemod >= "2018-02-12 00" & timemod<= "2018-02-15 00"),]

SparkRNetwork_DF <- test2
#nrow(SparkRNetwork_DF)
#test3$hFac <- droplevels(cut(test3$timemod, breaks="hour"))
#res2 <- aggregate(cbind(x1, x2) ~ hFac + hosts, data=Data, FUN=mean)

#str(SparkRNetwork_DF)
date.array <- c("2018-02-14 00", "2018-02-14 01", "2018-02-14 02", "2018-02-14 03", "2018-02-14 04", "2018-02-14 05",
                "2018-02-14 06", "2018-02-14 07", "2018-02-14 08", "2018-02-14 09", "2018-02-14 10", "2018-02-14 11", "2018-02-14 12", "2018-02-14 13", "2018-02-14 14", "2018-02-14 15", "2018-02-14 16", "2018-02-14 17", "2018-02-14 18", "2018-02-14 19", "2018-02-14 20", "2018-02-14 21", "2018-02-14 22", "2018-02-14 23",
                "2018-02-15 00", "2018-02-15 01", "2018-02-15 02", "2018-02-15 03", "2018-02-15 04", "2018-02-15 05",
                "2018-02-15 06", "2018-02-15 07", "2018-02-15 08", "2018-02-15 09", "2018-02-15 10", "2018-02-15 11", "2018-02-15 12", "2018-02-15 13", "2018-02-15 14", "2018-02-15 15", "2018-02-15 16", "2018-02-15 17", "2018-02-15 18", "2018-02-15 19", "2018-02-15 20", "2018-02-15 21", "2018-02-15 22", "2018-02-15 23")

# date.array <- c("2018-02-12 00", "2018-02-12 01", "2018-02-12 02", "2018-02-12 03", "2018-02-12 04", "2018-02-12 05",
#                 "2018-02-12 06", "2018-02-12 07", "2018-02-12 08", "2018-02-12 09", "2018-02-12 10", "2018-02-12 11", "2018-02-12 12", "2018-02-12 13", "2018-02-12 14", "2018-02-12 15", "2018-02-12 16", "2018-02-12 17", "2018-02-12 18", "2018-02-12 19", "2018-02-12 20", "2018-02-12 21", "2018-02-12 22", "2018-02-12 23",
#                 "2018-02-13 00", "2018-02-13 01", "2018-02-13 02", "2018-02-13 03", "2018-02-13 04", "2018-02-13 05",
#                 "2018-02-13 06", "2018-02-13 07", "2018-02-13 08", "2018-02-13 09", "2018-02-13 10", "2018-02-13 11", "2018-02-13 12", "2018-02-13 13", "2018-02-13 14", "2018-02-13 15", "2018-02-13 16", "2018-02-13 17", "2018-02-13 18", "2018-02-13 19", "2018-02-13 20", "2018-02-13 21", "2018-02-13 22", "2018-02-13 23")

length(date.array)
date.array[1]
for(m in 1:length(date.array)){
   current_hour <- filter(SparkRNetwork_DF, SparkRNetwork_DF$timeoriginal == date.array[m])
   current_hour_df <- collect(current_hour)
   dataFileNetworkGraph <- paste("NGFile_",date.array[m], ".csv", sep="")
   #write.table(current_hour_df, append=TRUE, col.names=F, row.names=F, file = dataFileNetworkGraph, sep = ",")
}

######################################actual creation of the consolidated network data##################################
#http://igraph.org/r/doc/ <- All functions of igraph
#http://jfaganuk.github.io/2015/01/24/basic-network-analysis/#whole-network
#network3D, cool stuff. https://christophergandrud.github.io/networkD3/

setwd("results_foldernetworkGraph") 
memory.limit(46217)
file.names <- list.files(getwd(), pattern="NG*", all.files=FALSE, full.names=FALSE)
length(file.names)
file.names
test <- NULL

#dataFileNetworkGraph <- "networkGraphSummary.csv"

#df = list("timeoriginal","ecount", "vcount","graph_density","max_degree","max_name")
#write.table(df, append=TRUE, col.names=F, row.names=F, file = dataFileNetworkGraph, sep = ",")
file.names[14]

for(i in 14:14){
  file.names[i]
  browser()
  graphData_original <-NULL
  graphData_original <- read.csv(file.names[i], header=FALSE, stringsAsFactors = FALSE)
  #str(graphData_original)
  names(graphData_original) <- c("timeoriginal", "sourceip", "destinationip")

  graphData_df <- graphData_original[c(2,3)]
  graphData_df <- distinct(graphData_df)
  #str(graphData_df)
  
 
  
  #graphData_df<- graphData_df[1:1400,]
  graph.edges <- as.matrix((graphData_df))
  g <- graph.edgelist(graph.edges, directed = FALSE)
  g <- simplify(g, remove.multiple = F, remove.loops = T)
  tkplot(g,edge.color="gray", vertex.color="coral", vertex.size=4)
  #tkplot(MGraph, vertex.size=3, vertex.label=NA, edge.arrow.size=0.5, edge.color="black")
  
  #graph_from_data_frame(graphData_df, directed = FALSE, vertices = NULL)
  #$simpleNetwork(graphData_df)
  
  ecount <- ecount(g)
  vcount <- vcount(g)
  vgraph_density <- graph.density(g)
  
  #max degree node
  max_degree <- max(degree(g))
  max_name=V(g)$name[degree(g)==max(degree(g))]
  #hist(degree)
  
  #average.path.length(g)
  #farthest.nodes(g)
  #centralization.betweenness(g1.edge3)$centralization
  #centralization.degree(g1.edge3)$centralization
  #eigen_centrality <- eigen_centrality(g)
  
  #diameter(g, directed=F,weights=NA)
  #diam <- get_diameter(g, directed=T)

  #edge_density <- edge_density(g, loops=T)
  #http://kateto.net/networks-r-igraph
  
  #Show modularity
  #g.edge3 <- subgraph.edges(g, which(E(g)$weight < 4))
  com <- edge.betweenness.community(g)
  V(g)$memb <- com$membership
  modularity(com)
  com <- edge.betweenness.community(g)
  plot(com, g)
  
  #graph_from_data_frame(d, directed = TRUE, vertices = NULL)
  #simpleNetwork(test2)
  
  #df= list(graphData_original[1,"timeoriginal"],ecount, vcount,graph_density,max_degree,max_name)
  #write.table(df, append=TRUE, col.names=F, row.names=F, file = dataFileNetworkGraph, sep = ",")
}

