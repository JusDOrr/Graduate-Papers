#Q1 - Import Data
# Setup the column names
votercols <- c("Party","HI","WPCS","AOTBR","PFF","ESA","RGIS","ASTB","ATNC","MxM","Imm","SCC","ES","SRTS","Crm","DFE","EAASA");

# Read in the .txt file data
voters = read.table("house-votes.txt", header = F, sep=",", stringsAsFactors=T, col.names=votercols)

#Q2 - Prepare the Data
# Alter Categorical rows to numeric by adding Dummy Coded, Yes/No Vote Rows
HouseVotes$HI.YES <- ifelse(HouseVotes$HI == "y", 1, 0)
HouseVotes$HI.NO <- ifelse(HouseVotes$HI == "n", 1, 0)
HouseVotes$WPCS.YES <- ifelse(HouseVotes$WPCS == "y", 1, 0)
HouseVotes$WPCS.NO <- ifelse(HouseVotes$WPCS == "n", 1, 0)
HouseVotes$AOTBR.YES <- ifelse(HouseVotes$AOTBR == "y", 1, 0)
HouseVotes$AOTBR.NO <- ifelse(HouseVotes$AOTBR == "n", 1, 0)
HouseVotes$PFF.YES <- ifelse(HouseVotes$PFF == "y", 1, 0)
HouseVotes$PFF.NO <- ifelse(HouseVotes$PFF == "n", 1, 0)
HouseVotes$ESA.YES <- ifelse(HouseVotes$ESA == "y", 1, 0)
HouseVotes$ESA.NO <- ifelse(HouseVotes$ESA == "n", 1, 0)
HouseVotes$RGIS.YES <- ifelse(HouseVotes$RGIS == "y", 1, 0)
HouseVotes$RGIS.NO <- ifelse(HouseVotes$RGIS == "n", 1, 0)
HouseVotes$ASTB.YES <- ifelse(HouseVotes$ASTB == "y", 1, 0)
HouseVotes$ASTB.NO <- ifelse(HouseVotes$ASTB == "n", 1, 0)
HouseVotes$ATNC.YES <- ifelse(HouseVotes$ATNC == "y", 1, 0)
HouseVotes$ATNC.NO <- ifelse(HouseVotes$ATNC == "n", 1, 0)
HouseVotes$MxM.YES <- ifelse(HouseVotes$MxM == "y", 1, 0)
HouseVotes$MxM.NO <- ifelse(HouseVotes$MxM == "n", 1, 0)
HouseVotes$Imm.YES <- ifelse(HouseVotes$Imm == "y", 1, 0)
HouseVotes$Imm.NO <- ifelse(HouseVotes$Imm == "n", 1, 0)
HouseVotes$SCC.YES <- ifelse(HouseVotes$SCC == "y", 1, 0)
HouseVotes$SCC.NO <- ifelse(HouseVotes$SCC == "n", 1, 0)
HouseVotes$ES.YES <- ifelse(HouseVotes$ES == "y", 1, 0)
HouseVotes$ES.NO <- ifelse(HouseVotes$ES == "n", 1, 0)
HouseVotes$SRTS.YES <- ifelse(HouseVotes$SRTS == "y", 1, 0)
HouseVotes$SRTS.NO <- ifelse(HouseVotes$SRTS == "n", 1, 0)
HouseVotes$Crm.YES <- ifelse(HouseVotes$Crm == "y", 1, 0)
HouseVotes$Crm.NO <- ifelse(HouseVotes$Crm == "n", 1, 0)
HouseVotes$DFE.YES <- ifelse(HouseVotes$DFE == "y", 1, 0)
HouseVotes$DFE.NO <- ifelse(HouseVotes$DFE == "n", 1, 0)
HouseVotes$EAASA.YES <- ifelse(HouseVotes$EAASA == "y", 1, 0)
HouseVotes$EAASA.NO <- ifelse(HouseVotes$EAASA == "n", 1, 0)

# Create a variable from the Party data and the numeric Dummy-Coded Votes
HouseVotesData <- HouseVotes[c(1,18:49)]

#Q3 - Run 2 ML Algorithms and compare them
# Run the KNN Algorithm against the dataset
library(class)
HouseVotesTrainKNN <- HouseVotesData[1:335, -1]
HouseVotesTestKNN <- HouseVotesData[336:435, -1]
HouseVotesTrainLabels <- HouseVotesData[1:335, 1]
HouseVotesTestLabels <- HouseVotesData[336:435, 1]
set.seed(12)
KNNPredict <- knn(train=HouseVotesTrainKNN, test=HouseVotesTestKNN, cl=HouseVotesTrainLabels, k=10)

# Run the C5.0 Algorithm against the dataset
library(C50)
HouseVotesTestC50 <- HouseVotesData[336:435, -1]
HouseVotesTestLabels <- HouseVotesData[336:435, 1]
set.seed(12)
C50Model <- C5.0(HouseVotesData[-1], HouseVotesData$Party, trials = 10)
C50Predict <- predict(C50Model, HouseVotesTestC50)

# Compare using Confusion Matrices and performance measures
library(gmodels)
library(caret)
CrossTable(HouseVotesTestLabels, KNNPredict)
CrossTable(HouseVotesTestLabels, C50Predict)

confusionMatrix(KNNPredict, HouseVotesTestLabels, positive = "republican")
confusionMatrix(C50Predict, HouseVotesTestLabels, positive = "republican")

#Q4 - Use 10-fold CV to estimate performance. Compare. pg. 340
library(irr)
set.seed(123)
folds <- createFolds(HouseVotesData$Party, k=10)

# 10-fold CV KNN
KNNCVResults <- lapply(folds, function(x) {
KNNTrain <- HouseVotesData[-x, -1]
KNNTest <- HouseVotesData[x, -1]
KNNPredict <- knn(train=KNNTrain, test=KNNTest, cl=HouseVotesData[-x,1], k=10)
KNNActual <- HouseVotesData[x, 1]
kappa <- kappa2(data.frame(KNNActual, KNNPredict))$value
return(kappa)
})
str(KNNCVResults)
mean(unlist(KNNCVResults))

# 10-fold CV C5.0
C50CVResults <- lapply(folds, function(x) {
C50Train <- HouseVotesData[-x,]
C50Test <- HouseVotesData[x,]
C50Model <- C5.0(Party ~ ., data = C50Train)
C50Predict <- predict(C50Model, C50Test)
C50Actual <- C50Test$Party
kappa <- kappa2(data.frame(C50Actual, C50Predict))$value
return(kappa)
})
str(C50CVResults)
mean(unlist(C50CVResults))

#Q5 - Perform automated parameter tuning using caret. pg. 352
# KNN Parameter Tuning
set.seed(299)
KNNParameterModel <- train(Party ~ ., data=HouseVotesData, method ="knn")
KNNParameterModel
KNNParameterPredict <- predict(KNNParameterModel, HouseVotesData)
table(KNNParameterPredict, HouseVotesData$Party)

# C5.0 Parameter Tuning
set.seed(299)
C50ParameterModel <- train(Party ~ ., data=HouseVotesData, method ="C5.0")
C50ParameterModel
C50ParameterPredict <- predict(C50ParameterModel, HouseVotesData)
table(C50ParameterPredict, HouseVotesData$Party)

#Q6 - Improve performance using ensemble learning and caret. Compare. pg. 359
# Ensemble Algorithm - Random Forests
library(randomForest)
ctrl <- trainControl(method="repeatedcv", number=10, repeats=10)

# KNN
set.seed(350)
KNNGrid <- expand.grid(.k=c(2,3,4,5,6,7,8))
KNNRFModel <- train(Party ~ ., data = HouseVotesData, method="knn", metric = "Kappa", trControl=ctrl, tuneGrid=KNNGrid)
KNNRFModel

# C5.0
set.seed(350)
C50Grid <- expand.grid(.model = "tree", .trials = c(5,10,15,20), .winnow = "FALSE")
C50RFModel <- train(Party ~ ., data = HouseVotesData, method="C5.0", metric = "Kappa", trControl=ctrl, tuneGrid=C50Grid)
C50RFModel
