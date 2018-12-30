library(tree)
library(ISLR)
library(randomForest)
library(e1071)
library(mlbench)
library(caret)
library(Boruta)
library(naivebayes)
library(MLmetrics)
library(pROC)
library(nnet)
#library("iml")

##################################
################# PRE-PROCESSING
##################################

#### Pre-processing ####

#read preprocesser dataset

processedMimicData<- read.delim(file = "delirium_preprocessing_long.csv", sep = ",", header = FALSE)
colnames(processedMimicData) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
                                  'P0', 'P1', 'P2', 'P3', 'P3a', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16',
                                  'ADEs',
                                  'LG100-4', 'LG103-8', 'LG27-5', 'LG55-6', 'LG74-7', 'LG78-8', 'LG80-4', 'LG97-8',
                                  'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
                                  'A16', 'B01', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
                                  'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
                                  'H01', 'H02', 'H03', 'H04', 'H05', 'J01', 'J02', 'J04', 'J05', 'J06', 'J07', 'L01', 'L02', 'L03', 'L04',
                                  'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
                                  'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
                                  'V08', 'V09', 'V10', 'v20', 'Renal', 'Alb', 'Plat')



#normalize the non-categorical values: trestbps(4), chol(5), thalach(8), oldpeak(10)
#categorical values: sex(2), cp(3), fbs(6), restecg(7), exang(9), slope(11),thal(13),num(14) 
#others:age (1), ca(12)


#Put as factor
#processedMimicData <- lapply(processedMimicData, factor)
processedMimicData$ADEs[processedMimicData$ADEs== 1] <- "ADE"
processedMimicData$ADEs[processedMimicData$ADEs== 0] <- "NOADE"

#normalize <- function(x) {
#  return ((x - min(x)) / (max(x) - min(x)))
#}
#for(i in 4:ncol(processedMimicData)){

#  processedMimicData[,i] <- as.numeric(processedMimicData[,i])
#  processedMimicData[,c(i)]<-normalize(processedMimicData[,c(i)])

#}

processedMimicData[,24] <- as.factor(processedMimicData[,24])
processedMimicData[,127] <- as.factor(processedMimicData[,127])
processedMimicData[,128] <- as.factor(processedMimicData[,128])
processedMimicData[,129] <- as.factor(processedMimicData[,129])


summary(processedMimicData)
# normalise function


# use boruta algorithm to find unimporant variables
borutaFeatureImportance <- Boruta(ADEs ~., data =processedMimicData[,-1], maxRuns =100, doTrace = 2)
print(borutaFeatureImportance)
plot(borutaFeatureImportance)


##################################
############## TRAINING PARAMETERS
##################################

# prepare training scheme
seedNumber = 3


columnToPredict = c(24)
n <- names(processedMimicData)


# Create formula for the classification model with all the variables
f <- as.formula(paste(paste(n[columnToPredict], collapse = " + "), paste(" ~"), paste(getSelectedAttributes(borutaFeatureImportance, withTentative = TRUE) , collapse = " + "))); f
#use this as f(results of boruta feature importance)
#f <- as.formula(ADEs ~ admtyp + age + hospital_stay + P0 + P1 + P5 + P6 + P7 + 
 #                 P9 + P11 + P12 + P14 + P16 + `LG100-4` + `LG27-5` + `LG55-6` + 
  #                `LG74-7` + `LG97-8` + A01 + B01 + C01 + D01 + G01 + H01 + 
   #               J01 + L01 + M01 + N01 + P01 + R01 + S01 + V01 + Renal + Alb)

set.seed(seedNumber)
control <- trainControl(method="cv",
                        number=10,
                        classProbs=TRUE,
                        summaryFunction = prSummary, #Compare using AUC, precision and recall.
                        savePredictions = TRUE)
# 
# folds <- 19
# cvIndex <- createFolds(factor(training$Y), folds, returnTrain = T)
# control <- trainControl(index = cvIndex,
#                    method = 'cv', 
#                    number = folds)
# 
# rfFit <- train(Y ~ ., data = training, 
#                method = "rf", 
#                trControl = control,
#                maximize = TRUE,
#                verbose = FALSE, ntree = 1000)


##################################
######TRAIN CLASSIFICATION MODELS
##################################

# Random Forest with 100 decision trees

set.seed(seedNumber)
rF100 <- train(f, 
               data=processedMimicData[, -1], 
               method="rf", ntree=100, 
               trControl=control)


# Random Forest with 500 decision trees
#set.seed(seedNumber)
#rF500 <- train(f, data=processedMimicData[, -1], method="rf", ntree=500, trControl=control)

#NNetwork
set.seed(seedNumber)
NNetwork <- train(f,
                  data = processedMimicData[,-1], 
                  method = "nnet", 
                  trControl = control)


# Polynomial Support Vector Machine
set.seed(seedNumber)
SVMPolynomial<- train(f, data = processedMimicData[,-1], 
                      method= 'svmPoly', 
                      trControl=control)


# Radial Support Vector Machine
set.seed(seedNumber)
SVMRadial<- train(f, data =processedMimicData[, -1], 
                  method= 'svmRadial', 
                  trControl=control)


models.names <- c("modelrF100.CRTHI",
                  "modelSVMPolynomial.CRTHI",
                  "modelSVMRadial.CRTHI",
                  "modelNNetwork.CRTHI")

#### Load Models ####
for (i in 1:length(models.names)){
  load(models.names[i])
}

# Generate structure that contains all the trained machine learning models
models.list <- list()
models.list[[1]] <- rF100
models.list[[2]] <- SVMPolynomial
models.list[[3]] <- SVMRadial
models.list[[4]] <- NNetwork

for(i in 1:length(models.list)) {
  print(models.list[[i]])
}


##################################
### SAVE FILES WITH NEW TRAINED MODELS
##################################
## Uncomment to save new trained models.
# save(rF100, file = toString(models.names[1]))
# save(rF500, file = toString(models.names[2]))
# save(SVMPolynomial, file = toString(models.names[5]))
# save(SVMRadial, file = toString(models.names[6]))
# save(NNetwork, file = toString(models.names[8]))


##################################
#### COMPARE PERFORMANCE OF MODELS
##################################
# confusion matrix: accuracy, AUC, precision, recall, F-measure
accuracy <-function(x) {(x[1,1]+x[2,2])/sum(sum(x))}

overallPerformance <- data.frame()
for(i in 1:length(models.list)) {
  performanceAcc <- accuracy(confusionMatrix(models.list[[i]])$table)
  performanceMoreMetrics <- getTrainPerf(models.list[[i]])[1:4]
  
  overallPerformance <- rbind.data.frame(overallPerformance,cbind(performanceAcc,performanceMoreMetrics))
}
colnames(overallPerformance) <- c("Accuracy", "AUC", "Precision", "Recall", "F-score")
rownames(overallPerformance)<- c("RF100","Polyn","Radial","NNet")
overallPerformance
View(overallPerformance)
