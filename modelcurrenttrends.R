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


#### Pre-processing ####

#read preprocessed dataset
processedMimicData <- read.delim(file = "2018_12_28_preprocessing_2.csv", sep = ",", header = FALSE)
processedAnticoagulants <- read.delim(file = "2018_12_28_preprocessed_anticoagulants.csv", sep = ",", header = FALSE)
processedImmuno<- read.delim(file = "2018_12_28_preprocessed_immuno.csv", sep = ",", header = FALSE)
processedCosticorteroid <- read.delim(file = "2018_12_28_preprocessed_costicorteroid.csv", sep = ",", header = FALSE)
processedAntibiotics<-read.delim(file = "2018_12_28_preprocessed_antibiotics.csv", sep = ",", header = FALSE)
processedDelirium<- read.delim(file = "delirium_preprocessing_long.csv", sep = ",", header = FALSE)


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

processedMimicData$ADEs[processedMimicData$ADEs== 1] <- "ADE"
processedMimicData$ADEs[processedMimicData$ADEs== 0] <- "NOADE"
processedMimicData[,24] <- as.factor(processedMimicData[,24])
processedMimicData[,127] <- as.factor(processedMimicData[,127])
summary(processedMimicData)


colnames(processedDelirium) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
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


processedDelirium$ADEs[processedDelirium$ADEs== 1] <- "ADE"
processedDelirium$ADEs[processedDelirium$ADEs== 0] <- "NOADE"
processedDelirium[,24] <- as.factor(processedDelirium[,24])
processedDelirium[,127] <- as.factor(processedDelirium[,127])
summary(processedDelirium)

#no B01
colnames(processedAnticoagulants) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
                                       'P0', 'P1', 'P2', 'P3', 'P3a', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16',
                                       'ADEs',
                                       'LG100-4', 'LG103-8', 'LG27-5', 'LG55-6', 'LG74-7', 'LG78-8', 'LG80-4', 'LG97-8',
                                       'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
                                       'A16', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
                                       'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
                                       'H01', 'H02', 'H03', 'H04', 'H05', 'J01', 'J02', 'J04', 'J05', 'J06', 'J07', 'L01', 'L02', 'L03', 'L04',
                                       'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
                                       'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
                                       'V08', 'V09', 'V10', 'v20', 'Renal', 'Alb', 'Plat')
processedAnticoagulants$ADEs[processedAnticoagulants$ADEs== 1] <- "ADE"
processedAnticoagulants$ADEs[processedAnticoagulants$ADEs== 0] <- "NOADE"
processedAnticoagulants$ADEs <- as.factor(processedAnticoagulants$ADEs)
processedAnticoagulants[,126] <- as.factor(processedAnticoagulants[,126])
summary(processedAnticoagulants)

# no L01 and L04
colnames(processedImmuno) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
                               'P0', 'P1', 'P2', 'P3', 'P3a', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16',
                               'ADEs',
                               'LG100-4', 'LG103-8', 'LG27-5', 'LG55-6', 'LG74-7', 'LG78-8', 'LG80-4', 'LG97-8',
                               'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
                               'A16', 'B01', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
                               'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
                               'H01', 'H02', 'H03', 'H04', 'H05', 'J01', 'J02', 'J04', 'J05', 'J06', 'J07', 'L02', 'L03',
                               'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
                               'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
                               'V08', 'V09', 'V10', 'v20', 'Renal', 'Alb', 'Plat')

#Put as factor class label and renal disease
processedImmuno$ADEs[processedImmuno$ADEs== 1] <- "ADE"
processedImmuno$ADEs[processedImmuno$ADEs== 0] <- "NOADE"
processedImmuno[,24] <- as.factor(processedImmuno[,24])
processedImmuno[,125] <- as.factor(processedImmuno[,125])
summary(processedImmuno)

#no H02
colnames(processedCosticorteroid) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
                                       'P0', 'P1', 'P2', 'P3', 'P3a', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16',
                                       'ADEs',
                                       'LG100-4', 'LG103-8', 'LG27-5', 'LG55-6', 'LG74-7', 'LG78-8', 'LG80-4', 'LG97-8',
                                       'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
                                       'A16', 'B01', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
                                       'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
                                       'H01', 'H03', 'H04', 'H05', 'J01', 'J02', 'J04', 'J05', 'J06', 'J07', 'L01', 'L02', 'L03', 'L04',
                                       'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
                                       'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
                                       'V08', 'V09', 'V10', 'v20', 'Renal', 'Alb', 'Plat')

processedCosticorteroid$ADEs[processedCosticorteroid$ADEs== 1] <- "ADE"
processedCosticorteroid$ADEs[processedCosticorteroid$ADEs== 0] <- "NOADE"
processedCosticorteroid$ADEs <- as.factor(processedCosticorteroid$ADEs)
processedCosticorteroid[,126] <- as.factor(processedCosticorteroid[,126])
summary(processedCosticorteroid)

#no J01
colnames(processedAntibiotics) <- c('hdm_id', 'admtyp', 'gender', 'age', 'hospital_stay', 
                                    'P0', 'P1', 'P2', 'P3', 'P3a', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P16',
                                    'ADEs',
                                    'LG100-4', 'LG103-8', 'LG27-5', 'LG55-6', 'LG74-7', 'LG78-8', 'LG80-4', 'LG97-8',
                                    'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15',
                                    'A16', 'B01', 'B02', 'B03', 'B05', 'B06', 'C01', 'C02', 'C03', 'C04', 'C05', 'C07', 'C08', 'C09', 'C10',
                                    'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10', 'D11', 'G01', 'G02', 'G03', 'G04',
                                    'H01', 'H02', 'H03', 'H04', 'H05', 'J02', 'J04', 'J05', 'J06', 'J07', 'L01', 'L02', 'L03', 'L04',
                                    'M01', 'M02', 'M03', 'M04', 'M05', 'M09', 'N01', 'N02', 'N03', 'N04', 'N05', 'N06', 'N07', 'P01', 'P02',
                                    'P03', 'R01', 'R02', 'R03', 'R05', 'R06', 'R07', 'S01', 'S02', 'S03', 'V01', 'V03', 'V04', 'V06', 'V07',
                                    'V08', 'V09', 'V10', 'v20', 'Renal', 'Alb', 'Plat')

processedAntibiotics$ADEs[processedAntibiotics$ADEs== 1] <- "ADE"
processedAntibiotics$ADEs[processedAnticoagulants$ADEs== 0] <- "NOADE"
processedAntibiotics$ADEs <- as.factor(processedAntibiotics$ADEs)
processedAntibiotics[,126] <- as.factor(processedAntibiotics[,126])
summary(processedAntibiotics)


#### SELECT DATA& Features ####


# use boruta algorithm to find unimporant variables
#borutaFeatureImportance <- Boruta(ADEs ~., data =selectedData[,-1], maxRuns =100, doTrace = 2)
#print(borutaFeatureImportance)
#plot(borutaFeatureImportance)
#FeaturesImportance <- as.data.frame(colMeans(borutaFeatureImportance[["ImpHistory"]]))
#FeaturesImportance
#getNonRejectedFormula(borutaFeatureImportance)


##### TRAIN AND TEST PARAMETERS ####

#use this as f: results of boruta feature importance per dataset
mimicDataFeatures <- as.formula(ADEs ~ admtyp + age + hospital_stay + P0 + P1 + P5 + P6 + P7 + 
                                  P9 + P11 + P12 + P16 + `LG100-4` + `LG27-5` + `LG55-6` + 
                                  `LG74-7` + `LG97-8` + A01 + A02 + A03 + A04 + A05 + A06 + 
                                  A07 + A10 + A11 + A12 + B01 + B02 + B03 + B05 + C01 + C02 + 
                                  C03 + C05 + C07 + C08 + C09 + C10 + D01 + D03 + D04 + D05 + 
                                  D06 + D07 + D08 + D09 + D10 + D11 + G01 + G02 + G03 + G04 + 
                                  H01 + H02 + H03 + H04 + J01 + J02 + J05 + L01 + L03 + L04 + 
                                  M01 + M02 + M03 + N01 + N02 + N03 + N05 + N06 + N07 + P01 + 
                                  R01 + R02 + R03 + R05 + R06 + S01 + S02 + S03 + V03 + V04 + 
                                  V06 + Renal + Alb + Plat)


#Selected dataset that you want to create a model for
selectedData <- processedMimicData
f<- mimicDataFeatures

#selectedData <- processedAnticoagulants
#f<- AnticoagulantsFeatures

#selectedData <- processedAntibiotics
#f<- 

#selectedData <- processedDelirium
#f<- 

#selectedData <- processedImmuno
#f<- 

#selectedData <- processedCosticorteroid
#f<- 



# prepare training scheme
seedNumber = 3

columnToPredict = c(24)
n <- names(selectedData)

set.seed(seedNumber)

## test with stratificated 10-fold cross validation 
folds=10
cvIndex <- createFolds(selectedData[,-1]$ADEs ,folds, returnTrain = T)
control <- trainControl(method="cv", index = cvIndex, number=folds, classProbs=TRUE, 
                        summaryFunction = prSummary, #Compare using AUC, precision and recall.
                        savePredictions = TRUE)



####TRAIN CLASSIFICATION MODELS ####

# Random Forest with 100 decision trees
set.seed(seedNumber)
rF100 <- train(f, 
               data=selectedData[,-1], 
               method="rf", ntree=100, 
               trControl=control)


# Random Forest with 500 decision trees
#set.seed(seedNumber)
#rF500 <- train(f, data=selectedData[, -1], method="rf", ntree=500, trControl=control)

#NNetwork
set.seed(seedNumber)
NNetwork <- train(f,
                  data = selectedData[,-1], 
                  method = "nnet", 
                  trControl = control)


# Polynomial Support Vector Machine
set.seed(seedNumber)
SVMPolynomial<- train(f, data = selectedData[,-1], 
                      method= 'svmPoly', 
                      trControl=control)


# Radial Support Vector Machine
set.seed(seedNumber)
SVMRadial<- train(f, data =selectedData[, -1], 
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


#### SAVE FILES WITH NEW TRAINED MODELS ####
## Uncomment to save new trained models.
# save(rF100, file = toString(models.names[1]))
# save(rF500, file = toString(models.names[2]))
# save(SVMPolynomial, file = toString(models.names[5]))
# save(SVMRadial, file = toString(models.names[6]))
# save(NNetwork, file = toString(models.names[8]))


#### COMPARE PERFORMANCE OF MODELS ####
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
