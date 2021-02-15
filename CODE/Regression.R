
library("ISLR")
library('tidyverse')


#read data
data<-read.csv("Final_Clean_data.csv")

#read all zipcodes saves in file
zipcodes<-read.csv("zipcode.csv")

#loop over each zip code and fit linear regression for merchants closing 
for (i in 1:157){
  #filter each zip code
  data1<-data[data['zip']==zipcodes[i,1],]
  #filter Violent crime and covid data
  data1<-data1[c(5,40:43)]
  #fit linear regression model
  m<-lm(merchants_all~.,data1)
  #keep only coefficients where p<-0.1
  a<-summary(m)$coef[summary(m)$coef[,4] <= .1, 4]
  #write these coefficients in result dataframe
  if (!is.na(a) && length(a)>0) {
    for (j in 1:length(a)){
      result[i,names(a[j])]<-a[j]
      
    }}
  result[i,'merchant_closing']<-mean(data1$merchants_all)
  
}
# write results in csv
write.csv(result, file='SMB.csv')





#loop over each zip code and fit linear regression for violent crime
for (i in 1:157){
  #filter each zip code
  data1<-data[data['zip']==zipcodes[i,1],]
  #filter Violent crime and covid data
  data1<-data1[c(41:43)]
  #fit linear regression model
  m<-lm(Violent~.,data1)
  #keep only coefficients where p<-0.1
  a<-summary(m)$coef[summary(m)$coef[,4] <= .1, 4]
  #write these coefficients in result dataframe
  if (!is.na(a) && length(a)>0) {
  for (j in 1:length(a)){
    result[i,names(a[j])]<-a[j]
    
  }}

}
# write results in csv
write.csv(result, file='violent.csv')


#loop over each zip code and fit linear regression for Non-violent crime

for (i in 1:157){
  #filter each zip code
  data1<-data[data['zip']==zipcodes[i,1],]
  #filter Violent crime and covid data
  data1<-data1[c(40,42:43)]
  #fit linear regression model
  m<-lm(Non.Violent~.,data1)
  #keep only coefficients where p<-0.1
  a<-summary(m)$coef[summary(m)$coef[,4] <= .1, 4]
  #write these coefficients in result dataframe
  if (!is.na(a) && length(a)>0) {
    for (j in 1:length(a)){
      result[i,names(a[j])]<-a[j]
      
    }}
  
}
# write results in csv
write.csv(result, file='Nonviolent.csv')









