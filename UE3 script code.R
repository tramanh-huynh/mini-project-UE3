library(rlang)
library(Hades)
library(SqlRender)
library(RPostgreSQL)
# install.packages("survival")
# install.packages("rlang", type="source")
# install.packages("dplyr")
# install.packages("lifecycle")
#install.packages("tidyverse")
library(dplyr)
library(survival)
library(stringr)
library(tidyverse)  ##erreur
library(DatabaseConnector)

## Configuration de la connexion à la base de données OMOP

dsn_database = "postgres"  # Specify the name of your Database
# Specify host name e.g.:"aws-us-east-1-portal.4.dblayer.com"
dsn_hostname = "broadsea-atlasdb"  
dsn_port = "5432"                # Specify your port number. e.g. 98939
dsn_uid = "postgres"         # Specify your username. e.g. "admin"
dsn_pwd = "mypass"        # Specify your password. e.g. "xxx"

tryCatch({
  drv <- dbDriver("PostgreSQL")
  print("Connecting to Database…")
  connec <- dbConnect(drv, 
                      dbname = dsn_database,
                      host = dsn_hostname, 
                      port = dsn_port,
                      user = dsn_uid, 
                      password = dsn_pwd)
  print("Database Connected!")
},
error=function(cond) {
  print("Unable to connect to Database.")
})

dbListTables(connec)
#dbGetQuery(connec, "select * from demo_cdm_results.cohort_cache limit 2")

# Execution d'une requête SQL

df <- dbSendQuery(connec, "SELECT count(*) FROM demo_cdm.person;")

# df <- dbSendQuery(connec, "SELECT * FROM demo_cdm.condition_occurrence;")

fetch(df, n=5) 

dbClearResult(dbListResults(connec)[[1]])

# read data
data(pbc)
View(pbcseq)
#help(pbcseq)


##PARTIE 1 : ETL OMOP

## Create person dataframe and transform
person <- data.frame(pbcseq$id, pbcseq$age, pbcseq$sex)
person$gender_concept_id <- ifelse(person$pbcseq.sex == "F", 8532,8507)
person$year_of_birth <- round(1986 - person$pbcseq.age)
person$month_of_birth <- 7
person$day_of_birth <- 1
person$birth_datetime <- ISOdate(person$year_of_birth, person$month_of_birth, 
                                 person$day_of_birth, hour = 0, min = 0, sec = 0)
#person$birth_datetime <- format(person$birth_datetime, format = "%Y-%m-%d %H:%M:%S")
person$person_source_value <- person$pbcseq.id #person_source_value
person$gender_source_value <- pbcseq$sex
person$gender_source_concept_id <- 0 
person <- person %>% distinct()  #remove duplicated lines 
person$person_id <- person$pbcseq.id + 10000 #person_id
person <- subset(person, select = -c(pbcseq.id,pbcseq.age,pbcseq.sex))
person$race_concept_id <- 0
person$ethnicity_concept_id <- 0
View(person)
class(person$birth_datetime)

#insert person to database demo_cdm.person 
for (i in 1:nrow(person)) { 
  row <- person[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.person(
      person_id, gender_concept_id, year_of_birth, 
      month_of_birth, day_of_birth, birth_datetime,
      person_source_value, gender_source_value, gender_source_concept_id,
      race_concept_id, ethnicity_concept_id
    ) values (
      ${person_id}, ${gender_concept_id}, ${year_of_birth}, 
      ${month_of_birth}, ${day_of_birth}, '${birth_datetime}', 
      ${person_source_value}, '${gender_source_value}', ${gender_source_concept_id},
      ${race_concept_id}, ${ethnicity_concept_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}
dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.person where person_id >= 10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.person where person_id = 10001;")




### Create observation_period dataframe and transform
observation_period <- data.frame(pbcseq$id, pbcseq$futime)
observation_period$person_id <- observation_period$pbcseq.id + 10000
observation_period$observation_period_start_date <- as.Date("1974-01-01") 
observation_period$observation_period_end_date <- as.Date("1974-01-01") + observation_period$pbcseq.futime
observation_period$period_type_concept_id <- 32878
observation_period <- observation_period %>% distinct() 
observation_period <- subset(observation_period,select = -c(pbcseq.id,pbcseq.futime))
observation_period$observation_period_id <- observation_period$person_id
View(observation_period)
class(observation_period$observation_period_start_date)



#insert observation_period to database demo_cdm.observation_period 
for (i in 1:nrow(observation_period)) { 
  row <- observation_period[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.observation_period(
      observation_period_id, person_id, observation_period_start_date, observation_period_end_date, 
      period_type_concept_id
    ) values (
      ${observation_period_id},${person_id}, '${observation_period_start_date}', '${observation_period_end_date}', 
      ${period_type_concept_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.observation_period where observation_period_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.observation_period where observation_period_id = 10001;")




#create visit_occurence dataframe and fransform
visit_occurrence <- data.frame(pbcseq$id,pbcseq$day)
visit_occurrence$person_id <- visit_occurrence$pbcseq.id + 10000
visit_occurrence$visit_concept_id <- c(32036)
visit_occurrence$visit_start_date <- as.Date("1974-01-01") + visit_occurrence$pbcseq.day
visit_occurrence$visit_start_datetime <- format(visit_occurrence$visit_start_date, format = "%Y-%m-%d %H:%M:%S")
visit_occurrence$visit_end_date <- visit_occurrence$visit_start_date
visit_occurrence$visit_end_datetime <- format(visit_occurrence$visit_end_date, format = "%Y-%m-%d %H:%M:%S")
visit_occurrence$visit_type_concept_id <- c(32856)
visit_occurrence$admitting_source_concept_id  <- c(38004207) 
visit_occurrence <- visit_occurrence %>% distinct() 
visit_occurrence <- subset(visit_occurrence, select = -c(pbcseq.id,pbcseq.day))
visit_occurrence$visit_occurrence_id <- seq(10001,11945) 
View(visit_occurrence)


#insert visit_occurrence to database demo_cdm.visit_occurrence 
for (i in 1:nrow(visit_occurrence)) { 
  row <- visit_occurrence[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.visit_occurrence(
      visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime, visit_end_date,
      visit_end_datetime, visit_type_concept_id, admitting_source_concept_id
    ) values (
      ${visit_occurrence_id},${person_id}, ${visit_concept_id}, '${visit_start_date}', '${visit_start_datetime}',
      '${visit_end_date}','${visit_end_datetime}',${visit_type_concept_id}, ${admitting_source_concept_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.visit_occurrence where visit_occurrence_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.visit_occurrence where visit_occurrence_id =10001;")




#Create condition_occurrence dataframe and transform
condition_occurrence_ascites <- data.frame(pbcseq$id,pbcseq$ascites,pbcseq$day)
condition_occurrence_ascites$person_id <- condition_occurrence_ascites$pbcseq.id + 10000
condition_occurrence_ascites$condition_concept_id <- ifelse(condition_occurrence_ascites$pbcseq.ascites == "1",200528,3655191)
condition_occurrence_ascites$condition_start_date <- as.Date("1974-01-01") + pbcseq$day
condition_occurrence_ascites$condition_start_datetime <- format(condition_occurrence_ascites$condition_start_date, format = "%Y-%m-%d %H:%M:%S")
condition_occurrence_ascites$condition_type_concept_id <- c(32856)
condition_occurrence_ascites$condition_source_value <- condition_occurrence_ascites$pbcseq.ascites
condition_occurrence_ascites$visit_occurrence_id <- seq(10001,11945)
condition_occurrence_ascites <- subset(condition_occurrence_ascites, select = -c(pbcseq.id,pbcseq.day,pbcseq.ascites))
condition_occurrence_ascites <-na.omit(condition_occurrence_ascites)
count(condition_occurrence_ascites)
#View(condition_occurrence_ascites)

condition_occurrence_hepato <- data.frame(pbcseq$id, pbcseq$hepato, pbcseq$day)
condition_occurrence_hepato$person_id <- condition_occurrence_hepato$pbcseq.id + 10000
condition_occurrence_hepato$condition_concept_id <- ifelse(condition_occurrence_hepato$pbcseq.hepato == "1",197676,45911628)
condition_occurrence_hepato$condition_start_date <- as.Date("1974-01-01") + pbcseq$day
condition_occurrence_hepato$condition_start_datetime <- format(condition_occurrence_hepato$condition_start_date, format = "%Y-%m-%d %H:%M:%S")
condition_occurrence_hepato$condition_source_value <- condition_occurrence_hepato$pbcseq.hepato
condition_occurrence_hepato$condition_type_concept_id <- c(32856)
condition_occurrence_hepato$visit_occurrence_id <- seq(10001,11945)
condition_occurrence_hepato <- subset(condition_occurrence_hepato, select = -c(pbcseq.id,pbcseq.day,pbcseq.hepato))
condition_occurrence_hepato <- na.omit(condition_occurrence_hepato)
count(condition_occurrence_hepato)
#View(condition_occurrence_hepato) 

condition_occurrence_edema <- data.frame(pbcseq$id, pbcseq$edema, pbcseq$day)
condition_occurrence_edema$person_id <- condition_occurrence_edema$pbcseq.id + 10000
condition_occurrence_edema$condition_concept_id <- ifelse(condition_occurrence_edema$pbcseq.edema == "0",4059917,433595)
condition_occurrence_edema$condition_start_date <- as.Date("1974-01-01") + pbcseq$day
condition_occurrence_edema$condition_start_datetime <- format(condition_occurrence_edema$condition_start_date, format = "%Y-%m-%d %H:%M:%S")
condition_occurrence_edema$condition_source_value <- condition_occurrence_edema$pbcseq.edema
condition_occurrence_edema$condition_type_concept_id <- c(32856)
condition_occurrence_edema$visit_occurrence_id <- seq(10001,11945)
condition_occurrence_edema <- subset(condition_occurrence_edema, select = -c(pbcseq.id,pbcseq.day,pbcseq.edema))
condition_occurrence_edema <- na.omit(condition_occurrence_edema)
count(condition_occurrence_edema)
#View(condition_occurrence_edema)

condition_occurrence_spiders <- data.frame(pbcseq$id, pbcseq$spiders, pbcseq$day)
condition_occurrence_spiders$person_id <- condition_occurrence_spiders$pbcseq.id + 10000 #no spider nevus
condition_occurrence_spiders$condition_concept_id <- ifelse(condition_occurrence_spiders$pbcseq.spiders == "1",4111857,45884341)
condition_occurrence_spiders$condition_start_date <- as.Date("1974-01-01") + pbcseq$day
condition_occurrence_spiders$condition_start_datetime <- format(condition_occurrence_spiders$condition_start_date, format = "%Y-%m-%d %H:%M:%S")
condition_occurrence_spiders$condition_source_value <- condition_occurrence_spiders$pbcseq.spiders
condition_occurrence_spiders$condition_type_concept_id <- c(32856)
condition_occurrence_spiders$visit_occurrence_id <- seq(10001,11945)
condition_occurrence_spiders <- subset(condition_occurrence_spiders, select = -c(pbcseq.id,pbcseq.day,pbcseq.spiders))
condition_occurrence_spiders <- na.omit(condition_occurrence_spiders)
count(condition_occurrence_spiders)
#View(condition_occurrence_spiders) 

condition_occurrence <- rbind(condition_occurrence_ascites, condition_occurrence_edema, 
                              condition_occurrence_hepato, condition_occurrence_spiders)
condition_occurrence <- na.omit(condition_occurrence)
count(condition_occurrence)
condition_occurrence$condition_occurrence_id <- seq(10001,17601)
condition_occurrence <- data.frame(condition_occurrence)
view(condition_occurrence)

#insert visit_occurrence to database demo_cdm.condition_occurrence 
for (i in 1:nrow(condition_occurrence)) { 
  row <- condition_occurrence[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.condition_occurrence(
      condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, 
      condition_type_concept_id, condition_source_value, visit_occurrence_id
    ) values (
      ${condition_occurrence_id},${person_id}, ${condition_concept_id}, '${condition_start_date}', '${condition_start_datetime}',
      ${condition_type_concept_id},${condition_source_value},${visit_occurrence_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.condition_occurrence where condition_occurrence_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.condition_occurrence where condition_occurrence_id =10001;")


#create drug_exposure table dataframe and transform
drug_exposure <- data.frame(pbcseq$id,pbcseq$futime,pbcseq$trt)
drug_exposure$person_id <- drug_exposure$pbcseq.id + 10000
drug_exposure$drug_concept_id <- ifelse(drug_exposure$pbcseq.trt == "0", 19047135,44520876) 
drug_exposure$drug_exposure_start_date <- as.Date("1974-01-01")
drug_exposure$drug_exposure_start_datetime <- format(drug_exposure$drug_exposure_start_date, format = "%Y-%m-%d %H:%M:%S")
drug_exposure$drug_exposure_end_date <- as.Date("1974-01-01") + drug_exposure$pbcseq.futime
drug_exposure$drug_exposure_end_datetime <- format(drug_exposure$drug_exposure_end_date, format = "%Y-%m-%d %H:%M:%S")
#drug_exposure$verbatim_end_date <- as.Date("1986-07-01") not given
drug_exposure$drug_type_concept_id<-c(32877) 
drug_exposure <- drug_exposure %>% distinct() 
drug_exposure <- subset(drug_exposure, select = -c(pbcseq.id,pbcseq.futime,pbcseq.trt))
drug_exposure$drug_exposure_id <- drug_exposure$person_id
View(drug_exposure)

#insert drug_exposure to database demo_cdm.drug_exposure 
for (i in 1:nrow(drug_exposure)) { 
  row <- drug_exposure[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.drug_exposure(
      drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, 
      drug_exposure_end_date, drug_exposure_end_datetime, drug_type_concept_id
    ) values (
      ${drug_exposure_id},${person_id}, ${drug_concept_id}, '${drug_exposure_start_date}', '${drug_exposure_start_datetime}',
      '${drug_exposure_end_date}','${drug_exposure_end_datetime}',${drug_type_concept_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.drug_exposure where drug_exposure_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.drug_exposure where drug_exposure_id =10001;")





#Create measurement dataframe and transform
measurement_bili <- data.frame(pbcseq$id , pbcseq$bili, pbcseq$day)
measurement_bili$person_id <- measurement_bili$pbcseq.id + 10000
measurement_bili$measurement_concept_id <- 4041529
measurement_bili$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_bili$measurement_datetime <- format(measurement_bili$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_bili$operator_concept_id <- 4172703
measurement_bili$value_as_number <- measurement_bili$pbcseq.bili
measurement_bili$unit_concept_id <- 8840
measurement_bili$unit_source_value <- "mg/dl"
measurement_bili$value_source_value <- measurement_bili$pbcseq.bili
measurement_bili$visit_occurrence_id <- seq(10001,11945)
measurement_bili <- subset(measurement_bili, select = -c(pbcseq.id,pbcseq.bili,pbcseq.day))
measurement_bili <- na.omit(measurement_bili)
#View(measurement_bili)

measurement_albumin <- data.frame(pbcseq$id , pbcseq$albumin, pbcseq$day)
measurement_albumin$person_id <- measurement_albumin$pbcseq.id + 10000
measurement_albumin$measurement_concept_id <- 4097664
measurement_albumin$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_albumin$measurement_datetime <- format(measurement_albumin$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_albumin$operator_concept_id <- 4172703
measurement_albumin$value_as_number <- measurement_albumin$pbcseq.albumin
measurement_albumin$unit_concept_id <- 8840
measurement_albumin$unit_source_value <- "mg/dl"
measurement_albumin$value_source_value <- measurement_albumin$pbcseq.albumin
measurement_albumin$visit_occurrence_id <- seq(10001,11945)
measurement_albumin <- subset(measurement_albumin, select = -c(pbcseq.id,pbcseq.albumin,pbcseq.day))
measurement_albumin <- na.omit(measurement_albumin)
#View(measurement_albumin)

measurement_alk.phos <- data.frame(pbcseq$id , pbcseq$alk.phos,pbcseq$day)
measurement_alk.phos$person_id <- measurement_alk.phos$pbcseq.id + 10000
measurement_alk.phos$measurement_concept_id <- 4230636
measurement_alk.phos$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_alk.phos$measurement_datetime <- format(measurement_alk.phos$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_alk.phos$operator_concept_id <- 4172703
measurement_alk.phos$value_as_number <- measurement_alk.phos$pbcseq.alk.phos
measurement_alk.phos$unit_concept_id <- 8645
measurement_alk.phos$unit_source_value <- "U/liter"
measurement_alk.phos$value_source_value <- measurement_alk.phos$pbcseq.alk.phos
measurement_alk.phos$visit_occurrence_id <- seq(10001,11945)
measurement_alk.phos <- subset(measurement_alk.phos, select = -c(pbcseq.id,pbcseq.alk.phos, pbcseq.day))
measurement_alk.phos <- na.omit(measurement_alk.phos)
#View(measurement_alk.phos)

measurement_ast <- data.frame(pbcseq$id , pbcseq$ast,pbcseq$day)
measurement_ast$person_id <- measurement_ast$pbcseq.id + 10000
measurement_ast$measurement_concept_id <- 4263457
measurement_ast$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_ast$measurement_datetime <- format(measurement_ast$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_ast$operator_concept_id <- 4172703
measurement_ast$value_as_number <- measurement_ast$pbcseq.ast
measurement_ast$unit_concept_id <- 8763
measurement_ast$unit_source_value <- "U/ml"
measurement_ast$value_source_value <- measurement_ast$pbcseq.ast
measurement_ast$visit_occurrence_id <- seq(10001,11945)
measurement_ast <- subset(measurement_ast, select = -c(pbcseq.id,pbcseq.ast,pbcseq.day))
measurement_ast <- na.omit(measurement_ast) # 60 values NA
#View(measurement_ast)

measurement_chol <- data.frame(pbcseq$id , pbcseq$chol,pbcseq$day)
measurement_chol$person_id <- measurement_chol$pbcseq.id + 10000
measurement_chol$measurement_concept_id <- 4299360
measurement_chol$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_chol$measurement_datetime <- format(measurement_chol$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_chol$operator_concept_id <- 4172703
measurement_chol$value_as_number <- measurement_chol$pbcseq.chol
measurement_chol$unit_concept_id <- 8840
measurement_chol$unit_source_value <- "mg/ml"
measurement_chol$value_source_value <- measurement_chol$pbcseq.chol
measurement_chol$visit_occurrence_id <- seq(10001,11945)
measurement_chol <- subset(measurement_chol, select = -c(pbcseq.id,pbcseq.chol,pbcseq.day))
measurement_chol <- na.omit(measurement_chol) #821 values NA
#View(measurement_chol)

measurement_platelet <- data.frame(pbcseq$id , pbcseq$platelet, pbcseq$day)
measurement_platelet$person_id <- measurement_platelet$pbcseq.id + 10000
measurement_platelet$measurement_concept_id <- 4267147
measurement_platelet$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_platelet$measurement_datetime <- format(measurement_platelet$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_platelet$operator_concept_id <- 4172703
measurement_platelet$value_as_number <- measurement_platelet$pbcseq.platelet
measurement_platelet$unit_concept_id <- 0 #ne peut pas trouver unit_concept_id
measurement_platelet$unit_source_value <- "ml/1000"
measurement_platelet$value_source_value <- measurement_platelet$pbcseq.platelet
measurement_platelet$visit_occurrence_id <- seq(10001,11945) 
measurement_platelet <- subset(measurement_platelet, select = -c(pbcseq.id,pbcseq.platelet, pbcseq.day))
measurement_platelet <- na.omit(measurement_platelet) #73 values  NA
#View(measurement_platelet)

measurement_protime <- data.frame(pbcseq$id , pbcseq$protime, pbcseq$day)
measurement_protime$person_id <- measurement_protime$pbcseq.id + 10000
measurement_protime$measurement_concept_id <- 4245261
measurement_protime$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_protime$measurement_datetime <- format(measurement_protime$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_protime$operator_concept_id <- 4172703
measurement_protime$value_as_number <- measurement_protime$pbcseq.protime
measurement_protime$unit_concept_id <- 8555
measurement_protime$unit_source_value <- "seconds"
measurement_protime$value_source_value <- measurement_protime$pbcseq.protime
measurement_protime$visit_occurrence_id <- seq(10001,11945)
measurement_protime <- subset(measurement_protime, select = -c(pbcseq.id,pbcseq.protime, pbcseq.day))
measurement_protime <- na.omit(measurement_protime)
#View(measurement_protime)

measurement_stage <- data.frame(pbcseq$id , pbcseq$stage, pbcseq$day)
measurement_stage$person_id <- measurement_stage$pbcseq.id + 10000
measurement_stage$measurement_concept_id <- 37018329
measurement_stage$measurement_date <- as.Date("1974-01-01") + pbcseq$day
measurement_stage$measurement_datetime <- format(measurement_stage$measurement_date, format = "%Y-%m-%d %H:%M:%S")
measurement_stage$operator_concept_id <- 4172703
measurement_stage$value_as_number <- measurement_stage$pbcseq.stage
measurement_stage$unit_concept_id <- 4106767  #int
measurement_stage$unit_source_value <- "stage"
measurement_stage$value_source_value <- measurement_stage$pbcseq.stage
measurement_stage$visit_occurrence_id <- seq(10001,11945)
measurement_stage <- subset(measurement_stage, select = -c(pbcseq.id,pbcseq.stage, pbcseq.day))
measurement_stage <- na.omit(measurement_stage)
#View(measurement_stage)

measurement <- rbind(measurement_albumin, measurement_alk.phos, measurement_ast, measurement_platelet,
                     measurement_bili, measurement_chol, measurement_protime, measurement_stage)
measurement$measurement_type_concept_id <- 32809
count(measurement)
measurement$measurement_id <- seq(10001,24606)
#view(measurement)

#insert measurement to database demo_cdm.measurement
for (i in 1:nrow(measurement)) { 
  row <- measurement[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.measurement(
      measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime, measurement_type_concept_id,
      operator_concept_id, value_as_number, unit_concept_id, unit_source_value, value_source_value,
      visit_occurrence_id
    ) values (
      ${measurement_id},${person_id}, ${measurement_concept_id}, '${measurement_date}', '${measurement_datetime}', ${measurement_type_concept_id},
      ${operator_concept_id},${value_as_number}, ${unit_concept_id}, '${unit_source_value}', ${value_source_value},
      ${visit_occurrence_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.measurement where measurement_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.measurement where measurement_id =10001;")




#Create observation dataframe and transform
observation <- data.frame(pbcseq$id,pbcseq$status, pbcseq$futime)
observation$person_id <- observation$pbcseq.id + 10000
observation$observation_concept_id <- ifelse(observation$pbcseq.status == "2",4306655,4263886)
observation$observation_date <- as.Date("1974-01-01") + observation$pbcseq.futime
observation$observation_datetime <- format(observation$observation_date, format = "%Y-%m-%d %H:%M:%S")
observation$observation_type_concept_id <- 32809
observation$value_as_number <- observation$pbcseq.status
observation$observation_source_value <- observation$pbcseq.status
observation <- observation %>% distinct()
observation$observation_id <- observation$pbcseq.id + 10000
observation <- subset(observation, select = -c(pbcseq.id,pbcseq.status, pbcseq.futime))
observation <- na.omit(observation)
#View(observation)


#insert observation to database demo_cdm.observation
for (i in 1:nrow(observation)) { 
  row <- observation[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.observation(
      observation_id, person_id, observation_concept_id, observation_date, observation_datetime, 
      observation_type_concept_id, value_as_number, observation_source_value
    ) values (
      ${observation_id},${person_id}, ${observation_concept_id}, '${observation_date}', '${observation_datetime}', 
      ${observation_type_concept_id}, ${value_as_number}, ${observation_source_value}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.observation where observation_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.observation where observation_id =10001;")




#Create death dataframe and transform
death_vivant <- data.frame(pbcseq$id,pbcseq$status, pbcseq$futime)
death <- death_vivant %>% filter(pbcseq.status == 2)
death$person_id <- death$pbcseq.id + 10000
death$death_date <- as.Date("1974-01-01") + death$pbcseq.futime
death$death_datetime <- format(death$death_date, format = "%Y-%m-%d %H:%M:%S")
death$death_type_concept_id <- c(32815) #death certificate 
death <- death %>% distinct()
death <- subset(death, select = -c(pbcseq.id, pbcseq.status, pbcseq.futime))
#view(death)



#insert death to database demo_cdm.death
for (i in 1:nrow(death)) { 
  row <- death[i, ]
  insert_query <- str_interp(
    "insert into demo_cdm.death(
      person_id, death_date, death_datetime, death_type_concept_id
    ) values (
      ${person_id}, '${death_date}', '${death_datetime}', ${death_type_concept_id}
    );"
    , row
  )
  
  dbGetQuery(connec, insert_query)
}

dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.death where person_id >=10001;")
dbGetQuery(connec, "SELECT * FROM demo_cdm.death where person_id =10006;")

#delete data 
#dbExecute(connec, "delete from demo_cdm.observation where observation_id >= 10001")


###PARTIE 2 : 

#Count number of patients participate this cohort : #312
dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.person where person_id >=  10001")

#Count number of admissions of patients : #1945
dbGetQuery(connec, "SELECT count(visit_occurrence_id) as number_admissions FROM demo_cdm.visit_occurrence where visit_occurrence_id > 10000")

#Count number of death patients at the end of this cohort : #140
dbGetQuery(connec, "SELECT count(*) FROM demo_cdm.death where person_id>= 10001")

#Count number of admissions of death patients : #725
dbGetQuery(connec, "SELECT count(visit_occurrence_id) FROM demo_cdm.visit_occurrence as visit, 
                    demo_cdm.death as death where visit.person_id = death.person_id and visit_occurrence_id > 10000")

#Count number of visit per every death patient : 
dbGetQuery(connec, "SELECT count(visit_occurrence_id) as count_visit, visit.person_id FROM demo_cdm.visit_occurrence as visit, 
                    demo_cdm.death as death where visit.person_id = death.person_id and visit_occurrence_id > 10000
                    group by visit.person_id order by count_visit")

#Count number of follow up time of death patients :
dbGetQuery(connec, "SELECT person_id, death_date - '1974-01-01' as days FROM demo_cdm.death where person_id > 10000")

# drug per death patients
dbGetQuery(connec, 
           "SELECT drug.person_id, drug.drug_concept_id
  FROM demo_cdm.drug_exposure as drug, demo_cdm.death as death 
  where drug.person_id = death.person_id and death.person_id > 10000"
)

# count number death patients per drug
dbGetQuery(connec, 
           "SELECT count(drug.person_id), drug.drug_concept_id
  FROM demo_cdm.drug_exposure as drug, demo_cdm.death as death 
  where drug.person_id = death.person_id and death.person_id > 10000
  group by drug.drug_concept_id"
)

# symptom per death patients
dbGetQuery(connec, 
           "SELECT condition.person_id, condition.condition_concept_id
  FROM demo_cdm.condition_occurrence as condition, demo_cdm.death as death 
  where condition.person_id = death.person_id and death.person_id > 10000
  group by condition.person_id, condition.condition_concept_id
  order by person_id"
)

# number of symptoms of death patient 10003 each visit
dbGetQuery(connec, 
           "SELECT person_id, visit_occurrence_id, condition_concept_id
  FROM 
    (SELECT condition.person_id , condition.condition_concept_id, condition.visit_occurrence_id
    FROM demo_cdm.condition_occurrence as condition, demo_cdm.death as death 
    where condition.person_id = death.person_id and death.person_id = 10003
    group by condition.person_id, condition.condition_concept_id, condition.visit_occurrence_id
    order by person_id) as symptom
  order by person_id, visit_occurrence_id"
)

# number of dead patients per symptom
dbGetQuery(connec, 
           "SELECT condition.condition_concept_id, count(distinct(condition.person_id))
  FROM demo_cdm.condition_occurrence as condition, demo_cdm.death as death 
  where condition.person_id = death.person_id and death.person_id > 10000
  group by condition.condition_concept_id"
)

# number of measurements per dead patients
dbGetQuery(connec, 
           "SELECT measurement.measurement_concept_id, count(distinct(death.person_id))
  FROM demo_cdm.measurement as measurement, demo_cdm.death as death 
  where measurement.person_id = death.person_id and death.person_id > 10000
  group by measurement.measurement_concept_id"
)

# measurement of death patient 10003 
dbGetQuery(connec, 
           "SELECT death.person_id, ms.visit_occurrence_id, ms.measurement_concept_id
  FROM demo_cdm.measurement as ms, demo_cdm.death as death 
  where ms.person_id = death.person_id and death.person_id = 10003
  order by ms.visit_occurrence_id
  "
)

# 4299360
dbGetQuery(connec, 
           "SELECT distinct(death.person_id)
  FROM demo_cdm.measurement as ms, demo_cdm.death as death 
  where ms.person_id not in (select person_id from demo_cdm.measurement
                            where measurement_concept_id = 4299360)
    and ms.person_id = death.person_id
    and death.person_id > 10000
  order by death.person_id"
)


###############################################################################
# Insertion de nouvelles données dans la table person

#q <- "INSERT INTO 
#demo_cdm.person(person_id, 
#gender_concept_id, 
#year_of_birth, 
#race_concept_id,
#ethnicity_concept_id
#) 
#values (10000,8532,1999, 0,0);"


#dbGetQuery(connec, q)

# q <- "SELECT * FROM demo_cdm.person WHERE person_id = 10000;"

# dbGetQuery(connec,q)


#creat databaseConnector
#serverName <- str_interp("${dsn_hostname}/${dsn_database}")
#connec2 <- connect(createConnectionDetails(dbms = "postgresql", server = serverName, 
#                                   user = dsn_uid, password = dsn_pwd, port = dsn_port))

#dbGetQuery(connec, "select * from webapi.source")
# dbGetQuery(connec, "select * from demo_cdm.person")
#dbGetQuery(connec, "select * from webapi.source_daimon")
#dbGetQuery(connec, "update webapi.source set is_cache_enabled = FALSE where source_id = 1")
# dbExecute(connec, "truncate table demo_cdm_results.cohort_cache")


# install.packages("remotes")
# remotes::install_github("OHDSI/CdmAtlasCutover")

#library(CdmAtlasCutover)

# refreshAtlasSources("http://127.0.0.1:80/WebAPI")

#test insertTable
#iiii <- insertTable(connec2, "demo_cdm", "person", person, createTable = FALSE, dropTableIfExists = FALSE)
#disconnect(connec2)

#connec3 <- src_postgres(dbname = dsn_database, host = dsn_hostname, 
#                         port = dsn_port, user = dsn_uid, password = dsn_pwd)$con

#resultTest <- dbWriteTable(connec3, name = "demo_cdm.person", value = person, row.names=FALSE, append = TRUE)
#resultTest <- dbGetException(connec3)
#resultTest
#dbDisconnect(connec3)

