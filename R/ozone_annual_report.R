## produce le elaborazioni annuali sui dati di ozono

## Operazioni:
## 1) estrazione
## 2) calcolo indicatori e loro validita'
## 3) identificazione eventi
## 4) scrittura su DB

prepare.ozone_annual_report <- function(con,
                                       id.staz,
                                       year=NULL,
                                       ...){
  ## 0) operazioni preliminari
  if(is.null(year)) {
    year <- format(as.POSIXct(paste(Sys.Date(),"00:00"), tz="Africa/Algiers")-60*60*24*30*5,"%Y") # l'anno di 5 mesi fa
  }
  f.date <- as.POSIXct(paste(year,"-12-31 23:59",sep=""), tz="Africa/Algiers") # fine anno
  i.date <- as.POSIXct(paste(Year(f.date)-1,"-12-31 16:00",sep=""), tz="Africa/Algiers") # margine per media mobile
  
  
  ## 1) estrazione
  if((as.POSIXct(Sys.time()) - i.date) > 365*2) {
    tab <- "storico"
  } else {
    tab <- "recente"
  }
  Dat <- dbqa.get.datastaz(con=con,ts.range=c(i.date,f.date),
                           tstep="H",
                           id.staz=id.staz,
                           id.param=7,
                           #flg=c(0,1),
                           flg=1,
                           bulk_read=28,  # valore ottimale
                           table=tab,
                           ...)
  
  out <- list(Dat=Dat, id.staz=id.staz)
  return(out)
}

calculate.ozone_annual_report <- function(data){
  
  Dat <- data$Dat
  
  if(!is.null(Dat)){
    ## 2) calcolo indicatori e loro validita'
    Time <- index(Dat)
    day <- Ymd(Time)
    yDat <- last(Dat,'1 year') # solo dati ultimo anno
    yTime <- index(yDat)
    yday <- Ymd(yTime)
    yDatR <- dbqa.round(as.vector(yDat),id.param=7) # solo dati ultimo anno, arrotondati (ma dovrebbero esserlo gia'...)
    
    ## numero di giorni e ore nell'anno
    ndays  <- Ndays.in.year(Year(yTime[1]))
    nhours <- ndays*24

    # media annua
    annual.mean      <- dbqa.round(mean(yDat, na.rm=T),id.param=7)
    # - max media mobile 8h
    ave.8h <- mean_window(x=as.vector(Dat),k=8,necess=6)
    max.ave.8h <- stat.period(x=ave.8h,period=day,necess=18,FUN=max)[-1]
    ## - no. sup. orari soglia 180 (valori arrotondati)
    cumul.nexc.180 <- sum(as.numeric(yDatR>180), na.rm=T)
    ## - no. sup. orari soglia 240 (valori arrotondati)
    cumul.nexc.240 <- sum(as.numeric(yDatR>240), na.rm=T)
    ## no. di dati orari validi
    annual.nValid     <- sum(as.numeric(!is.na(yDatR)))
    annual.nExpected  <- nhours/24*23
    annual.efficiency <- round_awayfromzero(annual.nValid/annual.nExpected*100)
    ## - no. sup. giorn. soglia 120
    cumul.nexc.120 <- sum(as.numeric(dbqa.round(max.ave.8h,id.param=7)>120), na.rm=T)
    
    ## - AOT40 annuale vegetazione
    vegetIdx <- as.numeric(format(Time, format="%m")) %in% 5:7
    vegetDat <- Dat[vegetIdx]
    vegetTime <- Time[vegetIdx]
    vegetHour <- as.numeric(format(vegetTime, format="%H"))
    dum <- aot(vegetDat, vegetHour, threshold=80, estimate=T, hr.min=8, hr.max=19)
    aot40.veget <- dbqa.round(dum$Aot,id.param=7)
    aot40.veget.PercValid <- dum$PercValid
    aot40.veget.NhValid <- dum$NhValid
    ## - AOT40 annuale foreste
    forestIdx <- as.numeric(format(Time, format="%m")) %in% 4:9
    forestDat <- Dat[forestIdx]
    forestTime <- Time[forestIdx]
    forestHour <- as.numeric(format(forestTime, format="%H"))
    dum <- aot(forestDat, forestHour, threshold=80, estimate=T, hr.min=8, hr.max=19)
    aot40.forest <- dbqa.round(dum$Aot,id.param=7)
    aot40.forest.PercValid <- dum$PercValid
    aot40.forest.NhValid <- dum$NhValid
    
    ## conta dati validi nella fascia oraria di interesse
    ## per il periodo aprile-settembre
    hr.min=8
    hr.max=19
    mo.necess=4:9
    yTime  <- index(yDat)
    yValid <- !is.na(yDat)
    yHr <- Hour(yTime)
    yMo <- Month(yTime)
    in.hr <- yHr>=hr.min & yHr<=hr.max
    yValid0820 <- yValid & in.hr
    mValid0820 <- tapply(X=yValid0820, INDEX=yMo, FUN=sum)
    ## calcola efficienza mensile per la fascia oraria di interesse
    mExpected0820  <- tapply(X=in.hr,  INDEX=yMo, FUN=sum)
    mEfficiency0820<- round_awayfromzero(mValid0820/mExpected0820*100)
    ## conta quanti mesi estivi soddisfacenti ci sono
    validMonths0820<- mEfficiency0820>=90 & unique(yMo) %in% mo.necess
    nValidMonths0820<- sum(as.numeric(validMonths0820), na.rm=T)
    
    ## conta giorni validi
    ## per il periodo aprile-settembre
    dValid <- !is.na(max.ave.8h)
    dDay <- unique(day)[-1]
    yyyymm <- substr(dDay,1,6)
    mValid     <- tapply(X=dValid, INDEX=yyyymm, FUN=sum)
    ## calcola efficienza mensile su base giornaliera
    mExpected  <- tapply(X=dValid, INDEX=yyyymm, FUN=length)
    mEfficiency<- round_awayfromzero(mValid/mExpected*100)
    ## conta quanti mesi estivi soddisfacenti ci sono
    mm <- as.numeric(substr(unique(yyyymm),5,6))
    validMonths<- mEfficiency>=90 & mm %in% mo.necess
    nValidMonths<- sum(as.numeric(validMonths), na.rm=T)
    
    annual.report <- data.frame(cumul.nexc.180=cumul.nexc.180,
                                cumul.nexc.240=cumul.nexc.240,
                                cumul.nexc.120=cumul.nexc.120,
                                annual.mean      =annual.mean,
                                annual.nValid    =annual.nValid,     
                                annual.nExpected =annual.nExpected,
                                annual.efficiency=annual.efficiency,
                                aot40.veget=           aot40.veget,
                                aot40.veget.PercValid= aot40.veget.PercValid,
                                aot40.veget.NhValid=   aot40.veget.NhValid,
                                aot40.forest=          aot40.forest,
                                aot40.forest.PercValid=aot40.forest.PercValid,
                                aot40.forest.NhValid= aot40.forest.NhValid,
                                nValidMonths=nValidMonths,
                                nValidMonths0820=nValidMonths0820)
    
  } else {
    annual.report <- data.frame(cumul.nexc.180=NA,
                                cumul.nexc.240=NA,
                                cumul.nexc.120=NA,
                                annual.mean      =NA,
                                annual.nValid    =NA,     
                                annual.nExpected =NA,
                                annual.efficiency=NA,
                                aot40.veget=           NA,
                                aot40.veget.PercValid= NA,
                                aot40.veget.NhValid= NA,
                                aot40.forest=          NA,
                                aot40.forest.PercValid=NA,
                                aot40.forest.NhValid=NA,
                                nValidMonths=NA,
                                nValidMonths0820=NA)
  }
  
  ## 3) identificazione eventi
  if(!is.null(Dat)){
    Ev180 <- detect.event(yDatR,180)
    if(is.data.frame(Ev180)) {
      Time180 <- index(yDat)[Ev180$index]
      Max180 <- Ev180$max
      events.180 <- data.frame(start.time=Time180,
                               duration=Ev180$duration,
                               max=Max180)
    } else {
      events.180 <- NULL
    }
    Ev240 <- detect.event(yDatR,240)
    if(is.data.frame(Ev240)) {
      Time240 <- index(yDat)[Ev240$index]
      Max240 <- Ev240$max
      events.240 <- data.frame(start.time=Time240,
                               duration=Ev240$duration,
                               max=Max240)      
    } else {
      events.240 <- NULL
    }
    events <- list(exc.180=events.180,
                   exc.240=events.240)
  } else {
    events <- NULL
  }
  
  Out <- list(annual.report=annual.report,
              events=events,
              id.staz=data$id.staz,
              first.time=yTime[1],
              last.time=yTime[length(yTime)])
  return(Out)
}

write.ozone_annual_report <- function(con,
                                     OAR,
                                     verbose=F,
                                     ...) {
  
  id.param = 7 # Ozono
  
  ## elimina record del GIORNO-CONFIG_STAZ-PARAMETRO
  dbqa.delete(con=con, tab="WEB_STAT",
              keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                     "ID_CONFIG_STAZ",
                     "ID_PARAMETRO"),
              values=c(paste("'",format(OAR$first.time,"%Y-%m-%d"),"'",sep=""),
                       OAR$id.staz,
                       id.param),
              verbose=verbose)
  dbCommit(con)
  ## preparativi
  date4db <- function(x) {format(x,format="%Y-%m-%d %H:%M")}
  prov <- unlist(dbGetQuery(con,
                            paste("select COD_PRV from AA_ARIA.T$01$CONFIG_STAZIONI",
                                  "where ID_CONFIG_STAZ=",OAR$id.staz)))
  ## inserisce elaborazioni annuali floating
  dbqa.insert(con=con, tab="WEB_STAT",
              values=data.frame(GIORNO         =date4db(OAR$first.time),
                                ID_CONFIG_STAZ =OAR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=c(33,34), # AOT40 vegetazione,foreste
                                ID_EVENTO      =0,
                                V_ELAB_F       =c(OAR$annual.report$aot40.veget,
                                                  OAR$annual.report$aot40.forest),
                                TS1_V1_ELAB    =c(date4db(OAR$first.time),
                                                  date4db(OAR$first.time)),
                                TS2_V1_ELAB    =c(date4db(OAR$last.time),
                                                  date4db(OAR$last.time)),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =c(as.numeric(!is.null(OAR$annual.report$aot40.veget.PercValid) &&
                                                               OAR$annual.report$aot40.veget.PercValid>=90),
                                                  as.numeric(!is.null(OAR$annual.report$aot40.forest.PercValid) &&
                                                               OAR$annual.report$aot40.forest.PercValid>=90)),
                                N_DATI         =c(OAR$annual.report$aot40.veget.NhValid,
                                                  OAR$annual.report$aot40.forest.NhValid),
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  ## inserisce elaborazioni annuali integer
  dbqa.insert(con=con, tab="WEB_STAT",
              values=data.frame(GIORNO         =date4db(OAR$first.time),
                                ID_CONFIG_STAZ =OAR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=c(128,10085,10089), #n.ore sup.120,180,240
                                ID_EVENTO      =0,
                                V_ELAB_I       =c(OAR$annual.report$cumul.nexc.120,
                                                  OAR$annual.report$cumul.nexc.180,
                                                  OAR$annual.report$cumul.nexc.240),
                                TS1_V1_ELAB    =date4db(OAR$first.time),
                                TS2_V1_ELAB    =date4db(OAR$last.time),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =c(as.numeric(!is.null(OAR$annual.report$nValidMonths) &&
                                                               OAR$annual.report$nValidMonths>=5),
                                                  as.numeric(!is.null(OAR$annual.report$nValidMonths0820) &&
                                                               OAR$annual.report$nValidMonths0820>=5),
                                                  as.numeric(!is.null(OAR$annual.report$nValidMonths0820) &&
                                                               OAR$annual.report$nValidMonths0820>=5)),
                                N_DATI         =OAR$annual.report$annual.nValid,
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  ## inserisce elaborazioni di evento floating
  if(!is.null(unlist(OAR$events))){
    nev180 <- ifelse(is.data.frame(OAR$events$exc.180),
                     nrow(OAR$events$exc.180),
                     length(OAR$events$exc.180))
    nev240 <- ifelse(is.data.frame(OAR$events$exc.240),
                     nrow(OAR$events$exc.240),
                     length(OAR$events$exc.240))
    if(nev180>0){
      dbqa.insert(con=con, tab="WEB_STAT",
                  values=data.frame(GIORNO         =date4db(OAR$first.time),
                                    ID_CONFIG_STAZ =OAR$id.staz,
                                    COD_PRV        =prov,
                                    ID_PARAMETRO   =id.param,
                                    ID_ELABORAZIONE=rep(83,nev180), #conc.max evento 180
                                    ID_EVENTO      =0:(nev180-1),
                                    V_ELAB_F       =OAR$events$exc.180$max,
                                    TS1_V1_ELAB    =date4db(OAR$events$exc.180$start.time),
                                    TS2_V1_ELAB    =date4db(OAR$events$exc.180$start.time+
                                                              3600*OAR$events$exc.180$duration),
                                    TS_INS         =date4db(Sys.time()),
                                    FLG_ELAB       =1,
                                    row.names = NULL),
                  to_date=c(1,8,9,10),
                  verbose=verbose,
                  ...)          
    }
    if(nev240>0){
      dbqa.insert(con=con, tab="WEB_STAT",
                  values=data.frame(GIORNO         =date4db(OAR$first.time),
                                    ID_CONFIG_STAZ =OAR$id.staz,
                                    COD_PRV        =prov,
                                    ID_PARAMETRO   =id.param,
                                    ID_ELABORAZIONE=rep(86,nev240), #conc.max evento 240
                                    ID_EVENTO      =0:(nev240-1),
                                    V_ELAB_F       =OAR$events$exc.240$max,
                                    TS1_V1_ELAB    =date4db(OAR$events$exc.240$start.time),
                                    TS2_V1_ELAB    =date4db(OAR$events$exc.240$start.time+
                                                              3600*OAR$events$exc.240$duration),
                                    TS_INS         =date4db(Sys.time()),
                                    FLG_ELAB       =1,
                                    row.names = NULL),
                  to_date=c(1,8,9,10),
                  verbose=verbose,
                  ...)          
    }
  }
}


