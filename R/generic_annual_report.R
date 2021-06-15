## produce le elaborazioni annuali per un generico inquinante

## Operazioni:
## 1) estrazione
## 2) calcolo indicatori e loro validita'
## 3) scrittura su DB

prepare.annual_report <- function(con,
                                  id.staz,
                                  id.param,
                                  year=NULL,
                                  tstep,
                                  ...){
  ## 0) operazioni preliminari
  if(is.null(year)) {
    year <- format(as.POSIXct(paste(Sys.Date(),"00:00"), tz="Africa/Algiers")-60*60*24*30*5,"%Y") # l'anno di 5 mesi fa
  }
  f.date <- as.POSIXct(paste(year,"-12-31 23:59",sep=""), tz="Africa/Algiers") # fine anno
  if(tstep=="H") {
    i.date <- as.POSIXct(paste(Year(f.date)-1,"-12-31 16:00",sep=""), tz="Africa/Algiers") # margine per media mobile    
  } else {
    i.date <- as.POSIXct(paste(Year(f.date),"-01-01 00:00",sep=""), tz="Africa/Algiers")
  }
  
  ## 1) estrazione
  if((as.POSIXct(Sys.time()) - i.date) > 365*2) {
    tab <- "storico"
  } else {
    tab <- "recente"
  }
  if(tstep=="H") {
    bulk_read <- 28  # valore ottimale per estrazione dati orari
  }else{
    bulk_read <- 7  # valore ottimale per estrazione dati giornalieri
  }
  Dat <- dbqa.get.datastaz(con=con,ts.range=c(i.date,f.date),
                           tstep=tstep,
                           id.staz=id.staz,
                           id.param=id.param,
                           #flg=c(0,1),
                           flg=1,
                           bulk_read=bulk_read,
                           table=tab,
                           ...)
  
  out <- list(Dat=Dat, id.staz=id.staz)
  return(out)
}

calculate.annual_report <- function(data,
                                    id.param,
                                    thr.daily.ave=NULL,
                                    thr.ave8h.max=NULL,
                                    thr.hourly=NULL,
                                    thr.multihourly=NULL,
                                    NH=3,
                                    critical.months=NULL){
  
  Dat <- data$Dat
  
  if(!is.null(Dat)){
    ## 2) calcolo indicatori e loro validità
    Time  <- index(Dat)
    day   <- Ymd(Time)
    yDat  <- last(Dat,'1 year') # solo dati ultimo anno
    yTime <- index(yDat)
    yday  <- Ymd(yTime)
    
    ## distingue dati orari-giornalieri
    hourly <- difftime(Time[2],Time[1],units="hours")==1
    daily  <- difftime(Time[2],Time[1],units="days")==1
    if(!hourly & !daily) stop("cannot manage timestep!")
    
    ## numero di giorni e ore nell'anno
    ndays  <- Ndays.in.year(Year(yTime[1]))
    nhours <- ndays*24
    
    ## calcola media annua con arrotondamento finale
    annual.mean      <- dbqa.round(mean(yDat, na.rm=T),id.param=id.param)
    annual.nValid    <- sum(as.numeric(!is.na(yDat)))
    if(hourly) {
      annual.percValid <- annual.nValid/nhours*100
      annual.nExpected <- nhours/24*23
    }
    if(daily) {
      annual.percValid <- annual.nValid/ndays*100
      annual.nExpected <- ndays-4 
    }
    annual.efficiency <- round_awayfromzero(annual.nValid/annual.nExpected*100)
    
    annual.report <- data.frame(annual.mean      =annual.mean,
                                annual.nValid    =annual.nValid,
                                annual.percValid =annual.percValid,     
                                annual.nExpected =annual.nExpected,
                                annual.efficiency=annual.efficiency)
  } else {
    annual.report <- data.frame(annual.mean      =NA,
                                annual.nValid    =NA,
                                annual.percValid =NA,     
                                annual.nExpected =NA,
                                annual.efficiency=NA)
  }
  
  ## calcola media nei mesi prescelti
  if(!is.null(critical.months)){
    if(!is.null(yDat)){
      cmDat <- yDat[as.numeric(Month(yTime)) %in% critical.months]
      critmonths.mean      <- dbqa.round(mean(cmDat, na.rm=T),id.param)
      critmonths.nValid    <- sum(as.numeric(!is.na(cmDat)))
      critmonths.percValid <- critmonths.nValid/length(cmDat)*100
      if(hourly) critmonths.nExpected <- length(cmDat)/24*23
      if(daily)  critmonths.nExpected <- length(cmDat)-(length(critical.months)%/%3) # esige un dato in meno a trimestre
      critmonths.efficiency <- round_awayfromzero(critmonths.nValid/critmonths.nExpected*100)
      
      annual.report <- data.frame(annual.report,
                                  critmonths.mean      =critmonths.mean,
                                  critmonths.nValid    =critmonths.nValid,
                                  critmonths.percValid =critmonths.percValid,     
                                  critmonths.nExpected =critmonths.nExpected,
                                  critmonths.efficiency=critmonths.efficiency)
    }else{
      annual.report <- data.frame(annual.report,
                                  critmonths.mean      =NA,
                                  critmonths.nValid    =NA,
                                  critmonths.percValid =NA,     
                                  critmonths.nExpected =NA,
                                  critmonths.efficiency=NA)
    }
  }
  
  ## calcola superamenti giornalieri
  if(!is.null(thr.daily.ave)){
    if(!is.null(Dat)){
      if(hourly) dDat <- stat.period(x=yDat,period=yday,necess=18,FUN=mean)
      if(daily)  dDat <- Dat
      daily.nexc      <- sum(as.numeric(dDat>thr.daily.ave), na.rm=T)
      daily.nValid    <- sum(as.numeric(!is.na(dDat)))
      daily.percValid <- daily.nValid/ndays*100
      
      annual.report <- data.frame(annual.report,
                                  daily.nexc     =daily.nexc,
                                  daily.nValid   =daily.nValid,
                                  daily.percValid=daily.percValid)
    }else{
      annual.report <- data.frame(annual.report,
                                  daily.nexc     =NA,
                                  daily.nValid   =NA,
                                  daily.percValid=NA)
    }
  }
  
  ## calcola superamenti giornalieri del max della media 8h
  if(!is.null(thr.ave8h.max)){
    if(!is.null(Dat)){
      if(hourly) ave.8h <- mean_window(x=as.vector(Dat),k=8,necess=6)
      if(daily)  stop("cannot calculate 8h moving average for daily data!")
      max.ave.8h <- stat.period(x=ave.8h,period=day,necess=18,FUN=max)[-1]
      ave8h.nexc      <- sum(as.numeric(dbqa.round(max.ave.8h,id.param)>thr.ave8h.max), na.rm=T)
      ave8h.yave      <- dbqa.round(mean(max.ave.8h, na.rm=T), id.param)
      ave8h.nValid    <- sum(as.numeric(!is.na(max.ave.8h)))
      ave8h.percValid <- ave8h.nValid/ndays*100
      
      annual.report <- data.frame(annual.report,
                                  ave8h.nexc     =ave8h.nexc,
                                  ave8h.yave     =ave8h.yave,
                                  ave8h.nValid   =ave8h.nValid,
                                  ave8h.percValid=ave8h.percValid)
    }else{
      annual.report <- data.frame(annual.report,
                                  ave8h.nexc     =NA,
                                  ave8h.yave     =NA,
                                  ave8h.nValid   =NA,
                                  ave8h.percValid=NA)
    }
  }
  
  ## calcola dati validi orari
  if(!is.null(thr.hourly) | !is.null(thr.multihourly)){
    if(!is.null(yDat) & hourly){
      hourly.nValid    <- sum(as.numeric(!is.na(yDat)))
      hourly.percValid <- hourly.nValid/nhours*100
      
      annual.report <- data.frame(annual.report,
                                  hourly.nValid   =hourly.nValid,
                                  hourly.percValid=hourly.percValid)
    }else{
      annual.report <- data.frame(annual.report,
                                  hourly.nValid   =NA,
                                  hourly.percValid=NA)
    }
  }
  
  ## conta superamenti orari
  if(!is.null(thr.hourly)){
    if(!is.null(yDat) & hourly){
      hourly.nexc <- sum(as.numeric(dbqa.round(yDat,id.param = id.param)>thr.hourly),
                         na.rm=T)
      
      annual.report <- data.frame(annual.report,
                                  hourly.nexc=hourly.nexc)
    }else{
      annual.report <- data.frame(annual.report,
                                  hourly.nexc=NA)
    }
  }
  
  ## conta superamenti orari per piu' di NH ore consecutive
  if(!is.null(thr.multihourly)){
    if(!is.null(yDat) & hourly){
      multihourly.exc <- detect.event(dbqa.round(yDat,id.param = id.param),
                                      thr.multihourly)$duration > NH
      multihourly.nexc <- sum(as.numeric(multihourly.exc), na.rm=T)
      
      annual.report <- data.frame(annual.report,
                                  multihourly.nexc=multihourly.nexc)
    }else{
      annual.report <- data.frame(annual.report,
                                  multihourly.nexc=NA)
    }
  }
  Out <- list(annual.report=annual.report,
              id.staz=data$id.staz,
              first.time=yTime[1],
              last.time=yTime[length(yTime)])
  return(Out)
}


write.annual_report <- function(con,
                                AR,
                                id.param,
                                verbose=F,
                                ...) {
  
  ## elimina record del GIORNO-CONFIG_STAZ-PARAMETRO
  dbqa.delete(con=con, tab="WEB_STAT",
              keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                     "ID_CONFIG_STAZ",
                     "ID_PARAMETRO"),
              values=c(paste("'",format(AR$first.time,"%Y-%m-%d"),"'",sep=""),
                       AR$id.staz,
                       id.param),
              verbose=verbose)
  dbCommit(con)
  ## preparativi
  date4db <- function(x) {format(x,format="%Y-%m-%d %H:%M")}
  prov <- unlist(dbGetQuery(con,
                            paste("select COD_PRV from AA_ARIA.T$01$CONFIG_STAZIONI",
                                  "where ID_CONFIG_STAZ=",AR$id.staz)))
  
  ## gestisce LOD
  lod <- dbqa.lod(con = con, id.param = id.param, days = AR$last.time)
  ## inserisce media annua
  id.elab=30
  flg.elab=as.numeric(!is.null(AR$annual.report$annual.efficiency) &&
                        AR$annual.report$annual.efficiency>=90)
  dbqa.insert(con=con, tab="WEB_STAT",
              values=data.frame(GIORNO         =date4db(AR$first.time),
                                ID_CONFIG_STAZ =AR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=id.elab,
                                ID_EVENTO      =0,
                                V_ELAB_F       =AR$annual.report$annual.mean,
                                TS1_V1_ELAB    =date4db(AR$first.time),
                                TS2_V1_ELAB    =date4db(AR$last.time),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =flg.elab,
                                N_DATI         =AR$annual.report$annual.nValid,
                                V_ELAB_C       =ifelse(test = AR$annual.report$annual.mean < lod,
                                                       yes  = as.character(lod),
                                                       no   = ""),
                                SEGNO          =ifelse(test = AR$annual.report$annual.mean < lod,
                                                       yes  = "<",
                                                       no   = ""),
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  
  ## inserisce numero superamenti della media giornaliera (PM10 o SO2)
  if(id.param %in% c(1,5) & ("daily.nexc" %in% colnames(AR$annual.report))) { 
    if(id.param==1) id.elab=10122
    if(id.param==5) id.elab=10130
    flg.elab=as.numeric(!is.null(AR$annual.report$annual.efficiency) &&
                          AR$annual.report$annual.efficiency>=90)
    dbqa.insert(con=con, tab="WEB_STAT",
                values=data.frame(GIORNO         =date4db(AR$first.time),
                                  ID_CONFIG_STAZ =AR$id.staz,
                                  COD_PRV        =prov,
                                  ID_PARAMETRO   =id.param,
                                  ID_ELABORAZIONE=id.elab,
                                  ID_EVENTO      =0,
                                  V_ELAB_I       =AR$annual.report$daily.nexc,
                                  TS1_V1_ELAB    =date4db(AR$first.time),
                                  TS2_V1_ELAB    =date4db(AR$last.time),
                                  TS_INS         =date4db(Sys.time()),
                                  FLG_ELAB       =flg.elab,
                                  N_DATI         =AR$annual.report$daily.nValid,
                                  row.names = NULL),
                to_date=c(1,8,9,10),
                verbose=verbose,
                ...)
  }
  
  ## inserisce numero superamenti della media oraria (NO2 o SO2)
  if(id.param %in% c(1,8) & ("hourly.nexc" %in% colnames(AR$annual.report))) { 
    if(id.param==1) id.elab=121
    if(id.param==8) id.elab=10119
    flg.elab=as.numeric(!is.null(AR$annual.report$annual.efficiency) &&
                          AR$annual.report$annual.efficiency>=90)
    dbqa.insert(con=con, tab="WEB_STAT",
                values=data.frame(GIORNO         =date4db(AR$first.time),
                                  ID_CONFIG_STAZ =AR$id.staz,
                                  COD_PRV        =prov,
                                  ID_PARAMETRO   =id.param,
                                  ID_ELABORAZIONE=id.elab,
                                  ID_EVENTO      =0,
                                  V_ELAB_I       =AR$annual.report$hourly.nexc,
                                  TS1_V1_ELAB    =date4db(AR$first.time),
                                  TS2_V1_ELAB    =date4db(AR$last.time),
                                  TS_INS         =date4db(Sys.time()),
                                  FLG_ELAB       =flg.elab,
                                  N_DATI         =AR$annual.report$hourly.nValid,
                                  row.names = NULL),
                to_date=c(1,8,9,10),
                verbose=verbose,
                ...)
  }
  
  ## inserisce numero superamenti del max giorn.media 8h (CO)
  if(id.param %in% c(10) & ("ave8h.nexc" %in% colnames(AR$annual.report))) { 
    if(id.param==10) id.elab=10118
    flg.elab=as.numeric(!is.null(AR$annual.report$annual.efficiency) &&
                          AR$annual.report$annual.efficiency>=90)
    dbqa.insert(con=con, tab="WEB_STAT",
                values=data.frame(GIORNO         =date4db(AR$first.time),
                                  ID_CONFIG_STAZ =AR$id.staz,
                                  COD_PRV        =prov,
                                  ID_PARAMETRO   =id.param,
                                  ID_ELABORAZIONE=id.elab,
                                  ID_EVENTO      =0,
                                  V_ELAB_I       =AR$annual.report$ave8h.nexc,
                                  TS1_V1_ELAB    =date4db(AR$first.time),
                                  TS2_V1_ELAB    =date4db(AR$last.time),
                                  TS_INS         =date4db(Sys.time()),
                                  FLG_ELAB       =flg.elab,
                                  N_DATI         =AR$annual.report$ave8h.nValid,
                                  row.names = NULL),
                to_date=c(1,8,9,10),
                verbose=verbose,
                ...)
  }
  
  ## inserisce media annua del max giorn.media 8h (CO)
  if(id.param %in% c(10) & ("ave8h.yave" %in% colnames(AR$annual.report))) { 
    if(id.param==10) id.elab=821
    flg.elab=as.numeric(!is.null(AR$annual.report$annual.efficiency) &&
                          AR$annual.report$annual.efficiency>=90)
    dbqa.insert(con=con, tab="WEB_STAT",
                values=data.frame(GIORNO         =date4db(AR$first.time),
                                  ID_CONFIG_STAZ =AR$id.staz,
                                  COD_PRV        =prov,
                                  ID_PARAMETRO   =id.param,
                                  ID_ELABORAZIONE=id.elab,
                                  ID_EVENTO      =0,
                                  V_ELAB_F       =AR$annual.report$ave8h.yave,
                                  TS1_V1_ELAB    =date4db(AR$first.time),
                                  TS2_V1_ELAB    =date4db(AR$last.time),
                                  TS_INS         =date4db(Sys.time()),
                                  FLG_ELAB       =flg.elab,
                                  N_DATI         =AR$annual.report$ave8h.nValid,
                                  row.names = NULL),
                to_date=c(1,8,9,10),
                verbose=verbose,
                ...)
  }
}