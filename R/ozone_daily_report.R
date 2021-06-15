## procedura che produce quotidianamente le elaborazioni sui dati di ozono

## Operazioni:
## 1) estrazione
## 2) calcolo indicatori e loro validita'
## 3) identificazione eventi
## 4) scrittura su DB

## DA COMMENTARE PRIMA DEL PACKAGING:
##-------------------------------------
#  source("./R/dbqa.functions.R")
#  source("./R/aqstat.functions.R")
#  source("./R/xts.functions.R")
#  source("./R/time.functions.R")
##-------------------------------------

prepare.ozone_daily_report <- function(con,
                                       id.staz,
                                       Date=NULL,
                                       ...){
  ## 0) operazioni preliminari
  if(is.null(Date)) {
    Date <- format(as.POSIXct(paste(Sys.Date(),"00:00"), tz="Africa/Algiers")-60*60*6,"%Y-%m-%d") # ieri
  }
  f.date <- as.POSIXct(paste(Date,"23:59"), tz="Africa/Algiers") # fine giornata
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

calculate.ozone_daily_report <- function(data){
  
  Dat <- data$Dat
  id.param=7
  
  if(!is.null(Dat)){
    ## 2) calcolo indicatori e loro validita'
    Time <- index(Dat)
    day <- Ymd(Time)
    yDat <- last(Dat,'1 year') # solo dati ultimo anno
    yTime <- index(yDat)
    yday <- Ymd(yTime)
    dDat <- last(Dat,'1 day') # solo dati ultimo giorno
    dTime <- index(dDat)
    dday <- Ymd(dTime)
    yDatR <- dbqa.round(as.vector(yDat),id.param=7) # solo dati ultimo anno, arrotondati (ma dovrebbero gia' esserlo)
    dDatR <- dbqa.round(as.vector(dDat),id.param=7) # solo dati ultimo giorno, arrotondati (ma dovrebbero gia' esserlo)
    ## - max giornaliero (con arrotondamento)
    max.day <- dbqa.round(stat.period(x=as.vector(dDat),period=dday,necess=1,FUN=max),id.param=7)
    hour.max.day <- Hour(index(dDat))[unlist(which.period(x=as.vector(dDat),
                                                          period=dday,
                                                          necess=1,
                                                          FUN=which.max))][1]
    # - orari di superamento 180 e 240
#     hours.exc.180 <- squeeze(Hour(index(dDat))[which(dDatR>180)])
#     hours.exc.240 <- squeeze(Hour(index(dDat))[which(dDatR>240)])
    # - max media mobile 8h (con arrotondamento)
    ave.8h <- dbqa.round(mean_window(x=as.vector(Dat),k=8,necess=6),id.param=7)
    max.ave.8h <- stat.period(x=ave.8h,period=day,necess=18,FUN=max)[-1]
    ## - no. sup. orari soglia 180 nel giorno (valori arrotondati)
    nexc.180 <- sum(as.numeric(dDatR>180), na.rm=T)
    ## - no. sup. orari soglia 240 nel giorno (valori arrotondati)
    nexc.240 <- sum(as.numeric(dDatR>240), na.rm=T)
    ## - no. sup. orari soglia 180 da inizio anno (valori arrotondati)
    cumul.nexc.180 <- sum(as.numeric(yDatR>180), na.rm=T)
    ## - no. sup. orari soglia 240 da inizio anno (valori arrotondati)
    cumul.nexc.240 <- sum(as.numeric(yDatR>240), na.rm=T)
    ## - no. sup. giorn. soglia 120 da inizio anno
    cumul.nexc.120 <- sum(as.numeric(dbqa.round(max.ave.8h,id.param)>120), na.rm=T)
    
    daily.report <- data.frame(max.day=      max.day,
                               hour.max.day= hour.max.day,
                               max.ave.8h=   max.ave.8h[length(max.ave.8h)],
                               nexc.180=nexc.180,
                               nexc.240=nexc.240,
                               cumul.nexc.180=cumul.nexc.180,
                               cumul.nexc.240=cumul.nexc.240,
                               cumul.nexc.120=cumul.nexc.120)
    
  } else {
    daily.report <- data.frame(max.day=      NA,
                               hour.max.day= NA,
                               max.ave.8h=   NA,
                               nexc.180=NA,
                               nexc.240=NA,
                               cumul.nexc.180=     NA,
                               cumul.nexc.240=     NA,
                               cumul.nexc.120=     NA)
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
      ## tiene solo gli eventi che finiscono l'ultimo giorno
      end.time <- Time180 + Ev180$duration*60*60
      last.day <- dday[1]
      events.180 <- events.180[Ymd(end.time)==last.day,]
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
      ## tiene solo gli eventi che finiscono l'ultimo giorno
      end.time <- Time240 + Ev240$duration*60*60
      last.day <- dday[1]
      events.240 <- events.240[Ymd(end.time)==last.day,]
    } else {
      events.240 <- NULL
    }
    events <- list(exc.180=events.180,
                   exc.240=events.240)
  } else {
    events <- NULL
  }
  
  Out <- list(daily.report=daily.report,
              events=events,
              id.staz=data$id.staz,
              first.time.year=yTime[1],
              last.time=dTime[24]+3600,
              first.time.day=dTime[1])
  return(Out)
}

write.ozone_daily_report <- function(con,
                                     ODR,
                                     empty=F,
                                     verbose=F,
                                     ...) {
  
  ## svuota tabella se richiesto
  if(empty){
    dbGetQuery(con, 'truncate table WEB_BOLLETTINO_OZONO')
  }
  
  id.param = 7 # Ozono
  
  ## elimina record del GIORNO-CONFIG_STAZ-PARAMETRO
  dbqa.delete(con=con, tab="WEB_BOLLETTINO_OZONO",
              keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                     "ID_CONFIG_STAZ",
                     "ID_PARAMETRO"),
              values=c(paste("'",format(ODR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                       ODR$id.staz,
                       id.param),
              verbose=verbose)
  dbCommit(con)
  date4db <- function(x) {format(x,format="%Y-%m-%d %H:%M")}
  prov <- unlist(dbGetQuery(con,
                            paste("select COD_PRV from AA_ARIA.T$01$CONFIG_STAZIONI",
                                  "where ID_CONFIG_STAZ=",ODR$id.staz)))
  ## inserisce elaborazioni giornaliere floating
  ## e se sono <LOD mette "<" in SEGNO e il LOD in V_ELAB_C
  lod <- dbqa.lod(con = con, id.param = id.param, days = ODR$first.time.day)
  dbqa.insert(con=con, tab="WEB_BOLLETTINO_OZONO",
              values=data.frame(GIORNO         =date4db(ODR$first.time.day),
                                ID_CONFIG_STAZ =ODR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=c(3,88), #max med 8h, max day
                                ID_EVENTO      =0,
                                V_ELAB_F       =c(ODR$daily.report$max.ave.8h,
                                                  ODR$daily.report$max.day),
                                TS1_V1_ELAB    =c(date4db(ODR$first.time.day),
                                                  date4db(ODR$first.time.day+
                                                           3600*ODR$daily.report$hour.max.day)),
                                TS2_V1_ELAB    =c(date4db(ODR$last.time),
                                                  date4db(ODR$first.time.day+
                                                           3600*ODR$daily.report$hour.max.day+
                                                           60*60)),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =1,
                                V_ELAB_C       =ifelse(test = c(ODR$daily.report$max.ave.8h,
                                                                ODR$daily.report$max.day) < lod,
                                                       yes  = as.character(lod),
                                                       no   = ""),
                                SEGNO          =ifelse(test = c(ODR$daily.report$max.ave.8h,
                                                                ODR$daily.report$max.day) < lod,
                                                       yes  = "<",
                                                       no   = ""),
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  ## inserisce elaborazioni giornaliere integer
  dbqa.insert(con=con, tab="WEB_BOLLETTINO_OZONO",
              values=data.frame(GIORNO         =date4db(ODR$first.time.day),
                                ID_CONFIG_STAZ =ODR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=c(84,87), #n.ore sup.180,240
                                ID_EVENTO      =0,
                                V_ELAB_I       =c(ODR$daily.report$nexc.180,
                                                  ODR$daily.report$nexc.240),
                                TS1_V1_ELAB    =date4db(ODR$first.time.day),
                                TS2_V1_ELAB    =date4db(ODR$last.time),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =1,
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  ## inserisce elaborazioni cumulate da inizio anno integer
  dbqa.insert(con=con, tab="WEB_BOLLETTINO_OZONO",
              values=data.frame(GIORNO         =date4db(ODR$first.time.day),
                                ID_CONFIG_STAZ =ODR$id.staz,
                                COD_PRV        =prov,
                                ID_PARAMETRO   =id.param,
                                ID_ELABORAZIONE=c(82,85,89), #n.ore sup.120,180,240
                                ID_EVENTO      =0,
                                V_ELAB_I       =c(ODR$daily.report$cumul.nexc.120,
                                                  ODR$daily.report$cumul.nexc.180,
                                                  ODR$daily.report$cumul.nexc.240),
                                TS1_V1_ELAB    =date4db(ODR$first.time.year),
                                TS2_V1_ELAB    =date4db(ODR$last.time),
                                TS_INS         =date4db(Sys.time()),
                                FLG_ELAB       =1,
                                row.names = NULL),
              to_date=c(1,8,9,10),
              verbose=verbose,
              ...)
  ## inserisce elaborazioni di evento floating
  if(!is.null(unlist(ODR$events))){
    nev180 <- ifelse(is.data.frame(ODR$events$exc.180),
                     nrow(ODR$events$exc.180),
                     length(ODR$events$exc.180))
    nev240 <- ifelse(is.data.frame(ODR$events$exc.240),
                     nrow(ODR$events$exc.240),
                     length(ODR$events$exc.240))
    if(nev180>0){
      dbqa.insert(con=con, tab="WEB_BOLLETTINO_OZONO",
                  values=data.frame(GIORNO         =date4db(ODR$first.time.day),
                                    ID_CONFIG_STAZ =ODR$id.staz,
                                    COD_PRV        =prov,
                                    ID_PARAMETRO   =id.param,
                                    ID_ELABORAZIONE=rep(83,nev180), #conc.max evento 180
                                    ID_EVENTO      =0:(nev180-1),
                                    V_ELAB_F       =ODR$events$exc.180$max,
                                    TS1_V1_ELAB    =date4db(ODR$events$exc.180$start.time),
                                    TS2_V1_ELAB    =date4db(ODR$events$exc.180$start.time+
                                                               3600*ODR$events$exc.180$duration),
                                    TS_INS         =date4db(Sys.time()),
                                    FLG_ELAB       =1,
                                    row.names = NULL),
                  to_date=c(1,8,9,10),
                  verbose=verbose,
                  ...)          
    }
    if(nev240>0){
      dbqa.insert(con=con, tab="WEB_BOLLETTINO_OZONO",
                  values=data.frame(GIORNO         =date4db(ODR$first.time.day),
                                    ID_CONFIG_STAZ =ODR$id.staz,
                                    COD_PRV        =prov,
                                    ID_PARAMETRO   =id.param,
                                    ID_ELABORAZIONE=rep(86,nev240), #conc.max evento 240
                                    ID_EVENTO      =0:(nev240-1),
                                    V_ELAB_F       =ODR$events$exc.240$max,
                                    TS1_V1_ELAB    =date4db(ODR$events$exc.240$start.time),
                                    TS2_V1_ELAB    =date4db(ODR$events$exc.240$start.time+
                                                             3600*ODR$events$exc.240$duration),
                                    TS_INS         =date4db(Sys.time()),
                                    FLG_ELAB       =1,
                                    row.names = NULL),
                  to_date=c(1,8,9,10),
                  verbose=verbose,
                  ...)          
    }
  }
}


