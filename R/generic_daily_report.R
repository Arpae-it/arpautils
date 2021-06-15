## procedura che produce quotidianamente le elaborazioni sui dati di un inquinante

## Operazioni:
## 1) estrazione
## 2) calcolo indicatori e loro validita'
## 3) identificazione eventi
## 4) scrittura su DB


prepare.daily_report <- function(con,
                                 id.staz,
                                 id.param,
                                 Date=NULL,
                                 tstep,
                                 ...){
  ## 0) operazioni preliminari
  if(is.null(Date)) {
    Date <- format(as.POSIXct(paste(Sys.Date(),"00:00"), tz="Africa/Algiers")-60*60*6,"%Y-%m-%d") # ieri
  }
  f.date <- as.POSIXct(paste(Date,"23:59"), tz="Africa/Algiers") # fine giornata
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
                           bulk_read=bulk_read,  # valore ottimale
                           table=tab,
                           ...)
  
  out <- list(Dat=Dat, id.staz=id.staz)
  return(out)
}

calculate.daily_report <- function(data,
                                   id.param,
                                   thr.daily.ave=NULL,
                                   thr.ave8h.max=NULL,
                                   thr.hourly=NULL,
                                   thr.multihourly=NULL,
                                   NH=3){
 
  Dat <- data$Dat
   
   if(!is.null(Dat)){
    ## 2) calcolo indicatori e loro validita'
    Time <- index(Dat)
    day <- Ymd(Time)
    #yDat <- last(Dat,'1 year') # solo dati ultimo anno. Non funziona per un valore solo
    if ( length(Dat) > 1) {
      yDat <- last(Dat,'1 year')
      } else {
      yDat <- last(Dat)
      }
    yTime <- index(yDat)
    yday <- Ymd(yTime)
    #dDat <- last(Dat,'1 day') # solo dati ultimo giorno. Non funziona per un valore solo
    if ( length(Dat) > 1) {
      dDat <- last(Dat,'1 day')
    } else {
      dDat <- last(Dat)
    }
    dTime <- index(dDat)
    dday <- Ymd(dTime)
    yDatR <- dbqa.round(as.vector(yDat),id.param=id.param) # solo dati ultimo anno, arrotondati (ma dovrebbero gia' esserlo)
    dDatR <- dbqa.round(as.vector(dDat),id.param=id.param) # solo dati ultimo giorno, arrotondati (ma dovrebbero gia' esserlo)
    
    ## distingue dati orari-giornalieri
    hourly<-FALSE
    daily<-FALSE 
    if ( length(Time) > 1)  {
      hourly <- difftime(Time[2],Time[1],units="hours")==1
      daily  <- difftime(Time[2],Time[1],units="days")==1
    }  else if ( length(Time) == 1)  {
      daily<-TRUE   #qui faccio una forzatura: suppongo che se c'e' un dato solo sia giornaliero (il 01/01)
    }

    if(!hourly & !daily) stop("cannot manage timestep!")
  } else {  #fine check su Dat
    daily.report <- NULL
    hourly <- FALSE
    daily <- FALSE 
    
  }
  
  if(hourly){
    ## - max giornaliero (con arrotondamento)
    max.day <- dbqa.round(stat.period(x=as.vector(dDat),period=dday,necess=18,FUN=max),
                          id.param=id.param)
    hour.max.day <- Hour(index(dDat))[unlist(which.period(x=as.vector(dDat),
                                                          period=dday,
                                                          necess=18,
                                                          FUN=which.max))][1]
    ## - media giornaliera (con arrotondamento)
    mean.day <- dbqa.round(stat.period(x=as.vector(dDat),period=dday,necess=18,FUN=mean),
                          id.param=id.param)
    # - max media mobile 8h (con arrotondamento)
    ave.8h <- dbqa.round(mean_window(x=as.vector(Dat),k=8,necess=6),id.param=id.param)
    max.ave.8h <- stat.period(x=ave.8h,period=day,necess=18,FUN=max)[-1] # tolgo primo giorno dell'anno precedente (NA)
    ## - no. sup. orari nel giorno (valori arrotondati)
    nexc.hourly <- sum(as.numeric(dDatR>thr.hourly), na.rm=T)
    hourly.nValid    <- sum(as.numeric(!is.na(dDatR)))
    
    daily.report <- data.frame(mean.day=     mean.day,
                               max.day=      max.day,
                               hour.max.day= hour.max.day,
                               max.ave.8h=   max.ave.8h[length(max.ave.8h)], # solo ultimo giorno
                               nexc.hourly=  nexc.hourly,
                               hourly.nValid=hourly.nValid)
    
  } else {
    daily.report <- data.frame(mean.day=     NA,
                               max.day=      NA,
                               hour.max.day= NA,
                               max.ave.8h=   NA,
                               nexc.hourly=  NA,
                               hourly.nValid=NA)
  }
  
  
  ## calcola superamenti giornalieri
  if(!is.null(thr.daily.ave)){
    if(!is.null(Dat)){
      if(hourly) DDat <- stat.period(x=yDat,period=yday,necess=18,FUN=mean)
      if(daily)  DDat <- Dat
      cumul.daily.nexc      <- sum(as.numeric(DDat>thr.daily.ave), na.rm=T)
      cumul.daily.nValid    <- sum(as.numeric(!is.na(DDat)))
      
      daily.report <- data.frame(daily.report,
                                 cumul.daily.nexc     =cumul.daily.nexc,
                                 cumul.daily.nValid   =cumul.daily.nValid)
    }else{
      daily.report <- data.frame(daily.report,
                                 cumul.daily.nexc     =NA,
                                 cumul.daily.nValid   =NA)
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
      
      daily.report <- data.frame(daily.report,
                                 cumul.ave8h.nexc     =ave8h.nexc,
                                 cumul.ave8h.yave     =ave8h.yave,
                                 cumul.ave8h.nValid   =ave8h.nValid)
    }else{
      daily.report <- data.frame(daily.report,
                                 cumul.ave8h.nexc     =NA,
                                 cumul.ave8h.yave     =NA,
                                 cumul.ave8h.nValid   =NA)
    }
  }
  
  ## calcola dati validi orari (dall'inizio dell'anno)
  if(!is.null(thr.hourly) | !is.null(thr.multihourly)){
    if(!is.null(yDat) & hourly){
      cumul.hourly.nValid    <- sum(as.numeric(!is.na(yDat)))
      
      daily.report <- data.frame(daily.report,
                                 cumul.hourly.nValid   =cumul.hourly.nValid)
    }else{
      daily.report <- data.frame(daily.report,
                                 cumul.hourly.nValid   =NA)
    }
  }
  
  ## conta superamenti orari (dall'inizio dell'anno)
  if(!is.null(thr.hourly)){
    if(!is.null(yDat) & hourly){
      cumul.hourly.nexc <- sum(as.numeric(yDat>thr.hourly), na.rm=T)
      
      daily.report <- data.frame(daily.report,
                                 cumul.hourly.nexc=cumul.hourly.nexc)
    }else{
      daily.report <- data.frame(daily.report,
                                 cumul.hourly.nexc=NA)
    }
  }
  
  ## conta superamenti orari per piu' di NH ore consecutive
  if(!is.null(thr.multihourly)){
    if(!is.null(yDat) & hourly){
      multihourly.exc <- detect.event(yDat,thr.multihourly)$duration > NH
      multihourly.nexc <- sum(as.numeric(multihourly.exc), na.rm=T)
      
      daily.report <- data.frame(daily.report,
                                 cumul.multihourly.nexc=multihourly.nexc)
    }else{
      daily.report <- data.frame(daily.report,
                                 cumul.multihourly.nexc=NA)
    }
  }
  
  
  ## 3) identificazione eventi
  if(!is.null(Dat) & !is.null(thr.hourly)){
    EvExc <- detect.event(yDatR,thr.hourly)
    if(is.data.frame(EvExc)) {
      TimeExc <- index(yDat)[EvExc$index]
      MaxExc <- EvExc$max
      events.Exc <- data.frame(start.time=TimeExc,
                               duration=EvExc$duration,
                               max=MaxExc)
      ## tiene solo gli eventi che finiscono l'ultimo giorno
      end.time <- TimeExc + EvExc$duration*60*60
      last.day <- dday[1]
      events.Exc <- events.Exc[Ymd(end.time)==last.day,]
    } else {
      events.Exc <- NULL
    }
    events <- list(events.Exc=events.Exc) # per simmetria con l'ozono
  } else {
    events <- NULL
  }
  
  Out <- list(daily.report=daily.report,
              events=events,
              id.staz=data$id.staz,
              first.time.year=yTime[1],
              last.time=if(hourly){dTime[24]+3600}else{dTime[1]+24*3600},
              first.time.day=dTime[1])
  return(Out)
}

write.daily_report <- function(con,
                               DR,
                               id.param,
                               verbose=F,
                               ...) {
  
  ## elimina record del GIORNO-CONFIG_STAZ-PARAMETRO

  date4db <- function(x) {format(x,format="%Y-%m-%d %H:%M")}
  prov <- unlist(dbGetQuery(con,
                            paste("select COD_PRV from AA_ARIA.T$01$CONFIG_STAZIONI",
                                  "where ID_CONFIG_STAZ=",DR$id.staz)))
  ## gestisce LOD
  lod <- dbqa.lod(con = con, id.param = id.param, days = DR$first.time.day)
  
  ## funzione per la scrittura su tabella WEB_STAT
  dbqa.insert_elab <- function(id_elab,id_evento=0,v_elab,type,ts1,ts2,n_dati,flg_elab=1,...) {
    vv <- data.frame(GIORNO         =date4db(DR$first.time.day),
                     ID_CONFIG_STAZ =DR$id.staz,
                     COD_PRV        =prov,
                     ID_PARAMETRO   =id.param,
                     ID_ELABORAZIONE=id_elab,
                     ID_EVENTO      =id_evento,
                     V_ELAB_I       =v_elab,
                     TS1_V1_ELAB    =ts1,
                     TS2_V1_ELAB    =ts2,
                     TS_INS         =date4db(Sys.time()),
                     FLG_ELAB       =flg_elab,
                     row.names = NULL)
    if(!is.na(n_dati)) {
      vv <- data.frame(vv,
                       N_DATI       =n_dati,
                       row.names = NULL)
      
    }
    if(type=="F") {
      colnames(vv)[7] <- "V_ELAB_F"
      vv <- data.frame(vv,
                       V_ELAB_C       =ifelse(test = v_elab < lod,
                                              yes  = as.character(lod),
                                              no   = ""),
                       SEGNO          =ifelse(test = v_elab < lod,
                                              yes  = "<",
                                              no   = ""),
                       row.names = NULL)
    }
    dbqa.insert(con=con, tab="WEB_STAT",
                values=vv,
                to_date=c(1,8,9,10),
                verbose=verbose,
                ...)
  }#fine definizione della funzione
  
  ## inserisce elaborazioni giornaliere floating
  # max med 8h (solo CO)
  if("max.ave.8h" %in% colnames(DR$daily.report) && !is.na(DR$daily.report$max.ave.8h)) {
    if(id.param==10) { 
      
      id.elab=3
      
      dbqa.delete(con=con, tab="WEB_STAT",
                  keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                         "ID_CONFIG_STAZ",
                         "ID_PARAMETRO",
                         "ID_ELABORAZIONE"),
                  values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                           DR$id.staz,
                           id.param, 
                           id.elab),
                  verbose=verbose)
      dbCommit(con)
      
      dbqa.insert_elab(id_elab=id.elab,
                       v_elab=DR$daily.report$max.ave.8h,
                       type="F",
                       ts1=date4db(DR$first.time.day),
                       ts2=date4db(DR$last.time),
                       n_dati=NA,
                       ...)
    }
  }
  
  # daily max
  if("max.day" %in% colnames(DR$daily.report) && !is.na(DR$daily.report$max.day)) {
    
    id.elab=88
    
    dbqa.delete(con=con, tab="WEB_STAT",
                keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                       "ID_CONFIG_STAZ",
                       "ID_PARAMETRO",
                       "ID_ELABORAZIONE"),
                values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                         DR$id.staz,
                         id.param, 
                         id.elab),
                verbose=verbose)
    dbCommit(con)
    
    dbqa.insert_elab(id_elab=id.elab,
                     v_elab=DR$daily.report$max.day,
                     type="F",
                     ts1=date4db(DR$first.time.day+3600*DR$daily.report$hour.max.day),
                     ts2=date4db(DR$first.time.day+3600*DR$daily.report$hour.max.day+60*60),
                     n_dati=DR$daily.report$hourly.nValid,
                     ...)
  }
  # daily mean
  if("mean.day" %in% colnames(DR$daily.report) && !is.na(DR$daily.report$mean.day)) {
    
    id.elab=2
    
    dbqa.delete(con=con, tab="WEB_STAT",
                keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                       "ID_CONFIG_STAZ",
                       "ID_PARAMETRO",
                       "ID_ELABORAZIONE"),
                values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                         DR$id.staz,
                         id.param, 
                         id.elab),
                verbose=verbose)
    dbCommit(con)
    
    dbqa.insert_elab(id_elab=id.elab,
                     v_elab=DR$daily.report$mean.day,
                     type="F",
                     ts1=date4db(DR$first.time.day),
                     ts2=date4db(DR$last.time),
                     n_dati=DR$daily.report$hourly.nValid,
                     ...)
  }
  
  ## inserisce elaborazioni giornaliere integer
  # hourly nexc (DI QUESTA NON C'E' ANCORA ID_ELABORAZIONE DEFINITO, CHIEDERE A FIL)
  
  ## inserisce elaborazioni cumulate da inizio anno integer
  ## NB con FLAG=0 e solo se l'anno ? tuttora in corso
  if(format(Sys.time(),format = "%Y")==format(DR$last.time,format = "%Y")) {
    # cumul daily (SO2, PM10)
    if(id.param%in%c(1,5)) {
      ide <- switch (as.character(id.param),
                     "1" = 122,
                     "5" = 130)
      
      dbqa.delete(con=con, tab="WEB_STAT",
                  keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                         "ID_CONFIG_STAZ",
                         "ID_PARAMETRO",
                         "ID_ELABORAZIONE"),
                  values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                           DR$id.staz,
                           id.param, 
                           ide),
                  verbose=verbose)
      dbCommit(con)
      
      dbqa.insert_elab(id_elab=ide,
                       v_elab=DR$daily.report$cumul.daily.nexc,
                       type="I",
                       ts1=date4db(DR$first.time.year),
                       ts2=date4db(DR$last.time),
                       n_dati=DR$daily.report$cumul.daily.nValid,
                       flg_elab = 0,
                       ...)
    }
    
    # cumul hourly (NO2)
    if(id.param==8) {
      ide <- switch (as.character(id.param),
                     "8" = 119)
      dbqa.delete(con=con, tab="WEB_STAT",
                  keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                         "ID_CONFIG_STAZ",
                         "ID_PARAMETRO",
                         "ID_ELABORAZIONE"),
                  values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                           DR$id.staz,
                           id.param, 
                           ide),
                  verbose=verbose)
      dbCommit(con)
      
      dbqa.insert_elab(id_elab=ide,
                       v_elab=DR$daily.report$cumul.hourly.nexc,
                       type="I",
                       ts1=date4db(DR$first.time.year),
                       ts2=date4db(DR$last.time),
                       n_dati=DR$daily.report$cumul.hourly.nValid,
                       flg_elab = 0,
                       ...)
    }
  } 
  
  # cumul max8h (CO)
  if(id.param==10) {
    ide <- switch (as.character(id.param),
                   "10" = 118)
    dbqa.delete(con=con, tab="WEB_STAT",
                keys=c("to_char(GIORNO,'YYYY-MM-DD')",
                       "ID_CONFIG_STAZ",
                       "ID_PARAMETRO",
                       "ID_ELABORAZIONE"),
                values=c(paste("'",format(DR$first.time.day,"%Y-%m-%d"),"'",sep=""),
                         DR$id.staz,
                         id.param, 
                         ide),
                verbose=verbose)
    dbCommit(con)
    
    dbqa.insert_elab(id_elab=ide,
                     v_elab=DR$daily.report$cumul.ave8h.nexc,
                     type="I",
                     ts1=date4db(DR$first.time.year),
                     ts2=date4db(DR$last.time),
                     n_dati=DR$daily.report$cumul.ave8h.nValid,
                     flg_elab = 0,
                     ...)
  }
  
  ## inserisce elaborazioni di evento floating (AL MOMENTO NON SONO RICHIESTE/GESTITE)
  #   if(!is.null(unlist(DR$events))){
  # 
  #   }
  dbCommit(con)

  }


