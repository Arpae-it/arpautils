## credenziali di accesso al DB prendile dal file credenziali.txt
db_usr = ""
db_pwd = ""
db_name = ""  
ldebug=FALSE


## funzione di help
script <- "OGDR.R"
usage <- function() {
  stop(paste("\n",
             "Uso: Rscript ",script," --args PROV INQ [DATA]\n",
             "Calcola le statistiche giornaliere QA\n",
             "\n",
             "  PROV    codice della provincia (PC/PR/RE/MO/BO/FE/RA/FC/RN)\n",
             "  INQ     codice dell'inquinante:\n",
             "          1=SO2, 20=benzene, 10=CO, 5=PM10, 8=NO2, 111=PM2.5, 7=O3\n",
             "  DATA    data in formato YYYY-MM-DD (default: ieri)\n",
             "\n",
             sep=""))
}

## gestisce argomenti
if(ldebug) {
  #__________________________________________
  args=c(3,"PR",8,"2021-05-28") #il primo argomento non serve
  print("***debug - INSERISCO I PARAMETRI A MANO :")
  print(args)
  #__________________________________________
} else {
 args <- commandArgs(trailingOnly = TRUE)
 if(length(args)>2) {
   Prov <- args[2]
   id.param <- args[3]
 } else {
   usage()
 }}

if(length(args)>2) {
  Prov <- args[2]
  id.param <- args[3]
} else {
  usage()
}
if(!Prov %in% c("PC","PR","RE","MO","BO","FE","RA","FC","RN")) {
  usage()
}
if(length(args)>3) {
  day <- args[4]
} else {
  day <- format(Sys.Date()-1,format="%Y-%m-%d")
}


## controlla se c'e' un'altra istanza aperta
filelock <- paste("OGDR_",Prov,".lock",sep="")
fileprof <- paste("OGDR_",Prov,"_profiling.csv",sep="")
lock <- file.exists(filelock)
if(lock && !ldebug) {
  stop(paste(script,"already in use"))
} else {
  file.create(filelock)
}

## calcola e scrive elaborazioni per statistiche annuali e bollettini
library(arpautils)
if(ldebug){
 source("R/generic_daily_report.R")
 source("R/ozone_daily_report.R")
 source("R/dbqa.functions.R")
 source("R/time.functions.R")
 source("R/xts.functions.R")
 source("R/aqstat.functions.R")
}

sessionInfo()->SI
#if(ldebug) print(SI)
tt <- NULL
incr.sys.time <- function(tab.in,name,expr) { #serve per contare il tempo che impiega
  t1 <- system.time(expr)
  tt <- rbind(tab.in,t1)
  rownames(tt)[nrow(tt)] <- name
  return(tt)
}
tt <- incr.sys.time(tt,"connect",{    
  cfg <- dbqa.config(db_usr, db_pwd, db_name)#configura con user del database, utente e database
  con <- dbqa.connect(db_usr, db_pwd, db_name) #si connette al database
})
## debug ---
if(ldebug) print(paste("***debug",Prov,as.POSIXct(day)))
#if(ldebug) {print("***debug"); print(con);}
## debug ---

tt <- incr.sys.time(tt,"list",{ #calcola il tempo che impiega
  SSS <- dbqa.list.active.staz_mobil(con,Prov,as.POSIXct(day)) 
      #legge sul database quali sono le province attive oggi
})

## debug inizio
if (ldebug) SSS<-c(2000219) 
## debug fine

for (staz in SSS) {
  if(exists("Dat")) rm(Dat) #Dat e' un oggetto  d'appoggio che, se gia' esistente, va cancellato per garantire che sia pulita
  tt <- incr.sys.time(tt,paste("prepare",staz),{
    if(id.param %in% c(5,111)) {#5=PM10
      tstep="d"
    } else {
      tstep="H"
    }
    if(id.param == 7) { #ozono
	   Dat <- prepare.ozone_daily_report(con,id.staz=staz,Date=day)	#scarica i dati dell'ozono con questa appostia procedura
    } else {
 	   Dat <- prepare.daily_report(con,id.staz=staz,id.param=id.param,Date=day,tstep=tstep) #altrimenti con una generica
	}	
  })
  if(exists("Dat") && !is.null(Dat) && !is.null(Dat$Dat) && sum(!is.na(Dat$Dat))>0)  {
    tt <- incr.sys.time(tt,paste("calculate",staz),{
      ## SOGLIE
      ## default
      thr.daily.ave=NULL
      thr.ave8h.max=NULL
      thr.hourly=NULL
      ## SO2
      if(id.param==1) {
        thr.daily.ave=125
        thr.hourly=350
      }
      ## PM10
      if(id.param==5) {
        thr.daily.ave=50
      }
      ## NO2
      if(id.param==8) {
        thr.hourly=200
      }
      ## CO
      if(id.param==10) {
        thr.ave8h.max=10
      }
      if(id.param == 7) { #ozono
	      ODR <- calculate.ozone_daily_report(Dat) #"ODR" = Ozone dAily Report
	    }else{
	      #print("***debug")
	      #if(ldebug) print(paste(id.param, thr.daily.ave, thr.ave8h.max, thr.hourly))
	      DR <- calculate.daily_report(data=Dat,id.param=id.param, #"DR" =dAily Report
                                     thr.daily.ave=thr.daily.ave,
                                     thr.ave8h.max=thr.ave8h.max,
                                     thr.hourly=thr.hourly)
	    }
    })
    tt <- incr.sys.time(tt,paste("write",staz),{
    if(id.param == 7) { #ozono	
      try(write.ozone_daily_report(con,ODR,verbose=F))
	  }else{
	    verbose<-F
	    if(ldebug) {
	      verbose<-T
	      #DR$daily.report$mean.day<-3
	      #DR$daily.report$max.day<-3
	      #DR$daily.report$hour.max.day<-3
	      #DR$daily.report$cumul.hourly.nexc<-3
	    }
      try(write.daily_report(con,DR,verbose=verbose,id.param=id.param))
## debug 
if(ldebug) print(DR)
## debug 
	  }
   })
  }  
}
dbDisconnect(con)
#write.csv(signif(tt,3),file=fileprof)
file.remove(filelock)


