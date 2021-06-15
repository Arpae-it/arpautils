## credenziali di accesso al DB
db_usr = "AA_USER"
db_pwd = "USER_AA"
db_name = "//192.168.41.110:1521/rmqac"  

## funzione di help
#script <- "OGDR.R"
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
# args <- commandArgs(trailingOnly = TRUE)
# if(length(args)>2) {
#   Prov <- args[2]
#   id.param <- args[3]
# } else {
#   usage()
# }
# 
# 
# if(!Prov %in% c("PC","PR","RE","MO","BO","FE","RA","FC","RN")) {
#   usage()
# }

#INSERISCO I PARAMETRI A MANO ______________
#id.param<-111
#Prov<-'BO'
args=c(3,"BO",8,"2020-07-09") #il primo argomento non serve
#__________________________________________
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
if(lock) {
  stop(paste(script,"already in use"))
} else {
  file.create(filelock)
}

## calcola e scrive elaborazioni per statistiche annuali e bollettini
library(arpautils)
sessionInfo()->SI
print(SI)
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
tt <- incr.sys.time(tt,"list",{ #calcola il tempo che impiega
  SSS <- dbqa.list.active.staz_mobil(con,Prov,as.POSIXct(day)) 
      #legge sul database quali sono le province attive oggi
})
#staz<-7000024
for (staz in SSS) {
  if(exists("Dat")) rm(Dat) #Dat ? un oggetto  d'appoggio che, se gi? esistente, va cancellato per garantire che sia pulita
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
	      DR <- calculate.daily_report(data=Dat,id.param=id.param, #"DR" =dAily Report
                                    thr.daily.ave=thr.daily.ave,
                                    thr.ave8h.max=thr.ave8h.max,
                                    thr.hourly=thr.hourly)
	}

    })
    tt <- incr.sys.time(tt,paste("write",staz),{
     if(id.param == 7) { #ozono	
      try(write.ozone_daily_report(con,ODR,verbose=F))
###Debug print(ODR)
	}else{
      try(write.daily_report(con,DR,verbose=F,id.param=id.param))
###Debug print(DR)
	}
    })
  }  
}
dbDisconnect(con)
#write.csv(signif(tt,3),file=fileprof)
file.remove(filelock)


