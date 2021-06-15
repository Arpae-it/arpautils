## credenziali di accesso al DB
db_usr = "AA_WEB"
db_pwd = "WEB_AA"
db_name = "//192.168.41.110:1521/rmqac"  

## funzione di help
script <- "GDR.R"
usage <- function() {
  stop(paste("\n",
             "Uso: Rscript ",script," --args PROV INQ [DATA]\n",
             "Calcola le statistiche giornaliere QA\n",
             "\n",
             "  PROV    codice della provincia (PC/PR/RE/MO/BO/FE/RA/FC/RN)\n",
             "  INQ     codice dell'inquinante:\n",
             "          1=SO2, 20=benzene, 10=CO, 5=PM10, 8=NO2\n",
             "  DATA    data in formato YYYY-MM-DD (default: ieri)\n",
             "\n",
             sep=""))
}

## gestisce argomenti
args <- commandArgs(trailingOnly = TRUE)
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
filelock <- paste("GDR_",Prov,".lock",sep="")
fileprof <- paste("GDR_",Prov,"_profiling.csv",sep="")
lock <- file.exists(filelock)
if(lock) {
  stop(paste(script,"already in use"))
} else {
  file.create(filelock)
}

## calcola e scrive elaborazioni per statistiche annuali
library(arpautils)
sessionInfo()->SI
print(SI)
tt <- NULL
incr.sys.time <- function(tab.in,name,expr) {
  t1 <- system.time(expr)
  tt <- rbind(tab.in,t1)
  rownames(tt)[nrow(tt)] <- name
  return(tt)
}
tt <- incr.sys.time(tt,"connect",{
  cfg <- dbqa.config(db_usr, db_pwd, db_name)
  con <- dbqa.connect(db_usr, db_pwd, db_name)
})
tt <- incr.sys.time(tt,"list",{
  SSS <- dbqa.list.active.staz(con,Prov,as.POSIXct(day))
})
for (staz in SSS) {
  print(paste("Staz",staz))
  if(exists("Dat")) rm(Dat)
  tt <- incr.sys.time(tt,paste("prepare",staz),{
    if(id.param %in% c(5,111)) {
      tstep="d"
    } else {
      tstep="H"
    }
    Dat <- prepare.daily_report(con,id.staz=staz,id.param=id.param,Date=day,tstep=tstep)
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
      DR <- calculate.daily_report(data=Dat,id.param=id.param,
                                    thr.daily.ave=thr.daily.ave,
                                    thr.ave8h.max=thr.ave8h.max,
                                    thr.hourly=thr.hourly)
      print(paste("__________Dat ",Dat))
      print(str(DR))
    })
    ##tt <- incr.sys.time(tt,paste("write",staz),{
      ##try(write.daily_report(con,DR,verbose=F,id.param=id.param))
    ##})
  }  
}
dbDisconnect(con)
write.csv(signif(tt,3),file=fileprof)
file.remove(filelock)


