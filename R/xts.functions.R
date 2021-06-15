## converte i dati come escono dal DBQA in un oggetto xts
dbqa.data2xts <- function(data,
                          Date="TS_INIZIO_RIL",
                          Value="VALORE",
                          TZ="Africa/Algiers") {
  out <- xts(data[[Value]], as.POSIXct(data[[Date]], tz=TZ), tzone=TZ) ## sembra sia l'unico modo per codificare UTC+1 senza DST
  return(out)
}


## regolarizza una serie temporale xts,
## a passi orari o giornalieri
xts.regolarize <- function(tstep, x,
                           f.time=(i<-index(x))[1],
                           l.time=i[length(i)],
                           TZ="Africa/Algiers") {
  ## controlla lo step
  if(!(tstep%in%c("d","H"))) stop(paste("Invalid timestep value, only `d` or `H` allowed"))
  
  ## costruisce la sequenza temporale regolare
  timestring <- paste(format(f.time,"%Y-%m-%d %H:%M"),
                      format(l.time,"%Y-%m-%d %H:%M"),
                      tstep,sep="/")
  Sys.setenv(TZ=TZ)
  s0 <- timeBasedSeq(timestring)
  
  ## costruisce un oggetto xts vuoto regolare
  x0 <- xts(rep(NA,length(s0)), s0, tzone=TZ)
  
  ## sincronizza l'oggetto xts con quello regolare vuoto
  xtmp <- merge.xts(x0,x,join="left", tzone=TZ)
  xreg <- xts(xtmp$x, index(xtmp), tzone=TZ)
  
  return(xreg)
}

## unisce molte serie temporali in una sola,
## regolarizzandole a passi orari o giornalieri
xts.blend <- function(tstep, TZ="Africa/Algiers", ...) {
  ## controlla lo step
  if(!(tstep%in%c("d","H"))) stop(paste("Invalid timestep value, only `d` or `H` allowed"))
  
  ## mette le serie temporali in un solo oggetto xts complessivo
  Xm <- merge.xts(..., tzone=TZ)
  
  ## costruisce la sequenza temporale regolare
  idx.time <- index(Xm)
  f.time <- idx.time[1]
  l.time <- idx.time[length(idx.time)]
  timestring <- paste(format(f.time,"%Y-%m-%d %H:%M"),
                      format(l.time,"%Y-%m-%d %H:%M"),
                      tstep,sep="/")
  s0 <- timeBasedSeq(timestring)
  
  ## costruisce un oggetto xts vuoto regolare
  x0 <- xts(rep(NA,length(s0)), s0, tzone=TZ)
  
  ## sincronizza l'oggetto xts complessivo con quello regolare vuoto
  Xm <- merge.xts(x0,Xm,join="left", tzone=TZ)
  
  ## tiene un solo valore per ogni passo temporale
  ## (se ne trova 2 o più restituisce mancante)
  alone <- function(x) {
    ifelse(test = sum(!is.na(x))==1,
           yes  = max(x,na.rm=T),
           no   = NA)
  }
  dm <- apply(Xm,MARGIN=1,FUN=alone)
  #dm[is.infinite(dm)] <- NA
  #dm[is.nan(dm)] <- NA
  xm <- xts(dm, s0)
  
  return(xm)
}
