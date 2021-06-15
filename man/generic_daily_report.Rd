\name{generic_daily_report}
\alias{prepare.daily_report}
\alias{calculate.daily_report}
\alias{write.daily_report}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Funzioni per produrre i bollettini quotidiani per un inquinante generico (una stazione)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Funzioni per produrre i bollettini quotidiani per una singola stazione per NO2, C6H6, SO2, CO, PM10: estrazione, calcoli, scrittura su DB.
}
\usage{
prepare.daily_report(con, id.staz, id.param,
                       Date=NULL, tstep, ...)
calculate.daily_report(data, id.param,
                       thr.daily.ave=NULL,
                       thr.ave8h.max=NULL,
                       thr.hourly=NULL,
                       thr.multihourly=NULL,
                       NH=3)
write.daily_report(con, DR, id.param,
                       verbose=F, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
identificativo della connessione al DB (stringa)
}
  \item{id.staz}{
codice numerico identificativo della stazione
}
  \item{Date}{
data per cui si richiede il report. Se lasciata \code{NULL} prende la giornata di ieri, facendo riferimento a \code{Sys.Date()}.
}
  \item{data}{
dati estratti da \code{prepare.daily_report}
}
  \item{DR}{
lista di \code{data.frame} prodotta da \code{calculate.daily_report} (vedi Value)
}
  \item{verbose}{
scrive a video alcune informazioni utili per il debug
}
  \item{\dots}{
parametri opzionali. La funzione \code{prepare.daily_report} li passa a \code{dbqa.get.datastaz}; la funzione \code{write.daily_report} li passa a \code{dbqa.insert}.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
La funzione \code{calculate.daily_report} restituisce una lista di due oggetti:

\code{daily.report} è un \code{data.frame} di un sola riga con le seguenti colonne (NB i superamenti sono calcolati solo se in input è fornita la corrispondente soglia):
\item{mean.day}{media giornaliera}
\item{max.day}{massima giornaliera}
\item{hour.max.day}{ora del massimo}
\item{max.ave.8h}{massimo della media su 8 ore}
\item{nexc.hourly}{numero di superamenti orari nella giornata}
\item{hourly.nValid}{numero di dati orari validi nella giornata}
\item{cumul.daily.nexc}{numero di superamenti della media giornaliera dall'inizio dell'anno}
\item{cumul.daily.nValid}{numero di dati validi della media giornaliera dall'inizio dell'anno}
\item{cumul.ave8h.nexc}{numero di superamenti del massimo della media su 8 ore dall'inizio dell'anno}
\item{cumul.ave8h.yave}{media del massimo della media su 8 ore dall'inizio dell'anno}
\item{cumul.ave8h.nValid}{numero di massimi giornalieri della media su 8 ore validi dall'inizio dell'anno}
\item{cumul.hourly.nValid}{numero di dati orari validi dall'inizio dell'anno}
\item{cumul.hourly.nexc}{numero di superamenti orari dall'inizio dell'anno}

\code{events} è una lista contenente un data.frame avente colonne \code{start.time} e \code{duration}, contenente gli eventi orari di superamento della soglia 

}
\references{
%% ~put references to the literature/web site here ~
}
\note{
%%  ~~further notes~~
}

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
\dontrun{
## report di ieri per Cittadella
day <- format(Sys.Date()-1,format='\%Y-\%m-\%d')
con <- dbqa.connect()
Dat <- prepare.daily_report(con,id.staz="2000003",Date=day)
ODR <- calculate.daily_report(Dat)
dbDisconnect(con)

}
}
