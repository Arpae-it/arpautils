\name{ozone_daily_report}
\alias{prepare.ozone_daily_report}
\alias{calculate.ozone_daily_report}
\alias{write.ozone_daily_report}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Funzioni per produrre i bollettini quotidiani per l'ozono (una stazione)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Funzioni per produrre i bollettini quotidiani per una singola stazione di ozono: estrazione, calcoli, scrittura su DB.
}
\usage{
prepare.ozone_daily_report(con, id.staz, Date=NULL, ...)
calculate.ozone_daily_report(data)
write.ozone_daily_report(con, ODR, empty=F, verbose=F, ...)
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
dati estratti da \code{prepare.ozone_daily_report}
}
  \item{ODR}{
lista di \code{data.frame} prodotta da \code{calculate.ozone_daily_report} (vedi Value)
}
  \item{empty}{
svuota tutta la tabella del DB prima di scriverci
}
  \item{verbose}{
scrive a video alcune informazioni utili per il debug
}
  \item{\dots}{
parametri opzionali. La funzione \code{prepare.ozone_daily_report} li passa a \code{dbqa.get.datastaz}; la funzione \code{write.ozone_daily_report} li passa a \code{dbqa.insert}.
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
La funzione \code{calculate.ozone_daily_report} restituisce una lista di due oggetti:

\code{daily.report} ? un \code{data.frame} di un sola riga con le seguenti colonne:
\item{max.day }{massimo giornaliero}
\item{hour.max.day }{ora in cui si e' verificato il massimo (inizio dell'intervallo orario)}
\item{max.ave.8h }{massimi giornaliero della media mobile su 8 ore}
\item{cumul.nexc.180 }{numero di superamenti orari della soglia 180, dall'inizio dell'anno}
\item{cumul.nexc.240 }{numero di superamenti orari della soglia 240, dall'inizio dell'anno}
\item{cumul.nexc.120 }{numero di superamenti di \code{max.ave.8h} della soglia 120, dall'inizio dell'anno}

\code{events} ? una lista contenente i due data.frame, ciascuno dei quali avente colonne \code{start.time} e \code{duration}:
\item{exc.180}{contenente gli eventi orari di superamento della soglia 180}
\item{exc.240}{contenente gli eventi orari di superamento della soglia 240}

}
\references{
%% ~put references to the literature/web site here ~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~
\section{Warning }{
 Usare l'opzione \code{empty=TRUE} solo se strettamente necessario: svuota tutta la tabella delle statistiche giornaliere dell'ozono sul DB.
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
\dontrun{
## report di ieri per Cittadella
db_usr="yourUsername"
db_pwd="yourPassword"
db_name="DBaddress"
day <- format(Sys.Date()-1,format='\%Y-\%m-\%d')
con <- dbqa.connect(db_usr, db_pwd, db_name)
Dat <- prepare.ozone_daily_report(con,id.staz="2000003",Date=day)
ODR <- calculate.ozone_daily_report(Dat)
dbDisconnect(con)

}
}
