\name{generic_annual_report}
\alias{prepare.annual_report}
\alias{calculate.annual_report}
\alias{write.annual_report}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Funzioni per produrre statistiche annuali (una stazione)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Funzioni per produrre le statistiche annuali tipiche (media annua, superamenti giornalieri della media e del max della media mobile su 8h) per una singola stazione: estrazione, calcoli, scrittura su DB.
}
\usage{
prepare.annual_report(con, id.staz, id.param, year=NULL, tstep, ...)
calculate.annual_report(data, id.param, thr.daily.ave=NULL, thr.ave8h.max=NULL,
                        thr.hourly=NULL, thr.multihourly=NULL, NH=3,
                        critical.months=NULL)
write.annual_report(con, AR, id.param, verbose=F, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
identificativo della connessione al DB (stringa)
}
  \item{id.staz}{
codice numerico identificativo della stazione
}
  \item{id.param}{
codice numerico identificativo dell'inquinante
}
  \item{year}{
anno per cui si richiede il report. Se lasciato \code{NULL} prende l'anno di 5 mesi fa, facendo riferimento a \code{Sys.Date()}.
}
  \item{tstep}{
stringa di carattere che identifica il timestep ("H" o "d", orario o giornaliero)
}
  \item{data}{
dati estratti da \code{prepare.annual_report}
}
  \item{thr.daily.ave}{
soglia per la media giornaliera
}
  \item{thr.ave8h.max}{
soglia per il max giornaliero della media mobile su 8h
}
  \item{thr.hourly}{
soglia oraria
}
  \item{thr.multihourly}{
soglia per superamenti orari di più ore consecutive
}
  \item{NH}{
numero di ore consecutive di superamento da contare se \code{thr.multihourly} non ? NULL
}
  \item{critical.months}{
vettore numerico dei mesi su cui calcolare la media di periodo (p.es. \code{c(1:3,10:12)} per la media invernale)
}
  \item{AR}{
lista di \code{data.frame} prodotta da \code{calculate.annual_report} (vedi Value)
}
  \item{verbose}{
scrive a video alcune informazioni utili per il debug
}
  \item{\dots}{
parametri opzionali. La funzione \code{prepare.annual_report} li passa a \code{dbqa.get.datastaz}; la funzione \code{write.annual_report} li passa a \code{dbqa.insert}.
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
La funzione \code{calculate.annual_report} restituisce un \code{data.frame} con:
\item{annual.mean     }{media annua}
\item{annual.nValid   }{numero di dati validi usati per il calcolo della media annua}
\item{annual.percValid}{percentuale di dati validi usati per il calcolo della media annua}
\item{annual.nExpected}{numero di dati attesi nell'anno}
\item{annual.efficiency}{rapporto tra numero di dati disponibili e attesi nell'anno}

se \code{thr.daily.ave} non ? \code{NULL} allora \code{data.frame} ha anche:
\item{daily.nexc}{superamenti della media giornaliera}
\item{daily.nValid}{numero di dati validi usati per il calcolo dei superamenti della media giornaliera}
\item{daily.percValid}{percentuale di dati validi usati per il calcolo dei superamenti della media giornaliera}

se \code{thr.ave8h.max} non ? \code{NULL} allora \code{data.frame} ha anche:
\item{ave8h.nexc}{superamenti del max giornaliero della media mobile su 8h}
\item{ave8h.nValid}{numero di dati validi usati per il calcolo dei superamenti del max giornaliero della media mobile su 8h}
\item{ave8h.percValid}{percentuale di dati validi usati per il calcolo dei superamenti del max giornaliero della media mobile su 8h}

se \code{thr.hourly} o \code{thr.multihourly} non sono \code{NULL} allora \code{data.frame} ha anche:
\item{hourly.nValid}{numero di dati orari validi}
\item{hourly.percValid}{percentuale di dati orari validi}

se \code{thr.hourly} non ? \code{NULL} allora \code{data.frame} ha anche:
\item{hourly.nexc}{numero di superamenti orari}

se \code{thr.multihourly} non ? \code{NULL} allora \code{data.frame} ha anche:
\item{multihourly.nexc}{numero di superamenti orari di almeno NH ore consecutive}

se \code{critical.months} non ? \code{NULL} allora \code{data.frame} ha anche:
\item{critmonths.mean     }{media dei mesi selezionati}
\item{critmonths.nValid   }{numero di dati validi nei mesi selezionati}
\item{critmonths.percValid}{percentuale di dati validi nei mesi selezionati}
\item{critmonths.nExpected}{numero di dati attesi nei mesi selezionati}
\item{critmonths.efficiency}{rapporto tra numero di dati disponibili e attesi nei mesi selezionati}

}
\references{
%% ~put references to the literature/web site here ~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~
%\section{Warning }{
% Usare l'opzione \code{empty=TRUE} solo se strettamente necessario: svuota tutta la tabella ??? dell'ozono sul DB.
%}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
\dontrun{
db_usr="yourUsername"
db_pwd="yourPassword"
db_name="DBaddress"
con1 <- dbqa.connect(db_usr, db_pwd, db_name)
dat <- prepare.annual_report(con=con1, id.staz="2000003")
calculate.annual_report(data=dat)
dbDisconnect(con1)
}
}
