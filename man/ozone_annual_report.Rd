\name{ozone_annual_report}
\alias{prepare.ozone_annual_report}
\alias{calculate.ozone_annual_report}
\alias{write.ozone_annual_report}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Funzioni per produrre le statistiche annuali per l'ozono (una stazione)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Funzioni per produrre le statistiche annuali per una singola stazione di ozono: estrazione, calcoli, scrittura su DB.
}
\usage{
prepare.ozone_annual_report(con, id.staz, year=NULL, ...)
calculate.ozone_annual_report(data)
write.ozone_annual_report(con, OAR, verbose=F, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
identificativo della connessione al DB (stringa)
}
  \item{id.staz}{
codice numerico identificativo della stazione
}
  \item{year}{
anno per cui si richiede il report. Se lasciata \code{NULL} prende l'anno di 5 mesi fa, facendo riferimento a \code{Sys.Date()}.
}
  \item{data}{
dati estratti da \code{prepare.ozone_annual_report}
}
  \item{OAR}{
lista di \code{data.frame} prodotta da \code{calculate.ozone_annual_report} (vedi Value)
}
  \item{verbose}{
scrive a video alcune informazioni utili per il debug
}
  \item{\dots}{
parametri opzionali. La funzione \code{prepare.ozone_annual_report} li passa a \code{dbqa.get.datastaz}; la funzione \code{write.ozone_annual_report} li passa a \code{dbqa.insert}.
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
La funzione \code{calculate.ozone_annual_report} restituisce una lista di due oggetti:

\code{annual.report} è un \code{data.frame} di un sola riga con le seguenti colonne:
\item{cumul.nexc.180 }{numero di superamenti orari della soglia 180}
\item{cumul.nexc.240 }{numero di superamenti orari della soglia 240}
\item{cumul.nexc.120 }{numero di superamenti di \code{max.ave.8h} della soglia 120}
\item{aot40.veget }{AOT40 per la vegetazione}
\item{aot40.veget.PercValid }{percentuale di dati validi per il calcolo dell'AOT40 per la vegetazione}
\item{aot40.forest }{AOT40 per le foreste}
\item{aot40.forest.PercValid }{percentuale di dati validi per il calcolo dell'AOT40 per le foreste}

\code{events} è una lista contenente i due data.frame, ciascuno dei quali avente colonne \code{start.time} e \code{duration}:
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
calculate.ozone_annual_report(data=dat)
dbDisconnect(con1)
}
}
