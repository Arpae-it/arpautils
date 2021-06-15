\name{dbqa.get.datasens}
\alias{dbqa.get.datasens}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Estrae i dati di un sensore 
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Estrae i dati di un sensore 
}
\usage{
dbqa.get.datasens(con, ts.range, id.cfgsens, id.param, 
                  flg = 1, flg.excl=NULL, flg.null=FALSE, 
                  verbose=FALSE, table="storico",...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{con} here~~
connessione al DB
}
  \item{ts.range}{
%%     ~~Describe \code{ts.range} here~~
periodo di interesse, definito come vettore \code{c(datainizio,datafine)}
}
  \item{id.cfgsens}{
%%     ~~Describe \code{id.cfgsens} here~~
codice numerico di configurazione del sensore
}
  \item{id.param}{
%%     ~~Describe \code{id.param} here~~
codice numerico dell'inquinante
}
  \item{flg}{
%%     ~~Describe \code{flg} here~~
valore (o valori) accettati per la flag di qualita' FLG_A. Se \code{NULL} non viene posta alcuna condizione includente sulla flag
}
  \item{flg.excl}{
%%     ~~Describe \code{flg} here~~
valore (o valori) non accettati per la flag di qualita' FLG_A. Se \code{NULL} non viene posta alcuna condizione escludente sulla flag
}
  \item{flg.null}{
%%     ~~Describe \code{flg} here~~
se \code{TRUE} vengono estratti anche record con valori di flag FLG_4 pari a 'NULL' nel DB, cioè non ancora sottoposti a validazione quotidiana
}
  \item{verbose}{
%%     ~~Describe \code{flg} here~~
parametro logico. Se \code{TRUE}, verranno visualizzate informazioni utili per il debug
}
  \item{table}{
%%     ~~Describe \code{flg} here~~
stringa che definisce la tabella da cui estrarre i dati. Al momento implementate le opzioni ``storico'' e ``annuale''.
}
  \item{\dots}{
%%     ~~Describe \code{\dots} here~~
argomenti opzionali, passati a \code{dbSendQuery}
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
}
