\name{dbqa.get.stat.param}
\alias{dbqa.get.stat.param}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Legge dal dbqa i parametri statistici per la validazione automatica dei dati
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Legge dal dbqa i parametri statistici per la validazione automatica dei dati

}
\usage{
dbqa.get.stat.param(con,id_config_staz=NULL,id_parametro=NULL,ts_dato=NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
 
  \item{con}{
%%     ~~Describe \code{x} here~~
connessione al dbqa
}
  \item{id_config_staz}{
%%     ~~Describe \code{x} here~~
 codice stazione
}
 \item{id_parametro}{
%%     ~~Describe \code{x} here~~
codice numerico dell'inquinante
}
  \item{ts_dato}{
%%     ~~Describe \code{x} here~~
time stamp del dato (giornaliero)
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
restituisce una lista con i parametri statisici

  \item{MIN_FLG_8}{Valore minimo stagionale FLG_8}
  \item{MAX_FLG_8}{Valore massimo stagionale FLG_8}
  \item{MIN_FLG_9}{Valore scostamento minimo FLG_9}
  \item{MAX_FLG_9}{Valore scostamento massimo FLG_9}
  \item{A_FLG_9}{Valore coefficiente angolare retta FLG_9}
  \item{B_FLG_9}{Valore intercetta retta FLG_9}
  
restituisce un codice errore numerico se passati nulli i parametri di ingresso

-2:  id_config_staz=NULL 

-3:  id_parametro=NULL

-4:  ts_dato=NULL

  
}

\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}

\examples{
con<-dbqa.connect()
param<-dbqa.get.stat.param(con,id_config_staz=5000066,id_parametro=5,ts_dato="2018-10-24")
param

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
