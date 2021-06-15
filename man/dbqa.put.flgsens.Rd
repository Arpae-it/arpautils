\name{dbqa.put.flgsens}
\alias{dbqa.put.flgsens}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Scrive flag di invalidazione calcolate per un sensore
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Scrive flag di invalidazione calcolate per un sensore
}
\usage{
dbqa.put.flgsens(con,
                    id_config_sensore=NULL,
                    id_config_staz=NULL, 
                    id_parametro=NULL, 
                    ts_dato=NULL,
                    valore=NULL,
                    flg_7=NULL, 
                    flg_8=NULL, 
                    flg_9=NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{x} here~~
connessione al DB
}
  \item{id_config_sensore}{
%%     ~~Describe \code{x} here~~
codice numerico di configurazione del sensore
}
  \item{id_config_staz}{
%%     ~~Describe \code{x} here~~
codice identificativo della stazione
}
  \item{id_parametro}{
%%     ~~Describe \code{x} here~~
codice numerico dell'inquinante
}
  \item{ts_dato}{
%%     ~~Describe \code{x} here~~
time stamp del dato
}
  \item{valore}{
%%     ~~Describe \code{x} here~~
valore del dato
}
  \item{flg_7 flg_8 flg_9}{
%%     ~~Describe \code{x} here~~
 valori dei flag. Almeno uno deve essere non nullo altrimenti esce con errore 
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
numeric error code

0 return without error

-1:  id_config_sensore=NULL

-2:  id_config_staz=NULL, 

-3:  id_parametro=NULL, 

-4:  ts_dato=NULL,

-5:  valore=NULL,

-6:  flg_7=NULL & flg_8=NULL & flg_9=NULL)
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

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
