\name{dbqa.get.idcfgsens}
\alias{dbqa.get.idcfgsens}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Estrae l'elenco dei codici sensore corrispondenti ad una data stazione, per un dato periodo, per un dato parametro (inquinante)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Estrae l'elenco dei codici sensore corrispondenti ad una data stazione, per un dato periodo, per un dato parametro (inquinante)
}
\usage{
dbqa.get.idcfgsens(con, id.param, i.date = NULL, f.date = NULL, id.staz)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{con} here~~
connessione al DB
}
  \item{id.param}{
%%     ~~Describe \code{id.param} here~~
codice numerico dell'inquinante
}
  \item{i.date}{
%%     ~~Describe \code{i.date} here~~
data iniziale
}
  \item{f.date}{
%%     ~~Describe \code{f.date} here~~
data finale
}
  \item{id.staz}{
%%     ~~Describe \code{id.staz} here~~
codice numerico identificativo della stazione
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
\references{
%% ~put references to the literature/web site here ~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
