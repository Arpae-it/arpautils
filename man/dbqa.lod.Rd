\name{dbqa.lod}
\alias{dbqa.lod}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
LOD
}
\description{
Restituisce il limite di rilevabilità (LOD = Limit Of Detection).
}
\usage{
dbqa.lod(con, id.param, days = Sys.Date())
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
connessione al DB
}
  \item{id.param}{
codice numerico identificativo del parametro (inquinante)
}
  \item{days}{
data o vettore di date , nel formato POSIXct o Date o come stringa "YYYY-MM-DD"
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
Vettore numerico dei valori del LOD per ciascuna data richiesta
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
