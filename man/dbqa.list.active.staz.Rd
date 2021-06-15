\name{dbqa.list.active.staz}
\alias{dbqa.list.active.staz}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Restituisce la lista delle stazioni attive.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Restituisce la lista delle stazioni attive in una provincia, in una data
}
\usage{
dbqa.list.active.staz(con, prov=c("PC","PR","RE","MO","BO","FE","RA","FC","RN"),
                      Day = Sys.Date(), mobile=FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{con} here~~
connessione al DB
}
  \item{prov}{
%%     ~~Describe \code{prov} here~~
sigla della provincia (può essere un vettore)
}
  \item{Day}{
%%     ~~Describe \code{Day} here~~
data richiesta, in formato POSIX
}
  \item{mobile}{
%%     ~~Describe \code{Day} here~~
se \code{TRUE}, include anche i laboratori mobili
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
Restituisce un vettore di tipo carattere, contenente i codici delle stazioni attive per la data e le province richieste
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
\examples{
}
