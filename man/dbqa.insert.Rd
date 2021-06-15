\name{dbqa.insert}
\alias{dbqa.insert}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Inserisce valori in una tabella del DB.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Inserisce valori in una tabella del DB.
}
\usage{
dbqa.insert(con, tab, values, columns = colnames(values), 
            to_date=NULL, update = F, verbose = F)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{con} here~~
connessione al DB
}
  \item{tab}{
%%     ~~Describe \code{tab} here~~
nome della tabella
}
  \item{values}{
%%     ~~Describe \code{values} here~~
vettore o \code{data.frame} contenente i valori da inserire in tabella
}
  \item{columns}{
%%     ~~Describe \code{columns} here~~
nomi dei campi da scrivere
}
  \item{to_date}{
%%     ~~Describe \code{columns} here~~
vettore numerico che identifica le colonne che sono carattere e in formato SQL 'YYYY-MM-DD HH24:MI', da convertire in tipo DATE di Oracle, attraverso la funzione TO_DATE
}
  \item{update}{
%%     ~~Describe \code{update} here~~
se \code{TRUE} aggiorna record esistenti
}
  \item{verbose}{
%%     ~~Describe \code{verbose} here~~
se \code{TRUE} fornisce informazioni utili al debug
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
\author{
%%  ~~who you are~~
Giovanni Bonafè
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
