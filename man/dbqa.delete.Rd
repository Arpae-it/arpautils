\name{dbqa.delete}
\alias{dbqa.delete}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Cancella record di una tabella nel DB QA
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Cancella record di una tabella nel DB QA
}
\usage{
dbqa.delete(con, tab, keys, values, verbose = F)
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
  \item{keys}{
%%     ~~Describe \code{keys} here~~
chiavi primarie
}
  \item{values}{
%%     ~~Describe \code{values} here~~
valori delle chiavi primarie che identificano i record da rimuovere
}
  \item{verbose}{
%%     ~~Describe \code{verbose} here~~
se \code{TRUE} fornisce informazioni utili per il debug
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
\author{
%%  ~~who you are~~
Giovanni Bonafè
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

