\name{dbqa.data2xts}
\alias{dbqa.data2xts}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Converte i dati estratti da DB in un oggetto di tipo \code{xts}
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Converte i dati estratti da DB in un oggetto di tipo \code{xts}
}
\usage{
dbqa.data2xts(data, Date = "TS_INIZIO_RIL", Value = "VALORE", TZ="Africa/Algiers")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
%%     ~~Describe \code{data} here~~
matrice o dataframe che include i dati, cosi' come sono estratti dal DB
}
  \item{Date}{
%%     ~~Describe \code{Date} here~~
il nome del campo che contiene data (e ora) in \code{data}
}
  \item{Value}{
%%     ~~Describe \code{Value} here~~
il nome del campo che contiene le concentrazioni in \code{data}
}
  \item{TZ}{
%%     ~~Describe \code{Value} here~~
timezone (vedi Warning)
}
}
\section{Warning}{
  Si fa riferimento all'ora ``Africa/Algiers'' poiche' e' l'unico standard codificato corrispondente all'ora del DB, nonche' all'ora a cui si riferisce la normativa: Central Europe Time senza Daylight Saving Time (ora legale).
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
Giovanni Bonafè, Arpa Emilia-Romagna
}
