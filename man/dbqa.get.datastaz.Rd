\name{dbqa.get.datastaz}
\alias{dbqa.get.datastaz}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Estrae i dati di una stazione per un dato parametro (inquinante)
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Estrae i dati di una stazione per un dato parametro (inquinante)
}
\usage{
dbqa.get.datastaz(con, ts.range, id.staz, id.param,
                  flg=1, tstep, 
                  lod.manage=ifelse({id.param \%in\% c(12,14,15,18,29)},
                  "half", "keep"),...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
connessione al DB
}
  \item{ts.range}{
periodo di interesse, definito come vettore \code{c(datainizio,datafine)}
}
  \item{id.staz}{
codice numerico identificativo della stazione
}
  \item{id.param}{
codice numerico dell'inquinante
}
  \item{flg}{
valore (o valori) accettati per la flag di qualita'
}
  \item{tstep}{
stringa di carattere che identifica il timestep:
\itemize{
  \item "H" orario
  \item "d" giornaliero
}
}
  \item{lod.manage}{
stringa di carattere che specifica come devono essere gestiti i valori inferiori al Limit Of Detection:
\itemize{
  \item "half" sostituisce con LOD/2 (è il default per benzoapirene e alcuni metalli)
  \item "keep" tiene il valore osservato (è il default per gli altri parametri)
}
}
  \item{\dots}{
%%     ~~Describe \code{\dots} here~~
argomenti opzionali, passati a \code{dbqa.get.datasens}
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

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{dbqa.get.datasens}}, \code{\link{dbqa.lod}}
}
