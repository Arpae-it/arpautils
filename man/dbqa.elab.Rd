\name{dbqa.elab}
\alias{dbqa.get.elab}
\alias{dbqa.descr.elab}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Funzioni per estrarre le elaborazioni sintetiche archiviate sul DB
}
\description{
Funzioni per estrarre le elaborazioni sintetiche archiviate sul DB: \code{dbqa.get.elab} estrae i valori, \code{dbqa.descr.elab} ne visualizza la descrizione.
}
\usage{
dbqa.get.elab(con, year, id.param, id.elab, 
              type.elab = "F", only.rrqa = T, 
              only.valid = T, keep.all = F)
dbqa.descr.elab(con, id.elab=NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
connessione al DB
}
  \item{year}{
anno
}
  \item{id.param}{
codice numerico identificativo del parametro
}
  \item{id.elab}{
codice numerico identificativo dell'elaborazione. In \code{dbqa.descr.elab} può essere \code{NULL} (è il default) per vedere la denominazione di tutte le elaborazioni definite (molte delle quali non sono però calcolate).
}
  \item{type.elab}{
tipo di elaborazione:
\itemize{
\item "I" se numero intero (p.es. numero di superamenti)
\item "F" se numero reale (p.es. media annua)
}
}
  \item{only.rrqa}{
variabile logica, se \code{TRUE} tiene solo stazioni RRQA
}
  \item{only.valid}{
variabile logica, se \code{TRUE} tiene solo elaborazioni valide
}
  \item{keep.all}{
variabile logica, se \code{TRUE} estrae tutti i campi della tabella, come sono nel DB
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
