\name{dbqa.get.idparam}
\alias{dbqa.get.idparam}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Restituisce l'ID numerico di un parametro
}
\description{
Restituisce l'ID numerico di un parametro (inquinante)
}
\usage{
dbqa.get.idparam(poll, con = NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{poll}{
nome o sigla del parametro (stringa)
}
  \item{con}{
connessione
}
}
\details{
Prima cerca in una lista di poche sigle usate di frequente. Se non trova corrispondenza, cerca una stringa simile tra i nomi nel DB.
}
\value{
ID numerico del parametro (inquinante). Se la stringa è ambigua o priva di corrispondenze, restituisce \code{NULL}, ma dà qualche messaggio utile.
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
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
