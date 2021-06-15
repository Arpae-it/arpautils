\name{dbqa.view.staz}
\alias{dbqa.view.staz}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Elenca le stazioni QA.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Elenca le stazioni, applicando la query 
\code{select distinct ID_STAZIONE, NOME_STAZIONE, COMUNE, PROVINCIA
from AA_ARIA.ANG_CONFIG_SENSORI}
}
\usage{
dbqa.view.staz(con, FUN = View)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{con}{
%%     ~~Describe \code{con} here~~
connessione al DB
}
  \item{FUN}{
azione da applicare per la visualizzazione della matrice (usare \code{return} se si vuole avere la matrice come output, vedi Examples)
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
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
\dontrun{
db_usr="yourUsername"
db_pwd="yourPassword"
db_name="DBaddress"
con <- dbqa.connect(db_usr, db_pwd, db_name)
dbqa.view.staz(con)

## ...oppure equivalente
Dat <- dbqa.view.staz(con, FUN=return)
View(Dat)
dbDisconnect(con)
}
}
