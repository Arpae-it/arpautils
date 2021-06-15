\name{dbqa.round}
\alias{dbqa.round}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
arrotonda concentrazioni
}
\description{
arrotonda concentrazioni in visualizzazione, secondo le indicazioni del Gruppo di Lavoro (Istruzione Operativa)
}
\usage{
dbqa.round(x, id.param)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
  valore da arrotondare
}
  \item{id.param}{
  identificativo del parametro nel DB
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
# concentrazioni casuali tra 0 e 50
x <- runif(n=100, min=0, max=50)
dbqa.round(x,7)  # come fosse ozono
dbqa.round(x,14) # come fosse cadmio
\dontrun{
dbqa.round(x,17) # come fosse antani
}
}
