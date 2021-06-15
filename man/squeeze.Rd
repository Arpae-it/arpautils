\name{squeeze}
\alias{squeeze}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Dato un vettore numerico, lo rappresenta come stringa sintetica
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Dato un vettore di interi, ne dà una rappresentazione sintetica in una stringa.
}
\usage{
squeeze(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
%%     ~~Describe \code{x} here~~
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
 x <- c(2:5,8,13:21)
 x
 paste(x,collapse=",")
 squeeze(x)
 squeeze(sample(x=1:1000,size=995))
}
