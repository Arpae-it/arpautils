\name{xts.regolarize}
\alias{xts.regolarize}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Regolarizza la scansione temporale di un oggetto xts
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Regolarizza la scansione temporale di un oggetto xts
}
\usage{
xts.regolarize(tstep, x, f.time=(i<-index(x))[1], l.time=i[length(i)], TZ="Africa/Algiers")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{tstep}{
%%     ~~Describe \code{step} here~~
stringa di carattere che identifica il timestep ("H" o "d", orario o giornaliero)
}
  \item{x}{
%%     ~~Describe \code{x} here~~
serie temporale di tipo \code{xts}
}
  \item{TZ}{
%%     ~~Describe \code{TZ} here~~
timezone
}
  \item{f.time}{
%%     ~~Describe \code{TZ} here~~
primo istante richiesto in output
}
  \item{l.time}{
%%     ~~Describe \code{TZ} here~~
ultimo istante richiesto in output
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
}
