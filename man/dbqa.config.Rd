\name{dbqa.config}
\alias{dbqa.config}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Prepara le credenziali di accesso al DB
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Prepara le credenziali di accesso al DB di qualita' dell'aria
}
\usage{
dbqa.config(db_usr=NULL,
            db_pwd=NULL,
            db_name=NULL,
            db_tz="Africa/Algiers",
            config.file="~/util/R/dbqa_keys.R",
            global=TRUE)
}
\arguments{
  \item{db_usr}{user}
  \item{db_pwd}{password}
  \item{db_name}{indirizzo}
  \item{db_tz}{timezone}
  \item{config.file}{file contenente le definizioni di \code{db_usr}, \code{db_pwd}, \code{db_name} e \code{db_tz}}
  \item{global}{TRUE se si vuole che \code{db_usr}, \code{db_pwd}, \code{db_name} e \code{db_tz} siano definite globalmente}
}
\details{
La definizione delle credenziali di accesso \code{db_usr}, \code{db_pwd} e \code{db_name} e della timezone \code{db_tz} può avvenire passando gli argomenti alla funzione, oppure raccogliendoli in un file personale \code{config.file}. Il file conterrà quattro righe:
\preformatted{
db_usr = "uuu"
db_pwd = "ppp"
db_name = "//111.111.11.111:2222/sss"
db_tz = "Africa/Algiers"
}
In questo esempio, "uuu" è il nome utente, "ppp" la password, 111.111.11.111 è l'indirizzo IP del server Oracle, 2222 la porta, "sss" il nome del service.
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
