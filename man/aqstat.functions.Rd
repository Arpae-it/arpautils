\name{aqstat.functions}
\alias{stat.period}
\alias{stat.period2}
\alias{which.period}
\alias{stat.window}
\alias{exc.period}
\alias{shift}
\alias{mean_window}
\alias{detect.event}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
%%  ~~function to do ... ~~
Funzioni base per il calcolo di statistiche di legge su dati QA.
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Funzioni base per il calcolo di statistiche di legge su dati QA.
}
\usage{
stat.period(x, period, necess, FUN = mean)
stat.period2(x, period, nmax.missing, FUN = mean)
which.period(x, period, necess, FUN=which.max)
exc.period(x, period, necess, threshold)

stat.window(x, window, necess, FUN = mean)
mean_window(x, k, necess)

detect.event(x, threshold)

shift(x, k)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
%%     ~~Describe \code{x} here~~
vettore dei valori di concentrazione
}
  \item{period}{
%%     ~~Describe \code{period} here~~
vettore, della stessa lunghezza di \code{x}, che identifica i periodi
}
  \item{window}{
%%     ~~Describe \code{period} here~~
vettore numerico di due elementi; descrive l'ampiezza della finestra mobile, p.es. \code{c(-7,0)} per la media mobile su 8 ore
}
  \item{necess}{
%%     ~~Describe \code{necess} here~~
numero di dati validi necessari in ciascun periodo
}
  \item{nmax.missing}{
%%     ~~Describe \code{nmax.missing} here~~
numero massimo di dati mancanti accettabili in ciascun periodo
}
  \item{FUN}{
%%     ~~Describe \code{FUN} here~~
funzione da applicare
}
  \item{threshold}{
%%     ~~Describe \code{threshold} here~~
soglia
}
  \item{k}{
%%     ~~Describe \code{k} here~~
in \code{shift}, numero di passi di cui si vuole spostare \code{x}; in \code{mean_window}, ampiezza della finestra
}}
\details{
%%  ~~ If necessary, more details than the description above ~~
Le funzioni \code{stat.period} e \code{stat.period2} calcolano una statistica \code{FUN} su periodi definiti, con approcci diversi nella gestione dei mancanti. La funzione \code{which.period} lavora come \code{stat.period}, ma si puo' usare per funzioni (come \code{which.min} o \code{which.max}) che non accettano l'argomento \code{na.rm}.

Invece \code{stat.window} opera su una finestra mobile, e chiama \code{shift} che sposta la serie temporale in avanti o indietro nel tempo. Piu' efficiente, ma limitata alla media mobile, e' \code{mean_window}.

La funzione \code{exc.period} fa il conteggio dei superamenti di una data soglia.
Invece \code{detect.event} restituisce una matrice contenente informazioni sui superamenti della soglia (vedi sezione \code{Value}).
}
\value{
Le funzioni \code{stat.period}, \code{stat.period2} e \code{which.period} restituiscono un vettore numerico contenente il valore di \code{FUN} calcolato su \code{x} per ciascuno dei valori di \code{period}.

La funzione \code{exc.period} restituisce un vettore numerico contenente il conteggio dei superamenti di \code{x} per ciascuno dei valori di \code{period}.

Le funzioni \code{stat.window} e \code{mean_window} restituiscono un vettore numerico della stessa lunghezza di \code{x} contenenti rispettivamente la statistica \code{stat} o la media sulla finestra mobile richiesta.

La funzione \code{detect.event} restituisce un \code{data.frame} contenente una riga per ciascun evento di superamento, con le colonne
\item{index.event.start}{posizione del primo timestep di superamento in \code{x}}
\item{event.duration}{durata - in timestep - dell'evento}
\item{event.max}{massimo di \code{x} nell'evento}
\item{index.event.max}{posizione in \code{x} del massimo di \code{x} nell'evento}

La funzione \code{shift} restituisce un vettore numerico della stessa lunghezza di \code{x}.
}
