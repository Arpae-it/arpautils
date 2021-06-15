<<<<<<< .mine
# arpautils 0.9.8 [2018-03-07]

 * calculate.daily_report: risolto bug nella gestione del 01/01 con un solo giorno di elaborazione
 * write.daily_report: risolto bug che cacellava le medie annuali per gli inquinanti CO e C6H6 
 * modificati gli id elaborazione dei contatori annuali

# arpautils 0.9.7 [2017-05-15]

 * calculate.ozone_daily_report: modificato il numero di dati necessari (da 18 a 1) per calcolare il massimo giornaliero sull'ora


||||||| .r413
=======
# arpautils 0.9.7 [2017-05-15]

 * calculate.ozone_daily_report: modificato il numero di dati necessari (da 18 a 1) per calcolare il massimo giornaliero sull'ora


>>>>>>> .r414
# arpautils 0.9.6 [2017-01-30]

 * dbqa.get.idcfgsens: risolto piccolo bug che si poteva manifestare al cambio di sensore
 
 
# arpautils 0.9.5 [2016-05-18]

 * calculate.ozone_annual_report: corretto bug in calcolo efficienza giornaliera
 
 
# arpautils 0.9.4 [2016-04-22]

 * write.daily_report: gestisce meglio i mancanti; sovrascrive; bug fixed
 
 
# arpautils 0.9.3 [2016-04-08]

 * corretto annual report (mancava un arrotondamento)
 * aggiunte funzioni prepare.daily_report, calculate.daily_report, write.daily_report
 
 
# arpautils 0.9.2 [2016-04-04]

 * aggiunta media annua del max giorn.media 8h per CO (id.elab=821)
 
 
# arpautils 0.9.1 [2016-02-22]

 * corretto errore in dbqa.descr.elab
 
 
 # arpautils 0.9.0 [2016-01-19]

 * modifiche a write.annual_report: ora scrive il numero di dati usati e gestisce casi <LOD
 * modifiche a write.ozone_annual_report: ora scrive il numero di dati usati
 * compatibilità con sintassi S3: mean.window rinominata in mean_window, round.awayfromzero rinominata in round_awayfromzero
 * rivista la gestione di Imports in DESCRIPTION e NAMESPACE


# arpautils 0.8.17 [2016-01-14]

 * nuove funzioni: dbqa.lod, dbqa.get.idparam, dbqa.get.elab, dbqa.descr.elab
 * dbqa.config ora permette di leggere le credenziali da un file e di esportarle come variabili globali
 * dbqa.connect adeguato di conseguenza
 * dbqa.get.datastaz gestisce il limit of detection
 

# arpautils 0.8.16 [2015-11-03]

 * calculate.ozone_annual_report restituisce anche annual.mean, aot40.veget.NhValid, aot40.forest.NhValid
 * aot restituisce anche NhValid


# arpautils 0.8.15 [2015-10-12]

 * detect.event restituisce anche index.max, indice del [primo] massimo nell'evento


# arpautils 0.8.14 [2015-05-18]

 * dbqa.list.active.staz include laboratori mobili, se richiesto


# arpautils 0.8.13 [2015-03-20]

 * timezone da BST a Africa/Algiers


# arpautils 0.8.12 [2015-01-28]

 * corretto bug nell'AOT 


# arpautils 0.8.11 [2015-01-19]

 * corretto bug nella valutazione della validita' delle statistiche annuali ozono


# arpautils 0.8.10 [2015-01-15]

 * gestione selezione stazioni RRQA con dbqa.isrrqa()
 * estrazione tipo_emis e tipo_area


# arpautils 0.8.9 [2015-01-09]

 * debug negli arrotondamenti in calculate.ozone_annual_report e calculate.ozone_daily_report


# arpautils 0.8.8 [2014-12-18]

 * reintrodotto arrotondamento del max media mobile (non era un bug!)
 * aggiustamenti minimi nel manuale


# arpautils 0.8.7 [2014-12-05]

 * adeguato l'arrotondamento (0.5 diventa 1)
 * arrotonda l'efficienza annuale (come UE)
 * dbqa.get.datasens accede a AA_ARIA.T$01$CONFIG_SENSORI (piu' veloce)


# arpautils 0.8.6 [2014-12-03]

 * migliora scrittura FLG_ELAB
 * considera VAL_OFFSET in estrazione da DB
 * arrotonda in visualizzazione in dbqa.get.datasens
 * tolto arrotondamento della media mobile (bug)


# arpautils 0.8.5 [2014-12-01]

 * aggiunta dbqa.round() per gestire gli arrotondamenti come prescritto dal GdL
 * aggiunta write.annual_report()
 * aggiunta Ndays.in.year()
 * corretto bug nel calcolo dell'efficienza


# arpautils 0.8.4 [2014-11-24]

 * aggiunti a calculate.annual_report(): superamenti orari, orari consecutivi, media di mesi selezionati, calcolo dati attesi ed efficienza


# arpautils 0.8.3 [2014-11-12]

 * migliorata gestione delle flag di validazione in dbqa.get.datasens()
 * completata la gestione delle statistiche annuali ozono con write.ozone_annual_report()


# arpautils 0.8.2 [2014-11-10]

 * corretto bug nella scrittura della durata dell'evento in write.ozone_annual_report()


# arpautils 0.8.1 [2014-10-31]

 * gestione piu' flessibile delle flag di validazione in dbqa.get.datasens()


# arpautils 0.8.0 [2014-10-27]

 * rimosse dal pacchetto le credenziali di accesso al DB


# arpautils 0.7.1 [2014-10-24]

 * aggiunto Sys.setenv(TZ="BST") in dbqa.config()
 * standardizzata sintassi delle stringhe che definiscono i formati data/ora SQL
 * aggiunte le funzioni prepare.ozone_annual_report(), calculate.ozone_annual_report()


# arpautils 0.6.1 [2014-10-10]

 * modificata dbqa.insert per correggere errore di scrittura dei campi DATE nel DB
 * aggiunte le funzioni prepare.annual_report, calculate.annual_report


# arpautils 0.5.3 [2014-07-21]

 * prepare.ozone_daily_report accede a tabella dati recenti
 * aggiornata documentazione
