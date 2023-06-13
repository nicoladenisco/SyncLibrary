/*
 *  SyncRulePlugin.java
 *  Creato il 9-mar-2013, 18.35.11
 *
 *  Copyright (C) 2013 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package org.sync.common.plugin;

import org.sync.common.FilterKeyData;
import org.sync.common.SyncContext;
import java.util.Date;
import java.util.List;
import org.commonlib5.utils.Pair;

/**
 * Interfaccia di un plugin di sincronizzazione generico verso workstation di refertazione.
 *
 * @author Nicola De Nisco
 */
public interface SyncRulePlugin extends SyncPlugin
{
  /**
   * Ritorna il nome del database di default per la regola.
   * @return
   */
  public String getDatabaseName();

  /**
   * Legge strategia di cancellazione.
   * @return
   */
  public SyncDeletePlugin getDelStrategy();

  /**
   * Salva strategia di cancellazione.
   * @param delStrategy
   */
  public void setDelStrategy(SyncDeletePlugin delStrategy);

  /**
   * Recupera il filtro generico per tutti gli agent.
   * @return
   */
  public FilterKeyData getFilter();

  /**
   * Imposta un filtro generico per tutti gli agent.
   * @param filter
   */
  public void setFilter(FilterKeyData filter);

  /**
   * Inizio di esecuzione della regola.
   * Consente operazioni di inizializzazione della regola.
   * @param context
   * @throws Exception
   */
  public void beginRumbleRule(SyncContext context)
     throws Exception;

  /**
   * Fine esecuzione della regola.
   * Consente operazioni di pulizia dopo l'esecuzione.
   * @param context
   * @throws Exception
   */
  public void endRumbleRule(SyncContext context)
     throws Exception;

  /**
   * Verifica per aggiornamento records.
   * Per ogni blocco riporta la chiave primaria e il timestamp di aggiornamento.
   * Il server risponde con un elenco di chiavi primarie da aggiornare.
   * @param nome nome del blocco
   * @param parametri parametri della richiesta: chiave/timestamp ultimo aggiornamento
   * @param oldTimestamp ultimo timestamp di una richiesta simile (può essere null)
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  public void verificaBlocco(String nome,
     List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception;

  /**
   * Acquisice i dati dei blocchi da aggiornare.
   * Per il blocco indicato e le chiavi primarie specificate nella richiesta,
   * estrae i record da aggiornare.
   * @param nome nome del blocco
   * @param parametri parametri della richiesta: chiave dei records
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  public void aggiornaBlocco(String nome,
     List<String> parametri, SyncContext context)
     throws Exception;

  /**
   * Lista nomi dei blocchi nella regola.
   * @return lista di stringhe
   * @throws Exception
   */
  public List<String> getListaBlocchi()
     throws Exception;

  /**
   * Pianifica aggiornamento richiesto.
   * @param nome
   * @param aggiorna
   * @param vInfo
   * @param context
   * @throws Exception
   */
  public void pianificaAggiornamento(String nome,
     List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception;

  /**
   * Ritorna il pool richisto.
   * @param poolName nome del pool
   * @return il pool dati oppure null
   * @throws Exception
   */
  public SyncPoolPlugin getPool(String poolName)
     throws Exception;
}
