/*
 *  SyncAgentPlugin.java
 *  Creato il Nov 19, 2017, 9:01:20 PM
 *
 *  Copyright (C) 2017 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package it.infomed.sync.plugins;

import it.infomed.sync.DeleteStrategy;
import it.infomed.sync.FilterKeyData;
import it.infomed.sync.SyncContext;
import java.util.Date;
import java.util.List;
import org.commonlib5.utils.Pair;

/**
 * Interfaccia di un agent di sincronizzazione.
 *
 * @author Nicola De Nisco
 */
public interface SyncAgentPlugin extends SyncPlugin
{
  /**
   * Ritorna la regola padre di questo agent.
   * @return
   */
  public SyncRulePlugin getParentRule();

  /**
   * Imposta la regola padre di questo agent.
   * @param parentRule
   */
  public void setParentRule(SyncRulePlugin parentRule);

  /**
   * Legge strategia di cancellazione.
   * @return
   */
  public DeleteStrategy getDelStrategy();

  /**
   * Salva strategia di cancellazione.
   * @param delStrategy
   */
  public void setDelStrategy(DeleteStrategy delStrategy);

  /**
   * Recupera il filtro specifico per questo agent.
   * @return
   */
  public FilterKeyData getFilter();

  /**
   * Imposta un filtro specifico per questo agent.
   * @param filter
   */
  public void setFilter(FilterKeyData filter);

  /**
   * Verifica per aggiornamento records.
   * Per ogni blocco riporta la chiave primaria e il timestamp di aggiornamento.
   * Il server risponde con un elenco di chiavi primarie da aggiornare.
   * @param parametri parametri della richiesta: chiave/timestamp ultimo aggiornamento
   * @param oldTimestamp ultimo timestamp di una richiesta simile (può essere null)
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception;

  /**
   * Acquisice i dati dei blocchi da aggiornare.
   * Per il blocco indicato e le chiavi primarie specificate nella richiesta,
   * estrae i record da aggiornare.
   * @param parametri parametri della richiesta: chiave dei records
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception;

  /**
   * Esamina vInfo e pianifica aggiornamento.
   * Questa funzione è generalmente implentata nello slave.
   * @param aggiorna lista chiavi da recuperare; deve essere popolata
   * @param vInfo informazioni sui record richiesti (verificaBlocco)
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  public void pianificaAggiornamento(List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception;
}
