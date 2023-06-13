/*
 *  SyncDeletePlugin.java
 *  Creato il 26-gen-2022, 10.17.15
 *
 *  Copyright (C) 2022 Informatica Medica s.r.l.
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

import com.workingdogs.village.Record;
import org.sync.common.SyncContext;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Interfaccia di un gestore di cancellazione record.
 *
 * @author Nicola De Nisco
 */
public interface SyncDeletePlugin extends SyncPlugin
{
  /**
   * Imposta agent che contiene questo gestore cancellazione.
   * @return
   */
  public SyncAgentPlugin getParentAgent();

  /**
   * Ritorna agent che contiene questo gestore cancellazione.
   * @param parentAgent
   */
  public void setParentAgent(SyncAgentPlugin parentAgent);

  /**
   * Collega i tipi di colonne richiesti.
   * @param databaseName
   * @param tableName
   * @throws Exception
   */
  public void caricaTipiColonne(String databaseName, String tableName)
     throws Exception;

  /**
   * Applica cancellazione dei record per record segnalati da eliminare.
   * @param lsKeys valori delle chiavi
   * @param arKeys definizioni delle chiavi primarie
   * @param context contesto di aggiornamento
   * @throws Exception
   */
  public void cancellaRecordsPerDelete(List<String> lsKeys,
     Map<String, String> arKeys, SyncContext context)
     throws Exception;

  /**
   * Applica cancellazione dei record per record segnalati come sconosciuti.
   * @param lsKeys valori delle chiavi
   * @param arKeys definizioni delle chiavi primarie
   * @param context contesto di aggiornamento
   * @throws Exception
   */
  public void cancellaRecordsPerUnknow(List<String> lsKeys,
     Map<String, String> arKeys, SyncContext context)
     throws Exception;

  /**
   * Preparazione e/o conferma dei valori prima di inserimento record.
   * @param r contenuto del record da validare
   * @param key
   * @param now
   * @param arKeys
   * @param valoriSelect
   * @param valoriUpdate
   * @param valoriInsert
   * @param context contesto di aggiornamento
   * @param con
   * @return vero per confermare salvataggio record
   * @throws Exception
   */
  public boolean confermaValoriRecord(Map r, Date now,
     String key, Map<String, String> arKeys,
     Map<String, Object> valoriSelect, Map<String, Object> valoriUpdate, Map<String, Object> valoriInsert,
     SyncContext context, Connection con)
     throws Exception;

  /**
   * Verifica per record cancellato.
   * @param r record da esaminare
   * @param context contesto di aggiornamento
   * @return vero se da considerarsi cancellato
   * @throws Exception
   */
  public boolean queryRecordDeleted(Record r, SyncContext context)
     throws Exception;
}
