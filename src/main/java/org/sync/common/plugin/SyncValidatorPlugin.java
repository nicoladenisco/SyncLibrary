/*
 *  SyncValidatorPlugin.java
 *  Creato il Nov 19, 2017, 8:57:45 PM
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
package org.sync.common.plugin;

import com.workingdogs.village.Record;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Validatore per la modifica dei records prima della lettura o scrittura.
 *
 * @author Nicola De Nisco
 */
public interface SyncValidatorPlugin extends SyncPlugin
{
  /**
   * Imposta agent che contiene questo validatore.
   * @return
   */
  public SyncAgentPlugin getParentAgent();

  /**
   * Ritorna agent che contiene questo validatore.
   * @param parentAgent
   */
  public void setParentAgent(SyncAgentPlugin parentAgent);

  /**
   * Prepara validazione di un blocco di records.
   * Generalmente implemetata solo lato master.
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception;

  /**
   * Completa validazione di un blocco di records.
   * Generalmente implemetata solo lato master.
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterFineValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception;

  /**
   * Valida un record dopo la lettura dal db.Generalmente implemetata solo lato master.
   * @param key chiave di sincronizzazione del record
   * @param r dati del record
   * @param arFields descrittori dei campi
   * @return 0=OK altrimenti blocca estrazione/utilizzo record
   * @throws Exception
   */
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception;

  /**
   * Prepara validazione di un blocco di records.
   * Generalmente implemetata solo lato slave.
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slavePreparaValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception;

  /**
   * Completa validazione di un blocco di records.
   * Generalmente implemetata solo lato slave.
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slaveFineValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception;

  /**
   * Valida un record prima della scrittura su db.
   * Generalmente implemetata solo lato slave.
   * @param key chiave di sincronizzazione del record
   * @param record dati del record
   * @param arFields descrittori dei campi
   * @param con connessione al db
   * @return 0=OK altrimenti blocca inserimento/aggiornamento record
   * @throws Exception
   */
  public int slaveValidaRecord(String key, Map record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception;
}
