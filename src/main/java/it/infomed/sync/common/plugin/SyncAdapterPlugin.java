/*
 *  SyncAdapterPlugin.java
 *  Creato il Nov 19, 2017, 8:57:07 PM
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
package it.infomed.sync.common.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Adapter per la modifica dei campi in casi speciali.
 *
 * @author Nicola De Nisco
 */
public interface SyncAdapterPlugin extends SyncPlugin
{
  /**
   * Imposta agent che contiene questo adapter.
   * @return
   */
  public SyncAgentPlugin getParentAgent();

  /**
   * Ritorna agent che contiene questo adapter.
   * @param parentAgent
   */
  public void setParentAgent(SyncAgentPlugin parentAgent);

  /**
   * Prepara validazione di un blocco di records.
   * Generalmente implemetata solo lato master.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName the value of dbName
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Completa validazione di un blocco di records.
   * Generalmente implemetata solo lato master.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName the value of dbName
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterFineValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Valida il valore di un campo dopo la lettura.
   * Generalmente implemetata solo lato master.
   * @param key chiave di sincronizzazione del record
   * @param r dati del record
   * @param v valore del campo
   * @param f descrittore del campo
   * @return valore sostitutivo dell'originale oppure null
   * @throws Exception
   */
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception;

  /**
   * Prepara validazione di un blocco di records.
   * Generalmente implemetata solo lato slave.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Completa validazione di un blocco di records.
   * Generalmente implemetata solo lato slave.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param lsRecs lista dei records in aggiornamento
   * @param arFields descrittori dei campi
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Valida il valore di un campo dopo la lettura.
   * Generalmente implemetata solo lato slave.
   * @param key chiave di sincronizzazione del record
   * @param record dati del record
   * @param v valore del campo
   * @param f descrittore del campo
   * @param con connessione al db
   * @throws Exception
   * @return the java.lang.Object
   */
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception;

  /**
   * Conversione di valori shared.
   * Converte opportunamente i valori nei record quando questo adapter viene utilizzato per un campo shared.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param lsRecs lista dei records delle chiavi da scambiare con il master
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slaveSharedFetchData(String uniqueName, String dbName, List<Record> lsRecs,
     FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Conversione di chiavi shared.
   * Durante la verifica le chiavi vanno opportunamente convertite da questo adapter.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param parametri array di chiavi da correggere
   * @param field campo di interesse per questo adapter
   * @param idxInKeys indice di questo campo all'interno delle chiavi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void slaveSharedConvertKeys(String uniqueName, String dbName, List<String> parametri,
     FieldLinkInfoBean field, int idxInKeys, SyncContext context)
     throws Exception;

  /**
   * Conversione di valori shared.
   * Converte opportunamente i valori nei record quando questo adapter viene utilizzato per un campo shared.
   * Il frutto della conversione va conservato per chiamate a masterSharedConvertKeys.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param lsRecs lista dei records delle chiavi da scambiare con il master
   * @param field campo di interesse per questo adapter
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterSharedFetchData(String uniqueName, String dbName, List<Record> lsRecs,
     FieldLinkInfoBean field, SyncContext context)
     throws Exception;

  /**
   * Conversione di chiavi shared.
   * Durante la verifica le chiavi vanno opportunamente convertite da questo adapter.
   * @param uniqueName nome della tabella di riferimento o altro elemento univoco
   * @param dbName nome del database
   * @param parametri array di chiavi da correggere
   * @param field campo di interesse per questo adapter
   * @param idxInKeys indice di questo campo all'interno delle chiavi
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  public void masterSharedConvertKeys(String uniqueName, String dbName, List<String> parametri,
     FieldLinkInfoBean field, int idxInKeys, SyncContext context)
     throws Exception;
}
