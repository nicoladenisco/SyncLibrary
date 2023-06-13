/*
 *  DeleteBeforeUpdateValidatorSlave.java
 *  Creato il May 20, 2020, 7:31:13 PM
 *
 *  Copyright (C) 2020 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package org.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Record;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.plugin.AbstractValidator;
import org.sync.db.DbPeer;
import org.sync.sincronizzazione.plugin.master.AgentGenericMaster;
import org.sync.sincronizzazione.plugin.master.AgentSharedGenericMaster;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Cancella il contenuto di una tabella prima dell'inizio dell'aggiornamento.
 * Nessuna verifica di integrità referenziale.
 * Una semplice DELETE con eventualmente dei parametri.
 * Questo validatore ha senso solo per uno slave e di conseguenza solo i
 * metodi slave sono implementati.
 *
 * @author Nicola De Nisco
 */
public class DeleteBeforeUpdateValidatorSlave extends AbstractValidator
{
  protected String tableName, dbName;
  protected String filter;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    filter = data.getAttributeValue("filter");
  }

  @Override
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericMaster) parentAgent).correctTableName;
    this.dbName = ((AgentGenericMaster) parentAgent).databaseName;
  }

  @Override
  public void slavePreparaValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericSlave) parentAgent).correctTableName;
    this.dbName = ((AgentGenericSlave) parentAgent).databaseName;

    String sSQL = "DELETE FROM " + tableName;
    if(isOkStr(filter))
      sSQL += " WHERE " + filter;

    DbPeer.executeStatement(sSQL, dbName);
  }

  @Override
  public void masterFineValidazione(
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slaveFineValidazione(
     List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception
  {
    return 0;
  }

  @Override
  public int slaveValidaRecord(String key, Map record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception
  {
    return 0;
  }
}
