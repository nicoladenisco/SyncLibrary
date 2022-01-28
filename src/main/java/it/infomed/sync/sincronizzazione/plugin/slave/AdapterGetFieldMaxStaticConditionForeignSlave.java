/*
 *  GetAccettazioneId.java
 *  Creato il May 8, 2018, 11:12:39 AM
 *
 *  Copyright (C) 2018 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package it.infomed.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.plugin.AbstractAdapter;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.NotImplementedException;

/**
 *
 * @author Nadia Romano
 */
public class AdapterGetFieldMaxStaticConditionForeignSlave extends AbstractAdapter
{
  protected String tableName, dbName, foreignId;
  protected String tmpCondition1, tmpValue1;
  protected String tmpCondition2, tmpValue2;

  private Map setup;

  @Override
  public void setConfig(String nomeAdapter, Map vData)
     throws Exception
  {
    setup = vData;

    if((tableName = (String) setup.get("foreign-table")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-table");
    if((foreignId = (String) setup.get("foreign-id")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-id");

    //prendo la prima condizione dalla configurazione
    if((tmpCondition1 = (String) setup.get("foreign-cond1")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond1");
    if((tmpValue1 = (String) setup.get("foreign-cond1-value")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond1-value");

    //prendo la seconda condizione dalla configurazione
    if((tmpCondition2 = (String) setup.get("foreign-cond2")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond1");
    if((tmpValue2 = (String) setup.get("foreign-cond2-value")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond2-value");

  }

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = uniqueName;
    this.dbName = dbName;
  }

  @Override
  public void masterFineValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = uniqueName;
    this.dbName = dbName;
  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    throw new NotImplementedException("method not implemented");
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String sSQL
       = "SELECT MAX(" + foreignId + ") FROM " + tableName
       + " WHERE " + tmpCondition1 + " = '" + tmpValue1 + "' AND " + tmpCondition2 + " = '" + tmpValue2 + "'";
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
    int id = (int) (lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1);
    String update
       = "UPDATE " + tableName + " SET " + foreignId + "= " + id
       + " WHERE " + tmpCondition1 + " = '" + tmpValue1 + "' AND " + tmpCondition2 + " = '" + tmpValue2 + "'";
    DbPeer.executeStatement(update, con);
    return id;
  }
}
