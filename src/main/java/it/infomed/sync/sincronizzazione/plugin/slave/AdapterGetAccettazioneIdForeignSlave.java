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
public class AdapterGetAccettazioneIdForeignSlave extends AbstractAdapter
{
  protected String tableName, dbName, foreignTableName,
     foreignId, cond1, cond1Value, cond2, cond2Value, cond3, cond3Value, cond4;
  private Map setup;

  @Override
  public void setConfig(String nomeAdapter, Map vData)
     throws Exception
  {
    setup = vData;

    if((foreignTableName = (String) setup.get("foreign-table")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-table");
    if((foreignId = (String) setup.get("foreign-id")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-id");
    if((cond1 = (String) setup.get("foreign-cond1")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond1");
    if((cond2 = (String) setup.get("foreign-cond2")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond2");
    if((cond3 = (String) setup.get("foreign-cond3")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond3");  
    if((cond1Value = (String) setup.get("foreign-cond1-value")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond1-value");
    if((cond2Value = (String) setup.get("foreign-cond2-value")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond2-value");
    if((cond3Value = (String) setup.get("foreign-cond3-value")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond3-value");
    if((cond4 = (String) setup.get("foreign-cond4")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-cond4");
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
    int id;
    String sSQL
       = "SELECT " + f.foreignField.first + " FROM " + tableName
       + " WHERE " + cond4 + "= '" + key + "' AND " + cond3 + " ='" + cond3Value + "'";
    List<Record> lsRec = DbPeer.executeQuery(sSQL, dbName, true, con);
    if(lsRec.isEmpty())
    {
      sSQL
         = "SELECT MAX(" + foreignId + ") FROM " + foreignTableName
         + " WHERE " + cond2 + " = '" + cond2Value + "' AND " + cond1 + " = " + cond1Value;
      List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
      id = (int) (lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1);
      String update
         = "UPDATE " + foreignTableName
         + " SET " + foreignId + " = " + id
         + " WHERE " + cond2 + " = '" + cond2Value + "' AND " + cond1 + " = " + cond1Value;
      DbPeer.executeStatement(update, con);
    }
    else
    {
      id = lsRec.get(0).getValue(1).asInt();
    }

    return id;
  }
}
