/*
 *  AdapterGetFieldMax.java
 *  Creato il Nov 28, 2017, 5:04:37 PM
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
package org.sync.sincronizzazione.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.plugin.AbstractAdapter;
import org.sync.db.DbPeer;
import org.sync.sincronizzazione.plugin.master.AgentGenericMaster;
import org.sync.sincronizzazione.plugin.master.AgentSharedGenericMaster;
import org.sync.sincronizzazione.plugin.slave.AgentGenericSlave;
import org.sync.sincronizzazione.plugin.slave.AgentSharedGenericSlave;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Adapter per generazione valore computando il massimo di un campo.
 * Questo adapter può essere utilizzato sia come master che come slave.
 * @author Nicola De Nisco
 */
public class AdapterGetFieldMax extends AbstractAdapter
{
  protected String tableName, dbName;

  @Override
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericMaster) parentAgent).correctTableName;
    this.dbName = ((AgentGenericMaster) parentAgent).databaseName;
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slavePreparaValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericSlave) parentAgent).correctTableName;
    this.dbName = ((AgentGenericSlave) parentAgent).databaseName;
  }

  @Override
  public void slaveFineValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    String sSQL = "SELECT MAX(" + f.field.first + ") FROM " + tableName;
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true);
    return lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1;
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String sSQL = "SELECT MAX(" + f.field.first + ") FROM " + tableName;
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
    return lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1;
  }
}
