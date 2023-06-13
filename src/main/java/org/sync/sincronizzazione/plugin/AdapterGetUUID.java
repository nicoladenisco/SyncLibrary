/*
 *  AdapterGetUUID.java
 *  Creato il Apr 24, 2018, 11:38:03 AM
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
import java.util.UUID;

/**
 * Adapter per la generazione di un UUID.
 * Ritorna un UUID da inserire nel campo verificando che non sia già utilizzato.
 * @author Nadia Romano
 */
public class AdapterGetUUID extends AbstractAdapter
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
  public void slavePreparaValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericSlave) parentAgent).correctTableName;
    this.dbName = ((AgentGenericSlave) parentAgent).databaseName;
  }

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    while(true)
    {
      UUID uuid = UUID.randomUUID();
      String randomUUIDString = uuid.toString();
      String sSQL
         = "SELECT " + f.field.first
         + "  FROM " + tableName
         + " WHERE " + f.field.first + " = '" + randomUUIDString + "'";
      List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true);
      if(lsRecs.isEmpty())
        return randomUUIDString;
    }
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    while(true)
    {
      UUID uuid = UUID.randomUUID();
      String randomUUIDString = uuid.toString();
      String sSQL
         = "SELECT " + f.field.first
         + "  FROM " + tableName
         + " WHERE " + f.field.first + " = '" + randomUUIDString + "'";
      List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
      if(lsRecs.isEmpty())
        return randomUUIDString;
    }
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slaveFineValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }
}
