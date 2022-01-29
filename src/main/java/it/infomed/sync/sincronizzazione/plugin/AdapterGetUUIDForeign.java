/*
 *  AdapterGetUUIDForeign.java
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
package it.infomed.sync.sincronizzazione.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.plugin.AbstractAdapter;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Adapter per la generazione di un UUID.
 * Ritorna un UUID da inserire nel campo verificando che non sia già utilizzato.
 * @author Nadia Romano
 */
public class AdapterGetUUIDForeign extends AbstractAdapter
{
  protected String tableName, dbName;

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = uniqueName;
    this.dbName = dbName;
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
  public void masterFineValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }
}
