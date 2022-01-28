/*
 *  AdapterGetFieldMaxForeign.java
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

/**
 * Adapter per generazione valore computando il massimo di un campo.
 * Questo adapter può essere utilizzato sia come master che come slave.
 * @author Nicola De Nisco
 */
public class AdapterGetFieldMaxForeign extends AbstractAdapter
{
  protected String tableName, dbName;

  @Override
  public void setConfig(String nomeAdapter, Map vData)
     throws Exception
  {
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
    String sSQL = "SELECT MAX(" + f.foreignField.first + ") FROM " + tableName;
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true);
    return lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1;
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String sSQL = "SELECT MAX(" + f.foreignField.first + ") FROM " + tableName;
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
    return lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1;
  }
}