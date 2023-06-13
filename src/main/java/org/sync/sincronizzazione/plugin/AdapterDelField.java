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
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.plugin.AbstractAdapter;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter per cancellazione logica Diamante.
 * @author Nicola De Nisco
 */
public class AdapterDelField extends AbstractAdapter
{
  protected String tableName, dbName;

  protected static final Map delFields = new HashMap();
  protected static final Object DEL_VALUE = 1;

  static
  {
    delFields.put("ANNULLATO", 10);
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String delVal = okStr(delFields.get(f.field.first.toUpperCase()));
    return delVal != null && isEquNocase(record.get(f.field.first), delVal) ? DEL_VALUE : null;
  }

  @Override
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
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
  }

  @Override
  public void slaveFineValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }
}
