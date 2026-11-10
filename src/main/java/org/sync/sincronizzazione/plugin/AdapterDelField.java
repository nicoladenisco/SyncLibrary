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
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.commonlib5.utils.StringOper.isEquNocase;
import org.jdom2.Element;
import org.sync.common.CaseInsensitiveMap;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.RecordDataMap;
import org.sync.common.RecordsDataBlock;
import org.sync.common.SyncContext;
import org.sync.common.Utils;
import org.sync.common.plugin.AbstractAdapter;
import org.sync.sincronizzazione.LocationType;

/**
 * Adapter per cancellazione logica.
 *
 * @author Nicola De Nisco
 */
public class AdapterDelField extends AbstractAdapter
{
  protected String tableName, dbName;

  protected final Map delFields = new CaseInsensitiveMap();
  protected Object delValue = 1;
  private Map setup;

  @Override
  public void setXML(LocationType location, Element data)
     throws Exception
  {
    setup = Utils.parseParams(data);
    commonSetup();
  }

  protected void commonSetup()
  {
    for(Map.Entry<String, String> entry : (Set<Map.Entry<String, String>>) setup.entrySet())
    {
      String key = entry.getKey();
      String value = entry.getValue();

      if(isEquNocase(key, "delval"))
        delValue = value;
      else
        delFields.put(key, value);
    }
  }

  @Override
  public Object slaveValidaValore(String key, RecordDataMap record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String delVal = okStr(delFields.get(f.field.first.toUpperCase()));
    return delVal != null && isEquNocase(record.get(f.field.first), delVal) ? delValue : null;
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
  public void slavePreparaValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slaveFineValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }
}
