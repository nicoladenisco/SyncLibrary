/*
 *  AdapterFixedValuesMap.java
 *  Creato il Nov 30, 2017, 13:23:00
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
package org.sync.common.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.Utils;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jdom2.Element;

/**
 * Adapter per mappare valori fissi specificati nel file XML.
 *
 * @author Nicola De Nisco
 */
public class AdapterFixedValuesMap extends AbstractAdapter
{
  protected String defaultValue = null;
  protected Map<String, String> valuesMap = new HashMap<>();
  private Map setup;

  @Override
  public void setXML(String location, Element data)
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

      if(isEquNocase(key, "defval"))
        defaultValue = value;
      else
        valuesMap.put(key, value);
    }
  }

  @Override
  public Object slaveValidaValore(String _key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String key = okStr(v);
    String val = valuesMap.get(key);
    return val == null ? defaultValue : val;
  }

  @Override
  public Object masterValidaValore(String _key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    String key = okStr(v.asString());
    String val = valuesMap.get(key);
    return val == null ? defaultValue : val;
  }

  @Override
  public void masterPreparaValidazione(List<Record> lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
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
