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
package it.infomed.sync.sincronizzazione.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractAdapter;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jdom2.Element;

/**
 * Adapter per limitare dimensioni delle stringhe.
 * Questo adapter può essere utilizzato sia come master che come slave.
 * @author Nicola De Nisco
 */
public class AdapterMaxLengthForeign extends AbstractAdapter
{
  protected int maxValue = 0;
  protected Map<String, String> valuesMap = new HashMap<>();
  private Map setup;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    setup = Utils.parseParams(data);
    commonSetup();
  }

  @Override
  public void setConfig(String nomeAdapter, Map data)
     throws Exception
  {
    setup = data;
    commonSetup();
  }

  protected void commonSetup()
  {
    for(Map.Entry<String, String> entry : (Set<Map.Entry<String, String>>) setup.entrySet())
    {
      String key = entry.getKey();
      String value = entry.getValue();

      if(isEquNocase(key, "maxval"))
        maxValue = parse(value, 0);
      else
        valuesMap.put(key, value);
    }
  }

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
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
    return maxValue == 0 ? v.asString() : okStr(v.asString(), maxValue);
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    return maxValue == 0 ? v : okStr(v, maxValue);
  }
}
