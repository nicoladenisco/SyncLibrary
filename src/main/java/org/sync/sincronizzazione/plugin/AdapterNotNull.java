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
import org.sync.common.Utils;
import org.sync.common.plugin.AbstractAdapter;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Adapter per generazione valore computando il massimo di un campo.
 * Questo adapter può essere utilizzato sia come master che come slave.
 * @author Nicola De Nisco
 */
public class AdapterNotNull extends AbstractAdapter
{
  protected String nullValue = "";
  protected Map setup;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    setup = Utils.parseParams(data);
    commonSetup();
  }

  protected void commonSetup()
  {
    nullValue = okStr(setup.get("nullval"));
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

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    return okStr(v, nullValue);
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    return okStr(v, nullValue);
  }
}
