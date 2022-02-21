/*
 *  AbstractAdapter.java
 *  Creato il Nov 27, 2017, 12:01:29 PM
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
package it.infomed.sync.common.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Classe base di tutti gli adapter.
 *
 * @author Nicola De Nisco
 */
public class AbstractAdapter extends AbstractPlugin
   implements SyncAdapterPlugin
{
  protected SyncAgentPlugin parentAgent;
  protected SyncDeletePlugin delStrategy;

  @Override
  public SyncAgentPlugin getParentAgent()
  {
    return parentAgent;
  }

  @Override
  public void setParentAgent(SyncAgentPlugin parentAgent)
  {
    this.parentAgent = parentAgent;
  }

  public SyncDeletePlugin getDelStrategy()
  {
    return delStrategy;
  }

  public void setDelStrategy(SyncDeletePlugin delStrategy)
  {
    this.delStrategy = delStrategy;
  }

  @Override
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void slavePreparaValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void slaveFineValidazione(List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void slaveSharedFetchData(List<Record> lsRecs, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    die("Questo adapter non può essere utilizzato insieme a shared. Implementare le funzionalità di sharing delle chiavi oppure usare un opportuno tableadapter.");
  }

  @Override
  public void slaveSharedConvertKeys(List<String> parametri,
     FieldLinkInfoBean field, int idxInKeys, SyncContext context)
     throws Exception
  {
    die("Questo adapter non può essere utilizzato insieme a shared. Implementare le funzionalità di sharing delle chiavi oppure usare un opportuno tableadapter.");
  }

  @Override
  public void masterSharedFetchData(List<Record> lsRecs, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    die("Questo adapter non può essere utilizzato insieme a shared. Implementare le funzionalità di sharing delle chiavi oppure usare un opportuno tableadapter.");
  }

  @Override
  public void masterSharedConvertKeys(List<String> parametri, FieldLinkInfoBean field, int idxInKeys, SyncContext context)
     throws Exception
  {
    die("Questo adapter non può essere utilizzato insieme a shared. Implementare le funzionalità di sharing delle chiavi oppure usare un opportuno tableadapter.");
  }
}
