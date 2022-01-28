/*
 *  AbstractValidator.java
 *  Creato il Nov 27, 2017, 12:05:01 PM
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
package it.infomed.sync.plugins;

import com.workingdogs.village.Record;
import it.infomed.sync.FieldLinkInfoBean;
import it.infomed.sync.SyncContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Classe base di tutti i validatori.
 *
 * @author Nicola De Nisco
 */
public class AbstractValidator extends AbstractPlugin
   implements SyncValidatorPlugin
{
  protected SyncAgentPlugin parentAgent;

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

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void masterFineValidazione(String uniqueName, String dbName, List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int slaveValidaRecord(String key, Map record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
