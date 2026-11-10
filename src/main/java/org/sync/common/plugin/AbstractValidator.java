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
package org.sync.common.plugin;

import com.workingdogs.village.Record;
import java.sql.Connection;
import java.util.List;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.RecordDataMap;
import org.sync.common.RecordsDataBlock;
import org.sync.common.SyncContext;

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
  public void masterPreparaValidazione(List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    notImplemented();
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    notImplemented();
  }

  @Override
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception
  {
    notImplemented();
    return 0;
  }

  @Override
  public void slavePreparaValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    notImplemented();
  }

  @Override
  public void slaveFineValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    notImplemented();
  }

  @Override
  public int slaveValidaRecord(String key, RecordDataMap record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception
  {
    notImplemented();
    return 0;
  }
}
