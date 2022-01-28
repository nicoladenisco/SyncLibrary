/*
 *  DeleteStrategyNothing.java
 *  Creato il 26-gen-2022, 13.06.26
 *
 *  Copyright (C) 2022 Informatica Medica s.r.l.
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
import it.infomed.sync.common.SyncContext;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Plugin per logica di cancellazione di default.
 *
 * @author Nicola De Nisco
 */
public class DeleteStrategyNothing extends AbstractDelete
{
  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
  }

  @Override
  public void cancellaRecordsPerDelete(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void cancellaRecordsPerUnknow(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
  }

  @Override
  public boolean confermaValoriRecord(Map r, String now, String key, Map<String, String> arKeys, Map<String, String> valoriSelect, Map<String, String> valoriUpdate, Map<String, String> valoriInsert, SyncContext context, Connection con)
     throws Exception
  {
    return true;
  }

  @Override
  public boolean queryRecordDeleted(Record r, SyncContext context)
     throws Exception
  {
    return false;
  }
}
