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
package org.sync.common.plugin;

import com.workingdogs.village.Record;
import org.sync.common.SyncContext;
import java.sql.Connection;
import java.util.Date;
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
  public boolean confermaValoriRecord(Map r, Date now,
     String key, Map<String, String> arKeys,
     Map<String, Object> valoriSelect, Map<String, Object> valoriUpdate, Map<String, Object> valoriInsert,
     SyncContext context, Connection con)
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
