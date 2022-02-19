/*
 *  AbstractDelete.java
 *  Creato il 26-gen-2022, 10.24.56
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
import it.infomed.sync.common.Utils;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Classe base dei gestori di cancellazione.
 *
 * @author Nicola De Nisco
 */
public class AbstractDelete extends AbstractPlugin
   implements SyncDeletePlugin
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
  public String getRole()
  {
    // la cancellazione avviene solo lato slave
    return "SLAVE";
  }

  @Override
  public void caricaTipiColonne(String databaseName, String tableName)
     throws Exception
  {
  }

  @Override
  public void cancellaRecordsPerDelete(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void cancellaRecordsPerUnknow(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean confermaValoriRecord(Map r, Date now,
     String key, Map<String, String> arKeys,
     Map<String, Object> valoriSelect, Map<String, Object> valoriUpdate, Map<String, Object> valoriInsert,
     SyncContext context, Connection con)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean queryRecordDeleted(Record r, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  protected String buildWhere(String key, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    StringBuilder sb = new StringBuilder();
    Map<String, String> keyMap = Utils.reverseKey(key, arForeignKeys);
    if(keyMap == null)
      return null;

    for(Map.Entry<String, String> entry : keyMap.entrySet())
    {
      String nome = entry.getKey();
      String valore = entry.getValue();
      String tipo = arForeignKeys.get(nome);
      if(tipo == null || valore == null)
        return null;

      sb.append(" AND ").append(nome).append("=").append(Utils.convertValue(valore, tipo));
    }

    return sb.length() == 0 ? null : sb.substring(5);
  }
}
