/*
 *  AgentTableUpdateLocalMaster.java
 *  Creato il Nov 24, 2017, 7:16:45 PM
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
package it.infomed.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Record;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.db.DbPeer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.torque.util.BasePeer;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;

/**
 * Adapter di sincronizzazione orientato alle tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentTableUpdateLocalMaster extends AgentSharedGenericLocalMaster
{
  protected Element tblLocal, tblForeign;
  protected String tableNameLocal, tableNameForeign, databaseName;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((tblLocal = tables.getChild("local")) == null)
      throw new SyncSetupErrorException(0, "tables/local");

    if((tblForeign = tables.getChild("foreign")) == null)
      throw new SyncSetupErrorException(0, "tables/foreign");

    if((tableNameLocal = okStrNull(tblLocal.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/local:name");

    if((tableNameForeign = okStrNull(tblForeign.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/foreign:name");
  }

  @Override
  public String getRole()
  {
    return ROLE_MASTER;
  }

  @Override
  public void populateConfigForeign(Map context)
     throws Exception
  {
    super.populateConfigForeign(context);
    context.put("foreign-table-name", tblForeign.getAttributeValue("name"));
    context.put("foreign-table-database", tblForeign.getAttributeValue("database"));
  }

  @Override
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);

    List<Record> lsRecs = haveTs() ? queryWithTimestamp(oldTimestamp) : queryWithoutTimestamp();

    if(lsRecs.isEmpty())
      return;

    for(int i = 0; i < arLocalKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arLocalKeys.getKeyByIndex(i));
      if(f.localAdapter != null)
        f.localAdapter.masterSharedFetchData(tableNameLocal, null, lsRecs, f, context);
      if(f.adapter != null)
        f.adapter.masterSharedFetchData(tableNameLocal, null, lsRecs, f, context);
    }

    if(haveTs())
      compilaBloccoMaster(arLocalKeys, parametri, lsRecs, oldTimestamp, timeStampLocal.first, v, context);
    else
      compilaBloccoMaster(arLocalKeys, parametri, lsRecs, null, null, v, context);
  }

  protected List<Record> queryWithTimestamp(Date oldTimestamp)
     throws Exception
  {
    if(correctLocal)
    {
      // corregge la condizione di ULT_MODIF a NULL: produce continui aggiornamenti dei records
      String tableName = okStrAny(correctTableName, tableNameLocal);
      StringBuilder su = new StringBuilder(128);
      su.append("UPDATE ").append(tableName)
         .append(" SET ").append(timeStampLocal.first).append("='").append(DateTime.formatIsoFull(new Date())).append("'")
         .append(" WHERE ").append(timeStampLocal.first).append(" IS NULL");
      BasePeer.executeStatement(su.toString());
    }

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    sb.append(timeStampLocal.first).append(",STATO_REC");
    arLocalKeys.forEach((f) -> sb.append(',').append(f.first));
    sb.append(" FROM ").append(tableNameLocal);
    sb.append(" WHERE 1=1");

    if(oldTimestamp != null)
      sb.append(" AND ").append(timeStampForeign.first).append(">='")
         .append(DateTime.formatIsoFull(oldTimestamp)).append('\'');

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterCompare)
        sb.append(" AND (").append(sql).append(")");

      // se richiesto aggiunge limitazione temporale
      if(filter.timeLimitCompare != 0)
      {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitCompare);
        Date firstDate = cal.getTime();
        sb.append(" AND (").append(timeStampForeign.first).append(" >= '")
           .append(DateTime.formatIsoFull(firstDate)).append("')");
      }
    }

    return DbPeer.executeQuery(sb.toString(), databaseName);
  }

  protected List<Record> queryWithoutTimestamp()
     throws Exception
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    sb.append("STATO_REC");
    arLocalKeys.forEach((f) -> sb.append(',').append(f.first));
    sb.append(" FROM ").append(tableNameLocal);
    sb.append(" WHERE 1=1");

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterCompare)
        sb.append(" AND (").append(sql).append(")");
    }

    return DbPeer.executeQuery(sb.toString(), databaseName);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);
    boolean fetchAllData = checkTrueFalse(context.get("fetch-all-data"), arLocalKeys.isEmpty());

    // estrae i soli campi significativi
    List<FieldLinkInfoBean> arRealFields = arFields.stream()
       .filter((f) -> f.localField != null && isOkStr(f.localField.first))
       .collect(Collectors.toList());

    String fields = join(arRealFields, (f) -> f.localField.first, ",", null);

    for(int i = 0; i < arLocalKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arLocalKeys.getKeyByIndex(i));

      if(f.localAdapter != null)
        f.localAdapter.masterSharedConvertKeys(tableNameLocal, "caleido", parametri, f, i, context);
      if(f.adapter != null)
        f.adapter.masterSharedConvertKeys(tableNameLocal, "caleido", parametri, f, i, context);
    }

    if(fetchAllData || arLocalKeys.size() == 1)
    {
      populateSingleKey(fetchAllData, fields, parametri, arRealFields, v, context);
      return;
    }

    populateMultipleKey(fields, parametri, arRealFields, v, context);
  }

  private void populateSingleKey(boolean fetchAllData, String fields,
     List<String> parametri, List<FieldLinkInfoBean> arRealFields, VectorRpc v, SyncContext context)
     throws Exception
  {
    StringBuilder sb = new StringBuilder();

    if(fetchAllData)
    {
      sb.append("SELECT ");
      sb.append(fields);
      sb.append(" FROM ").append(tableNameLocal);
      sb.append(" WHERE ((STATO_REC IS NULL) OR (STATO_REC < 10))");
    }
    else
    {
      String fieldLink = arLocalKeys.getKeyByIndex(0);

      sb.append("SELECT ");
      sb.append(fieldLink).append(',').append(fields);
      sb.append(" FROM ").append(tableNameLocal);
      sb.append(" WHERE ((STATO_REC IS NULL) OR (STATO_REC < 10))");
      if(!parametri.isEmpty())
      {
        sb.append(" AND ").append(fieldLink).append(" IN (");
        sb.append(join(parametri.iterator(), ',', '\'')).append(")");
      }
    }

    if(filter.timeLimitFetch > 0)
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
      sb.append(" AND (ULT_MODIF >= '").append(DateTime.formatIsoFull(cal.getTime())).append("')");
    }

    List<Record> lsRecs = DbPeer.executeQuery(sb.toString(), databaseName);
    if(lsRecs.isEmpty())
      return;

    popolaTuttiRecords(tableNameLocal, "caleido", lsRecs, arRealFields, v, context);
  }

  private void populateMultipleKey(String fields,
     List<String> parametri, List<FieldLinkInfoBean> arRealFields, VectorRpc v, SyncContext context)
     throws Exception
  {
    int pos;
    String fieldLink = arLocalKeys.getKeyByIndex(0);

    // estrae la prima chiave da utilizzare nella select
    HashSet<String> keyVals = new HashSet<>();
    for(String key : parametri)
    {
      if((pos = key.indexOf('^')) == -1)
        continue;

      keyVals.add(key.substring(0, pos));
    }

    // prepara la select utilizzando la prima delle chiavi
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    sb.append(fieldLink).append(',').append(fields);
    sb.append(" FROM ").append(tableNameLocal);
    if(!parametri.isEmpty())
    {
      sb.append(" WHERE ").append(fieldLink).append(" IN (");
      sb.append(join(keyVals.iterator(), ',', '\'')).append(")");
    }

    // estrae i records dove almeno la prima chiave è verificata
    List<Record> lsRecs = DbPeer.executeQuery(sb.toString(), databaseName);
    if(lsRecs.isEmpty())
      return;

    // produce una nuova lista di record controllando i valori delle altre chiavi
    List<Record> lsRecsFlt = filtraRecors(lsRecs, parametri, context);

    popolaTuttiRecords(tableNameLocal, "caleido", lsRecsFlt, arRealFields, v, context);
  }
}
