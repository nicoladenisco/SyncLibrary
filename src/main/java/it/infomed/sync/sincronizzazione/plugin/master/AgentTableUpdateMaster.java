/*
 *  AgentTableUpdateMaster.java
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
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DbPeer;
import java.util.*;
import java.util.stream.Collectors;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;

/**
 * Adapter di sincronizzazione orientato alle tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentTableUpdateMaster extends AgentSharedGenericMaster
{
  protected Element tblElement;
  protected String tableName;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((tblElement = tables.getChild(location)) == null)
      throw new SyncSetupErrorException(0, "tables/" + location);

    if((tableName = okStrNull(tblElement.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/" + location + ":name");

    if(correctTableName == null)
      correctTableName = tableName;

    caricaTipiColonne();
  }

  @Override
  public String getRole()
  {
    return ROLE_MASTER;
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

    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.masterSharedFetchData(lsRecs, f, context);
    }

    if(haveTs())
      compilaBloccoMaster(arKeys, parametri, lsRecs, oldTimestamp, timeStamp.first, v, context);
    else
      compilaBloccoMaster(arKeys, parametri, lsRecs, null, null, v, context);
  }

  protected List<Record> queryWithTimestamp(Date oldTimestamp)
     throws Exception
  {
    if(correct)
    {
      // corregge la condizione di ULT_MODIF a NULL: produce continui aggiornamenti dei records
      StringBuilder su = new StringBuilder(128);
      su.append("UPDATE ").append(correctTableName)
         .append(" SET ").append(timeStamp.first).append("='").append(DateTime.formatIsoFull(new Date())).append("'")
         .append(" WHERE ").append(timeStamp.first).append(" IS NULL");
      DbPeer.executeStatement(su.toString(), databaseName);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    sb.append(timeStamp.first).append(",STATO_REC");
    arKeys.forEach((f) -> sb.append(',').append(f.first));
    sb.append(" FROM ").append(tableName);
    sb.append(" WHERE 1=1");

    if(oldTimestamp != null)
      sb.append(" AND ").append(timeStamp.first).append(">='")
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
        sb.append(" AND (").append(timeStamp.first).append(" >= '")
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
    arKeys.forEach((f) -> sb.append(',').append(f.first));
    sb.append(" FROM ").append(tableName);
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
    boolean fetchAllData = checkTrueFalse(context.get("fetch-all-data"), arKeys.isEmpty());

    // estrae i soli campi significativi
    List<FieldLinkInfoBean> arRealFields = arFields.stream()
       .filter((f) -> f.field != null && isOkStr(f.field.first))
       .collect(Collectors.toList());

    String fields = join(arRealFields, (f) -> f.field.first, ",", null);

    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));

      if(f.adapter != null)
        f.adapter.masterSharedConvertKeys(parametri, f, i, context);
    }

    if(fetchAllData || arKeys.size() == 1)
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
      sb.append(" FROM ").append(tableName);
      sb.append(" WHERE ((STATO_REC IS NULL) OR (STATO_REC < 10))");
    }
    else
    {
      String fieldLink = arKeys.getKeyByIndex(0);

      sb.append("SELECT ");
      sb.append(fieldLink).append(',').append(fields);
      sb.append(" FROM ").append(tableName);
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

    popolaTuttiRecords(lsRecs, arRealFields, v, context);
  }

  private void populateMultipleKey(String fields,
     List<String> parametri, List<FieldLinkInfoBean> arRealFields, VectorRpc v, SyncContext context)
     throws Exception
  {
    int pos;
    String fieldLink = arKeys.getKeyByIndex(0);

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
    sb.append(" FROM ").append(tableName);
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

    popolaTuttiRecords(lsRecsFlt, arRealFields, v, context);
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    if((schema = Database.schemaTable(databaseName, tableName)) == null)
      throw new SyncSetupErrorException(String.format(
         "Tabella %s non trovata nel database %s.", tableName, databaseName));

    caricaTipiColonne(schema);

    if(delStrategy != null)
      delStrategy.caricaTipiColonne(databaseName, tableName);
  }
}
