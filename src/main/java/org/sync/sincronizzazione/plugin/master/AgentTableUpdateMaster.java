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
package org.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Record;
import java.util.*;
import java.util.stream.Collectors;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.db.Database;
import org.sync.db.DbPeer;
import org.sync.db.QueryParamsBean;
import org.sync.sincronizzazione.LocationType;
import org.sync.sincronizzazione.RoleType;

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
  public void setXML(LocationType location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((tblElement = tables.getChild(location.getFunction())) == null)
      throw new SyncSetupErrorException(0, "tables/" + location);

    if((tableName = okStrNull(tblElement.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/" + location + ":name");

    if(correctTableName == null)
      correctTableName = tableName;

    caricaTipiColonne();
  }

  @Override
  public RoleType getRole()
  {
    return RoleType.MASTER;
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
      String sSQL
         = "UPDATE " + correctTableName
         + "   SET " + timeStamp.first + "=?"
         + " WHERE " + timeStamp.first + " IS NULL";

      DbPeer.executeStatement(sSQL, Arrays.asList(Database.currentTimestamp()));
    }

    QueryParamsBean qb = new QueryParamsBean();
    qb.setSelect(timeStamp.first + ",STATO_REC," + join(arKeys.keySet().iterator(), ','));
    qb.setFrom(tableName);

    if(oldTimestamp != null)
    {
      qb.addWhere(timeStamp.first + " >= ${ts:oldTimestamp}");
      qb.addValues("oldTimestamp", oldTimestamp);
    }

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterCompare)
        qb.addWhere(sql);

      // se richiesto aggiunge limitazione temporale
      if(filter.timeLimitCompare != 0)
      {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitCompare);
        Date firstDate = cal.getTime();
        qb.addWhere(timeStamp.first + " >= ${firstDate:ts}");
        qb.addValues("firstDate", firstDate);
      }
    }

    String sqlSelect = qb.makeSQLstring();
    return DbPeer.executeQuery(sqlSelect, qb.getValuesMap(), databaseName);
  }

  protected List<Record> queryWithoutTimestamp()
     throws Exception
  {
    QueryParamsBean qb = new QueryParamsBean();
    qb.setSelect("STATO_REC," + join(arKeys.keySet().iterator(), ','));
    qb.setFrom(tableName);

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterCompare)
        qb.addWhere(sql);
    }

    String sqlSelect = qb.makeSQLstring();
    return DbPeer.executeQuery(sqlSelect, qb.getValuesMap(), databaseName);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);
    boolean fetchAllData = checkTrueFalse(context.get("fetch-all-data"), arKeys.isEmpty());

    // cut-off per lista parametri eccessivamente lunga
    if(parametri.size() >= limiteQueryParametri)
    {
      log.info("La richiesta verifica parametri eccede " + limiteQueryParametri + " records; tutti i records verranno ritornati.");
      fetchAllData = true;
    }

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

    if(fetchAllData)
    {
      populateFetchAll(fields, arRealFields, v, context);
      return;
    }

    if(arKeys.size() == 1)
    {
      populateSingleKey(fields, parametri, arRealFields, v, context);
      return;
    }

    populateMultipleKey(fields, parametri, arRealFields, v, context);
  }

  private void populateFetchAll(String fields,
     List<FieldLinkInfoBean> arRealFields, VectorRpc v, SyncContext context)
     throws Exception
  {
    List<Record> lsRecs = queryFetchAll(fields);
    if(lsRecs.isEmpty())
      return;

    popolaTuttiRecords(lsRecs, arRealFields, v, context);
  }

  private void populateSingleKey(String fields,
     List<String> parametri, List<FieldLinkInfoBean> arRealFields, VectorRpc v, SyncContext context)
     throws Exception
  {
    List<Record> lsRecs = queryUnaChiave(fields, arKeys.getKeyByIndex(0), parametri);

    if(lsRecs.isEmpty())
      return;

    popolaTuttiRecords(lsRecs, arRealFields, v, context);
  }

  private List<Record> queryFetchAll(String fields)
     throws Exception
  {
    QueryParamsBean qb = new QueryParamsBean();
    qb.setSelect(fields);
    qb.setFrom(tableName);
    qb.setWhere("((STATO_REC IS NULL) OR (STATO_REC < 10))");

    if(filter.timeLimitFetch > 0)
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
      qb.addWhere("ULT_MODIF >= ${timelimit:ts}");
      qb.addValues("timelimit", cal.getTime());
    }

    String sSQL = qb.makeSQLstring();
    return DbPeer.executeQuery(sSQL, qb.getValuesMap(), databaseName);
  }

  private List<Record> queryUnaChiave(String fields, String fieldLink, Collection<String> parametriLink)
     throws Exception
  {
    QueryParamsBean qb = new QueryParamsBean();
    qb.setSelect(fieldLink + "," + fields);
    qb.setFrom(tableName);
    qb.setWhere("((STATO_REC IS NULL) OR (STATO_REC < 10))");

    if(!parametriLink.isEmpty())
    {
      qb.addWhere(fieldLink + " IN (" + join(parametriLink.iterator(), ',', '\'') + ")");
    }

    if(filter.timeLimitFetch > 0)
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
      qb.addWhere("ULT_MODIF >= ${timelimit:ts}");
      qb.addValues("timelimit", cal.getTime());
    }

    String sSQL = qb.makeSQLstring();
    return DbPeer.executeQuery(sSQL, qb.getValuesMap(), databaseName);
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

    // estrae i records dove almeno la prima chiave è verificata
    List<Record> lsRecs = queryUnaChiave(fields, fieldLink, keyVals);
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
