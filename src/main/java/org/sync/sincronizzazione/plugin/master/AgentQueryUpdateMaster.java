/*
 *  AgentQueryUpdateMaster.java
 *  Creato il Dec 1, 2017, 7:54:14 PM
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

import com.workingdogs.village.QueryDataSet;
import com.workingdogs.village.Record;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringJoin;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;
import org.sync.SyncMacroResolver;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.db.Database;
import org.sync.db.DbPeer;
import org.sync.db.QueryParamsBean;
import org.sync.sincronizzazione.LocationType;

/**
 * Agent di sincronizzazione orientato alle query.
 *
 * @author Nicola De Nisco
 */
public class AgentQueryUpdateMaster extends AgentSharedGenericMaster
{
  protected Element queryElement;
  protected String tableName, databaseName;
  protected boolean ignoreOldTimestamp;
  protected final SyncMacroResolver resolver = new SyncMacroResolver();
  protected final QueryParamsBean qb = new QueryParamsBean();

  @Override
  public void setXML(LocationType location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((queryElement = tables.getChild("query-" + location)) == null)
      throw new SyncSetupErrorException(0, "query-" + location);

    preparaLimitiDate();

    qb.setSelect(resolver.resolveMacro(okStr(queryElement.getChildText("select"))));
    qb.setFrom(resolver.resolveMacro(okStr(queryElement.getChildText("from"))));
    qb.setWhere(resolver.resolveMacro(okStr(queryElement.getChildText("where"))));
    qb.setOrderby(resolver.resolveMacro(okStr(queryElement.getChildText("orderby"))));
    qb.setGroupby(resolver.resolveMacro(okStr(queryElement.getChildText("groupby"))));
    qb.setHaving(resolver.resolveMacro(okStr(queryElement.getChildText("having"))));

    // la tabella locale non è obbligatoria: la fonte dati è comunque la query
    tableName = okStrNull(queryElement.getAttributeValue("table-name"));
    databaseName = okStr(queryElement.getAttributeValue("database-name"), getParentRule().getDatabaseName());

    // in casi particolari (join multiple) si deve ignorare l'ultima time stamp
    ignoreOldTimestamp = checkTrueFalse(queryElement.getAttributeValue("ignoreOldTimestamp"), ignoreOldTimestamp);

    if(arKeys.size() != 1)
      throw new SyncSetupErrorException("Questo agent supporta solo una chiave singola.");

    if(correctTableName == null)
      correctTableName = tableName;

    // questo adapter può esistere solo master quindi la delete strategy è inutile
    delStrategy = null;
    caricaTipiColonne();
  }

  protected void preparaLimitiDate()
     throws Exception
  {
    Date tlCompare, tlFetch;
    GregorianCalendar cal = new GregorianCalendar(1900, 1, 1);
    tlCompare = tlFetch = cal.getTime();

    if(filter != null)
    {
      if(filter.getTimeLimitCompare() > 0)
      {
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -filter.getTimeLimitCompare());
        tlCompare = cal.getTime();
      }

      if(filter.getTimeLimitFetch() > 0)
      {
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -filter.getTimeLimitFetch());
        tlFetch = cal.getTime();
      }
    }

    resolver.putValue("TIME_LIMIT_DATE_COMPARE", DateTime.formatIsoFull(tlCompare));
    resolver.putValue("TIME_LIMIT_DATE_FETCH", DateTime.formatIsoFull(tlFetch));
    resolver.putValue("TIME_LIMIT_DATE", DateTime.formatIsoFull(tlFetch));
  }

  @Override
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);

    if(!haveTs())
      return;

    QueryParamsBean qc = (QueryParamsBean) qb.clone();
    if(oldTimestamp != null && !ignoreOldTimestamp)
    {
      qc.addWhere(timeStamp + " >= ${oldTimestamp:ts}");
      qc.addValues("oldTimestamp", oldTimestamp);
    }

    List<Record> lsRecs = DbPeer.executeQuery(qc, databaseName);
    if(lsRecs.isEmpty())
      return;

    compilaBloccoMaster(arKeys, parametri, lsRecs, oldTimestamp, timeStamp, v, context);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);
    boolean fetchAllData = checkTrueFalse(context.get("fetch-all-data"), arKeys.isEmpty());
    QueryParamsBean qc = (QueryParamsBean) qb.clone();

    // estrae i soli campi significativi
    List<FieldLinkInfoBean> arRealFields = arFields.stream()
       .filter((f) -> f.field != null && isOkStr(f.field.first))
       .collect(Collectors.toList());

    if(!fetchAllData && !parametri.isEmpty())
    {
      // per ora il link è solo su campo singolo
      String fieldLink = arKeys.getKeyByIndex(0);
      StringJoin sj = StringJoin.build().add(parametri);

      if("INTEGER".equalsIgnoreCase(arKeys.getPairByIndex(0).second))
      {
        qc.addWhere(fieldLink + " IN (" + sj.join() + ")");
      }
      else
      {
        sj.setDelimitatore("\'");
        qc.addWhere(fieldLink + " IN (" + sj.join() + ")");
      }
    }

    List<Record> lsRecs = DbPeer.executeQuery(qc, databaseName);
    if(lsRecs.isEmpty())
      return;

    popolaTuttiRecords(lsRecs, arRealFields, v, context);
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    try(Connection con = Database.getConnection(databaseName))
    {
      try(QueryDataSet qds = qb.buildQueryDataset(con))
      {
        schema = qds.schema();
      }
    }

    if(schema == null)
      throw new SyncSetupErrorException(String.format(
         "Query non eseguibile sul database %s.", databaseName));

    caricaTipiColonne(schema, arFields);

    if(delStrategy != null)
      delStrategy.caricaTipiColonne(databaseName, tableName);
  }
}
