/*
 *  AgentQueryUpdateLocalMaster.java
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
package it.infomed.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Record;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.db.DbPeer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.torque.criteria.SqlEnum;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;
import org.rigel5.SetupHolder;
import org.rigel5.db.sql.FiltroData;
import org.rigel5.db.sql.QueryBuilder;
import org.rigel5.table.RigelColumnDescriptor;
import org.sirio5.utils.SirioMacroResolver;

/**
 * Agent di sincronizzazione orientato alle query.
 *
 * @author Nicola De Nisco
 */
public class AgentQueryUpdateLocalMaster extends AgentSharedGenericLocalMaster
{
  protected Element queryLocal, tblForeign;
  protected String tableNameLocal, tableNameForeign, databaseName;
  protected QueryBuilder qb;
  protected SirioMacroResolver resolver = new SirioMacroResolver();
  protected boolean ignoreOldTimestamp;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((tblForeign = tables.getChild("foreign")) == null)
      throw new SyncSetupErrorException(0, "tables/foreign");

    if((tableNameForeign = okStrNull(tblForeign.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/foreign:name");

    if((queryLocal = tables.getChild("query-local")) == null)
      throw new SyncSetupErrorException(0, "query-local");

    preparaLimitiDate();

    qb = SetupHolder.getQueryBuilder();
    qb.setSelect(resolver.resolveMacro(okStr(queryLocal.getChildText("select"))));
    qb.setFrom(resolver.resolveMacro(okStr(queryLocal.getChildText("from"))));
    qb.setWhere(resolver.resolveMacro(okStr(queryLocal.getChildText("where"))));
    qb.setOrderby(resolver.resolveMacro(okStr(queryLocal.getChildText("orderby"))));
    qb.setGroupby(resolver.resolveMacro(okStr(queryLocal.getChildText("groupby"))));
    qb.setHaving(resolver.resolveMacro(okStr(queryLocal.getChildText("having"))));

    // la tabella locale non è obbligatoria: la fonte dati è comunque la query
    tableNameLocal = okStrNull(queryLocal.getAttributeValue("table-name"));

    // in casi particolari (join multiple) si deve ignorare l'ultima time stamp
    ignoreOldTimestamp = checkTrueFalse(queryLocal.getAttributeValue("ignoreOldTimestamp"), ignoreOldTimestamp);

    if(arLocalKeys.size() != 1)
      throw new SyncSetupErrorException("Questo agent supporta solo una chiave singola.");
  }

  protected void preparaLimitiDate()
     throws Exception
  {
    Date tlCompare, tlFetch;
    GregorianCalendar cal = new GregorianCalendar(1900, 1, 1);
    tlCompare = tlFetch = cal.getTime();

    if(filter != null)
    {
      if(filter.timeLimitCompare > 0)
      {
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitCompare);
        tlCompare = cal.getTime();
      }

      if(filter.timeLimitFetch > 0)
      {
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
        tlFetch = cal.getTime();
      }
    }

    resolver.putValue("TIME_LIMIT_DATE_COMPARE", DateTime.formatIsoFull(tlCompare));
    resolver.putValue("TIME_LIMIT_DATE_FETCH", DateTime.formatIsoFull(tlFetch));
    resolver.putValue("TIME_LIMIT_DATE", DateTime.formatIsoFull(tlFetch));
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

    if(!haveTs())
      return;

    FiltroData fd = new FiltroData();
    if(oldTimestamp != null && !ignoreOldTimestamp)
      fd.addWhere(RigelColumnDescriptor.PDT_STRING, timeStampLocal.first, SqlEnum.GREATER_THAN, DateTime.formatIsoFull(oldTimestamp));

    qb.setParametri(fd);
    String sSQL = qb.makeSQLstring();

    List<Record> lsRecs = DbPeer.executeQuery(sSQL, databaseName);
    if(lsRecs.isEmpty())
      return;

    compilaBloccoMaster(arLocalKeys, parametri, lsRecs, oldTimestamp, timeStampLocal.first, v, context);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    VectorRpc v = new VectorRpc();
    context.put("records-data", v);
    boolean fetchAllData = checkTrueFalse(context.get("fetch-all-data"), arLocalKeys.isEmpty());
    FiltroData fd = new FiltroData();

    // estrae i soli campi significativi
    List<FieldLinkInfoBean> arRealFields = arFields.stream()
       .filter((f) -> f.localField != null && isOkStr(f.localField.first))
       .collect(Collectors.toList());

    if(!fetchAllData && !parametri.isEmpty())
    {
      // per ora il link è solo su campo singolo
      String fieldLink = arLocalKeys.getKeyByIndex(0);
      if("INTEGER".equalsIgnoreCase(arLocalKeys.getPairByIndex(0).second))
        fd.addWhere(RigelColumnDescriptor.PDT_INTEGER, fieldLink, SqlEnum.IN, parametri);
      else
        fd.addWhere(RigelColumnDescriptor.PDT_STRING, fieldLink, SqlEnum.IN, parametri);
    }

    qb.setParametri(fd);
    String sSQL = qb.makeSQLstring();

    List<Record> lsRecs = DbPeer.executeQuery(sSQL, databaseName);
    if(lsRecs.isEmpty())
      return;

    popolaTuttiRecords(tableNameLocal, "caleido", lsRecs, arRealFields, v, context);
  }
}
