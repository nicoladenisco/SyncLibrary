/*
 *  AgentPoolUpdateLocalMaster.java
 *  Creato il Mar 12, 2020, 11:44:49 AM
 *
 *  Copyright (C) 2020 Informatica Medica s.r.l.
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
import it.infomed.sync.common.plugin.SyncPoolPlugin;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;

/**
 * Agent di sincronizzazione con utilizzo di un pool di dati.
 *
 * @author Nicola De Nisco
 */
public class AgentPoolUpdateLocalMaster extends AgentSharedGenericLocalMaster
{
  protected Element poolLocal, tblForeign;
  protected String tableNameForeign, poolName, poolData;

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

    if((poolLocal = tables.getChild("pool-local")) == null)
      throw new SyncSetupErrorException(0, "tables/pool-local");

    if((poolName = okStrNull(poolLocal.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/pool-local:name");

    if((poolData = okStrNull(poolLocal.getAttributeValue("data"))) == null)
      throw new SyncSetupErrorException(0, "tables/pool-local:data");
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

    SyncPoolPlugin pool = getParentRule().getPool(poolName);
    if(pool == null)
      die("Missing data pool for name " + poolName);

    List<Record> lsRecs = pool.getDatiVerifica(dataBlockName, poolData,
       oldTimestamp, arFields, (Map<String, String>) context.get("extraFilter"));
    if(lsRecs.isEmpty())
      return;

    // NOTA IMPORTANTE: il timestamp ultimo aggiornamento non viene passato
    // in quanto è responsabilità del pool stabilire quali record devono essere aggiornati
    compilaBloccoMaster(arLocalKeys, parametri, lsRecs,
       null, // vedi nota
       timeStampLocal == null ? null : timeStampLocal.first,
       v, context);
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

    SyncPoolPlugin pool = getParentRule().getPool(poolName);
    if(pool == null)
      die("Missing data pool for name " + poolName);

    List<Record> lsRecs = pool.getDatiAggiorna(dataBlockName, poolData, arLocalKeys,
       fetchAllData ? null : parametri, arFields, (Map<String, String>) context.get("extraFilter"));
    if(lsRecs.isEmpty())
      return;

    // se il pool non sa filtrare i records in base ai parametri, applichiamo un filtro a posteriori
    if(!fetchAllData && !pool.haveNativeFilter())
      lsRecs = filtraRecors(lsRecs, parametri, context);

    popolaTuttiRecords(dataBlockName, "caleido", lsRecs, arRealFields, v, context);
  }
}
