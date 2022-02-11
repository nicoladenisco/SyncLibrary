/*
 *  AgentPoolUpdateMaster.java
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
import com.workingdogs.village.Schema;
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
 * Agent di sincronizzazione con utilizzo di un poolElement di dati.
 *
 * @author Nicola De Nisco
 */
public class AgentPoolUpdateMaster extends AgentSharedGenericMaster
{
  protected Element poolElement;
  protected String poolName, poolData;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((poolElement = tables.getChild("pool")) == null)
      throw new SyncSetupErrorException(0, "tables/pool");

    if((poolName = okStrNull(poolElement.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/pool:name");

    if((poolData = okStrNull(poolElement.getAttributeValue("data"))) == null)
      throw new SyncSetupErrorException(0, "tables/pool:data");
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

    Pair<List<Record>, Schema> dati = pool.getDatiVerifica(dataBlockName, poolData,
       oldTimestamp, arFields, (Map<String, String>) context.get("extraFilter"));
    if(dati.first.isEmpty())
      return;

    if(dati.second != null && schema != null)
      caricaTipiColonne(dati.second);

    // NOTA IMPORTANTE: il timestamp ultimo aggiornamento non viene passato
    // in quanto è responsabilità del poolElement stabilire quali record devono essere aggiornati
    compilaBloccoMaster(arKeys, parametri, dati.first,
       null, // vedi nota
       timeStamp == null ? null : timeStamp.first,
       v, context);
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

    SyncPoolPlugin pool = getParentRule().getPool(poolName);
    if(pool == null)
      die("Missing data pool for name " + poolName);

    Pair<List<Record>, Schema> dati = pool.getDatiAggiorna(dataBlockName, poolData, arKeys,
       fetchAllData ? null : parametri, arFields, (Map<String, String>) context.get("extraFilter"));
    List<Record> lsRecs = dati.first;
    if(lsRecs.isEmpty())
      return;

    if(dati.second != null && schema != null)
      caricaTipiColonne(dati.second);

    // se il poolElement non sa filtrare i records in base ai parametri, applichiamo un filtro a posteriori
    if(!fetchAllData && !pool.haveNativeFilter())
      lsRecs = filtraRecors(lsRecs, parametri, context);

    popolaTuttiRecords(dataBlockName, databaseName, lsRecs, arRealFields, v, context);
  }
}
