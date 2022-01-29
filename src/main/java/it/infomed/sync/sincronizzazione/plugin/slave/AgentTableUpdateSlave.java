/*
 *  AgentTableUpdateSlave.java
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
package it.infomed.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.common.*;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DbPeer;
import java.util.*;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Adapter di sincronizzazione orientato alle tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentTableUpdateSlave extends AgentSharedGenericSlave
{
  protected Element tblElement;
  protected String tableName, databaseName;
  protected Schema tableSchema;

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

    databaseName = okStr(data.getAttributeValue("database-name"), getParentRule().getDatabaseName());

    if(correctTableName == null)
      correctTableName = tableName;

    caricaTipiColonne();
  }

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
  }

  @Override
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    String fkeys = join(arKeys.keySet().iterator(), ",", null);

    String fwhere = "";
    for(Map.Entry<String, String> entry : arKeys.entrySet())
    {
      String nome = entry.getKey();
      String tipo = entry.getValue();

      if("STRING".equals(tipo))
        fwhere += " AND ((" + nome + " IS NOT NULL) AND (" + nome + "<> 'null'))";
      else
        fwhere += " AND (" + nome + " IS NOT NULL)";
    }
    if(!fwhere.isEmpty())
      fwhere = fwhere.substring(5);

    List<Record> lsRecs = haveTs() ? queryWithTimestamp(fkeys, fwhere, oldTimestamp)
                             : queryWithoutTimestamp(fkeys, fwhere);

    if(lsRecs.isEmpty())
      return;

    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.slaveSharedFetchData(tableName, databaseName, lsRecs, f, context);
    }

    if(haveTs())
      compilaBloccoSlave(arKeys, parametri, lsRecs, oldTimestamp, timeStamp, mapTimeStamps);
    else
      compilaBloccoSlave(arKeys, parametri, lsRecs, null, null, null);
  }

  protected List<Record> queryWithTimestamp(String fkeys, String fwhere, Date oldTimestamp)
     throws Exception
  {
    String sSQL;

    switch(timeStamp.first)
    {
      case "AUTO":
        caricaTimestamps(tableName);
      case "NONE":
        sSQL
           = "SELECT " + fkeys
           + "  FROM " + tableName
           + " WHERE " + fwhere;
        break;

      default:
        sSQL
           = "SELECT " + timeStamp.first + "," + fkeys
           + " FROM " + tableName
           + " WHERE " + fwhere;

        if(oldTimestamp != null)
          sSQL += " AND (" + timeStamp.first + " > '" + DateTime.formatIsoFull(oldTimestamp) + "')";

        break;
    }

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterFetch)
        sSQL += " AND (" + sql + ")";

      // se richiesto aggiunge limitazione temporale
      if(filter.timeLimitFetch != 0)
      {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
        Date firstDate = cal.getTime();
        sSQL += " AND (" + timeStamp.first + " >= '" + DateTime.formatIsoFull(firstDate) + "')";
      }
    }

    return DbPeer.executeQuery(sSQL, databaseName);
  }

  protected List<Record> queryWithoutTimestamp(String fkeys, String fwhere)
     throws Exception
  {
    String sSQL
       = "SELECT " + fkeys
       + "  FROM " + tableName
       + " WHERE " + fwhere;

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterFetch)
        sSQL += " AND (" + sql + ")";
    }

    return DbPeer.executeQuery(sSQL, databaseName);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    Vector v = (Vector) context.getNotNull("records-data");
    if(!v.isEmpty())
      salvaTuttiRecords(tableName, databaseName, tableSchema, v, context);
  }

  @Override
  public void pianificaAggiornamento(List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    int pos;
    List<String> delete = new ArrayList<>();
    List<String> unknow = new ArrayList<>();

    for(String s : vInfo)
    {
      if((pos = s.lastIndexOf('/')) != -1)
      {
        String key = s.substring(0, pos);
        String val = s.substring(pos + 1);

        switch(val)
        {
          case "NEW":
            aggiorna.add(key);
            break;

          case "UPDATE":
            aggiorna.add(key);
            break;

          case "DELETE":
            delete.add(key);
            break;

          case "UNKNOW":
            unknow.add(key);
            break;
        }
      }
    }

    if(delStrategy == null)
      return;

    if(delete.isEmpty() && unknow.isEmpty())
      return;

    if(!delete.isEmpty())
    {
      convertiChiavi(delete, context);
      delStrategy.cancellaRecordsPerDelete(unknow, arKeys, context);
    }

    if(!unknow.isEmpty())
    {
      convertiChiavi(unknow, context);
      delStrategy.cancellaRecordsPerUnknow(unknow, arKeys, context);
    }
  }

  protected void convertiChiavi(List<String> lsKeys, SyncContext context)
     throws Exception
  {
    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.slaveSharedConvertKeys(tableName, databaseName, lsKeys, f, i, context);
    }
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    tableSchema = Database.schemaTable(databaseName, tableName);

    if(tableSchema == null)
      throw new SyncSetupErrorException(String.format(
         "Tabella %s non trovata nel database %s.", tableName, databaseName));

    if(timeStamp != null && !isOkStr(timeStamp.second) && isEquNocase(timeStamp.first, "AUTO"))
      timeStamp.second = findInSchema(timeStamp.first).type();

    for(FieldLinkInfoBean f : arFields)
    {
      if(!isOkStr(f.field.second))
      {
        f.field.second = findInSchema(f.field.first).type();
      }
    }

    for(Pair<String, String> f : arKeys.getAsList())
    {
      if(!isOkStr(f.second))
      {
        f.second = findInSchema(f.first).type();
      }
    }

    if(delStrategy != null)
      delStrategy.caricaTipiColonne(databaseName, tableName);
  }

  protected Column findInSchema(String nomeColonna)
     throws Exception
  {
    return findInSchema(tableSchema, nomeColonna);
  }
}
