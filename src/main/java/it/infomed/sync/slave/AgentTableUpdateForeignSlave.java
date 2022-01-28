/*
 *  AgentTableUpdateForeignSlave.java
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
package it.infomed.sync.slave;

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.*;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DbPeer;
import java.util.*;
import org.commonlib.utils.DateTime;
import org.commonlib.utils.Pair;
import static org.commonlib.utils.StringOper.join;

/**
 * Adapter di sincronizzazione orientato alle tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentTableUpdateForeignSlave extends AgentSharedGenericForeignSlave
{
  protected String tableNameForeign, databaseNameForeign;
  protected Schema tableSchema;

  @Override
  public void setConfig(String nomeAgent, Map vData)
     throws Exception
  {
    super.setConfig(nomeAgent, vData);

    tableNameForeign = (String) vData.get("foreign-table-name");
    databaseNameForeign = (String) vData.get("foreign-table-database");

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
    String fkeys = join(arForeignKeys.keySet().iterator(), ",", null);

    String fwhere = "";
    for(Map.Entry<String, String> entry : arForeignKeys.entrySet())
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

    for(int i = 0; i < arForeignKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arForeignKeys.getKeyByIndex(i));
      if(f.foreignAdapter != null)
        f.foreignAdapter.slaveSharedFetchData(tableNameForeign, databaseNameForeign, lsRecs, f, context);
      if(f.adapter != null)
        f.adapter.slaveSharedFetchData(tableNameForeign, databaseNameForeign, lsRecs, f, context);
    }

    if(haveTs())
      compilaBloccoSlave(arForeignKeys, parametri, lsRecs, oldTimestamp, timeStampForeign, mapTimeStamps);
    else
      compilaBloccoSlave(arForeignKeys, parametri, lsRecs, null, null, null);
  }

  protected List<Record> queryWithTimestamp(String fkeys, String fwhere, Date oldTimestamp)
     throws Exception
  {
    String sSQL;

    switch(timeStampForeign.first)
    {
      case "AUTO":
        caricaTimestamps(tableNameForeign);
      case "NONE":
        sSQL
           = "SELECT " + fkeys
           + "  FROM " + tableNameForeign
           + " WHERE " + fwhere;
        break;

      default:
        sSQL
           = "SELECT " + timeStampForeign.first + "," + fkeys
           + " FROM " + tableNameForeign
           + " WHERE " + fwhere;

        if(oldTimestamp != null)
          sSQL += " AND (" + timeStampForeign.first + " > '" + DateTime.formatIsoFull(oldTimestamp) + "')";

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
        sSQL += " AND (" + timeStampForeign.first + " >= '" + DateTime.formatIsoFull(firstDate) + "')";
      }
    }

    return DbPeer.executeQuery(sSQL, databaseNameForeign);
  }

  protected List<Record> queryWithoutTimestamp(String fkeys, String fwhere)
     throws Exception
  {
    String sSQL
       = "SELECT " + fkeys
       + "  FROM " + tableNameForeign
       + " WHERE " + fwhere;

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterFetch)
        sSQL += " AND (" + sql + ")";
    }

    return DbPeer.executeQuery(sSQL, databaseNameForeign);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    Vector v = (Vector) context.getNotNull("records-data");
    if(!v.isEmpty())
      salvaTuttiRecords(tableNameForeign, databaseNameForeign, tableSchema, v, context);
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
            if(delStrategy != null && !"nothing".equalsIgnoreCase(delStrategy.tipoDelete))
              delete.add(key);
            break;

          case "UNKNOW":
            if(delStrategy != null && !"nothing".equalsIgnoreCase(delStrategy.tipoUnknow))
              unknow.add(key);
            break;
        }
      }
    }

    if(!delete.isEmpty())
      cancellaRecords(delete, delStrategy.tipoDelete, delStrategy.sqlUpdateDeleteStatement, delStrategy.sqlGenericDeleteStatement, context);
    if(!unknow.isEmpty())
      cancellaRecords(unknow, delStrategy.tipoUnknow, delStrategy.sqlUpdateUnknowStatement, delStrategy.sqlGenericUnknowStatement, context);
  }

  protected void cancellaRecords(List<String> lsKeys, String tipo,
     List<DeleteStrategy.DeleteField> sqlUpdateStatement, List<String> sqlGenericStatement, SyncContext context)
  {
    try
    {
      if(lsKeys.isEmpty())
        return;

      for(int i = 0; i < arForeignKeys.size(); i++)
      {
        FieldLinkInfoBean f = findField(arForeignKeys.getKeyByIndex(i));
        if(f.foreignAdapter != null)
          f.foreignAdapter.slaveSharedConvertKeys(tableNameForeign, databaseNameForeign, lsKeys, f, i, context);
        if(f.adapter != null)
          f.adapter.slaveSharedConvertKeys(tableNameForeign, databaseNameForeign, lsKeys, f, i, context);
      }

      switch(okStr(tipo).toLowerCase())
      {
        case "":
        case "nothing":
          return;

        case "logical":
          cancellazioneLogica(lsKeys, sqlUpdateStatement, sqlGenericStatement, context);
          return;

        default:
          die("Questo agent supporta solo la cancellazione logica.");
      }
    }
    catch(Exception ex)
    {
      log.error("Errore cancellando blocco di records.", ex);
    }
  }

  protected void cancellazioneLogica(List<String> lsKeys,
     List<DeleteStrategy.DeleteField> sqlUpdateStatement, List<String> sqlGenericStatement, SyncContext context)
     throws Exception
  {
    if(lsKeys.isEmpty())
      return;

    String updateStm = join(sqlUpdateStatement,
       (f) -> findInSchema(f.field.first) == null ? "" : f.field.first + "=" + convertValue(f.valueDelete, f.field),
       ",", null);

    switch(arForeignKeys.size())
    {
      case 0:
        return;

      case 1:
        // versione ottimizzata per chiave singola
        Pair<String, String> fkey = arForeignKeys.getPairByIndex(0);
        boolean numeric = Utils.isNumeric(fkey.second);

        if(!updateStm.isEmpty())
        {
          String where = "";
          if(numeric)
            where = join(lsKeys.iterator(), ',');
          else
            where = join(lsKeys.iterator(), ',', '\'');

          String sSQL
             = "UPDATE " + tableNameForeign + "\n"
             + "   SET " + updateStm + "\n"
             + " WHERE " + fkey.first + " IN (" + where + ")";
          DbPeer.executeStatement(sSQL, databaseNameForeign);
        }

        if(!sqlGenericStatement.isEmpty())
        {
          for(String key : lsKeys)
          {
            String sWhere = fkey.first + "=" + (numeric ? key : "'" + key + "'");

            for(String s : sqlGenericStatement)
            {
              if(s.isEmpty())
                continue;

              s = s.replace("${key}", sWhere);
              DbPeer.executeStatement(s, databaseNameForeign);
            }
          }
        }
        break;

      default:
        // versione generica per chiavi multiple
        for(String key : lsKeys)
        {
          cancellaRecord(key, updateStm, sqlGenericStatement);
        }
        break;
    }
  }

  protected void cancellaRecord(String key, String updateStm, List<String> sqlGenericStatement)
  {
    try
    {
      String sWhere = buildWhere(key);
      if(sWhere == null)
        return;

      if(!updateStm.isEmpty())
      {
        String sSQL
           = "UPDATE " + tableNameForeign + "\n"
           + "   SET " + updateStm + "\n"
           + " WHERE " + sWhere;
        DbPeer.executeStatement(sSQL, databaseNameForeign);
      }

      if(!sqlGenericStatement.isEmpty())
      {
        for(String s : sqlGenericStatement)
        {
          if(s.isEmpty())
            continue;

          s = s.replace("${key}", sWhere);
          DbPeer.executeStatement(s, databaseNameForeign);
        }
      }
    }
    catch(Exception ex)
    {
      log.error("Errore in cancellazione record " + key, ex);
    }
  }

  private String buildWhere(String key)
     throws Exception
  {
    StringBuilder sb = new StringBuilder();
    Map<String, String> keyMap = reverseKey(key, arForeignKeys);
    if(keyMap == null)
      return null;

    for(Map.Entry<String, String> entry : keyMap.entrySet())
    {
      String nome = entry.getKey();
      String valore = entry.getValue();
      String tipo = arForeignKeys.get(nome);
      if(tipo == null || valore == null)
        return null;

      sb.append(" AND ").append(nome).append("=").append(convertValue(valore, tipo));
    }

    return sb.length() == 0 ? null : sb.substring(5);
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    tableSchema = Database.schemaTable(databaseNameForeign, tableNameForeign);

    if(tableSchema == null)
      throw new SyncSetupErrorException(String.format(
         "Tabella %s non trovata nel database %s.", tableNameForeign, databaseNameForeign));

    if(timeStampForeign != null && !isOkStr(timeStampForeign.second) && isEquNocase(timeStampForeign.first, "AUTO"))
      timeStampForeign.second = findInSchema(timeStampForeign.first).type();

    for(FieldLinkInfoBean f : arFields)
    {
      if(!isOkStr(f.foreignField.second))
      {
        f.foreignField.second = findInSchema(f.foreignField.first).type();
      }
    }

    for(Pair<String, String> f : arForeignKeys.getAsList())
    {
      if(!isOkStr(f.second))
      {
        f.second = findInSchema(f.first).type();
      }
    }

    if(delStrategy != null)
    {
      for(DeleteStrategy.DeleteField f : delStrategy.sqlUpdateDeleteStatement)
      {
        if(!isOkStr(f.field.second))
        {
          f.field.second = findInSchema(f.field.first).type();
        }
      }
      for(DeleteStrategy.DeleteField f : delStrategy.sqlUpdateUnknowStatement)
      {
        if(!isOkStr(f.field.second))
        {
          f.field.second = findInSchema(f.field.first).type();
        }
      }
    }
  }

  protected Column findInSchema(String nomeColonna)
     throws Exception
  {
    return findInSchema(tableSchema, nomeColonna);
  }
}
