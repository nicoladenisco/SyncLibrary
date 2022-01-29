/*
 *  DeleteStrategy.java
 *  Creato il Jan 31, 2020, 6:40:20 PM
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
package it.infomed.sync.sincronizzazione.plugin;

import com.workingdogs.village.DataSetException;
import com.workingdogs.village.Record;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractDelete;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Cancellazione logica per stato_rec.
 *
 * @author Nicola De Nisco
 */
public class DeleteStrategyStatorec extends AbstractDelete
{
  private String databaseNameForeign, tableNameForeign;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
  }

  @Override
  public void caricaTipiColonne(String databaseName, String tableName)
     throws Exception
  {
    this.databaseNameForeign = databaseName;
    this.tableNameForeign = tableName;
  }

  @Override
  public void cancellaRecordsPerDelete(List<String> lsKeys, Map<String, String> arKeys, SyncContext context)
     throws Exception
  {
    cancellazioneLogica(lsKeys, arKeys, context);
  }

  @Override
  public void cancellaRecordsPerUnknow(List<String> lsKeys, Map<String, String> arKeys, SyncContext context)
     throws Exception
  {
    cancellazioneLogica(lsKeys, arKeys, context);
  }

  protected void cancellazioneLogica(List<String> lsKeys,
     Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    if(lsKeys.isEmpty())
      return;

    switch(arForeignKeys.size())
    {
      case 0:
        return;

      case 1:
        // versione ottimizzata per chiave singola
        String where = "";
        Map.Entry<String, String> fkey = arForeignKeys.entrySet().iterator().next();
        String nomeCampo = fkey.getKey();
        String tipoCampo = fkey.getValue();
        boolean numeric = Utils.isNumeric(tipoCampo);

        if(numeric)
          where = join(lsKeys.iterator(), ',');
        else
          where = join(lsKeys.iterator(), ',', '\'');

        String sSQL
           = "UPDATE " + tableNameForeign + "\n"
           + "   SET STATO_REC=10\n"
           + " WHERE " + nomeCampo + " IN (" + where + ")";
        DbPeer.executeStatement(sSQL, databaseNameForeign);
        break;

      default:
        // versione generica per chiavi multiple
        for(String key : lsKeys)
        {
          cancellaRecord(key, arForeignKeys, context);
        }
        break;
    }
  }

  protected void cancellaRecord(String key,
     Map<String, String> arForeignKeys, SyncContext context)
  {
    try
    {
      String sWhere = buildWhere(key, arForeignKeys, context);
      if(sWhere == null)
        return;

      String sSQL
         = "UPDATE " + tableNameForeign + "\n"
         + "   SET STATO_REC=10\n"
         + " WHERE " + sWhere;
      DbPeer.executeStatement(sSQL, databaseNameForeign);
    }
    catch(Exception ex)
    {
      log.error("Errore in cancellazione record " + key, ex);
    }
  }

  @Override
  public boolean confermaValoriRecord(Map r, String now, String key, Map<String, String> arKeys, Map<String, String> valoriSelect, Map<String, String> valoriUpdate, Map<String, String> valoriInsert, SyncContext context, Connection con)
     throws Exception
  {
    valoriInsert.put("STATO_REC", "0");
    valoriUpdate.put("STATO_REC", "0");
    return true;
  }

  @Override
  public boolean queryRecordDeleted(Record r, SyncContext context)
     throws Exception
  {
    try
    {
      return r.getValue("STATO_REC").asInt() >= 10;
    }
    catch(DataSetException e)
    {
      return false;
    }
  }
}
