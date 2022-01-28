/*
 *  AgentSharedGenericForeignSlave.java
 *  Creato il Dec 1, 2017, 10:27:24 AM
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

import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.*;
import it.infomed.sync.db.DatabaseException;
import it.infomed.sync.db.DbPeer;
import it.infomed.sync.db.SqlTransactAgent;
import it.infomed.sync.sincronizzazione.CaleidoSyncService;
import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.commonlib.utils.ArrayMap;
import org.commonlib.utils.DateTime;
import org.commonlib.utils.Pair;

/**
 * Classe base degli Agent con un concetto di campi condivisi e timestamp.
 *
 * @author Nicola De Nisco
 */
public class AgentSharedGenericForeignSlave extends AgentGenericSlave
{
  protected ArrayMap<String, String> arForeignKeys = new ArrayMap<>();
  protected Pair<String, String> timeStampForeign;
  protected Map<String, Date> mapTimeStamps = new HashMap<>();

  @Override
  public void setConfig(String nomeAgent, Map vData)
     throws Exception
  {
    super.setConfig(nomeAgent, vData);

    timeStampForeign = Utils.parseNameTypeIgnore((Map) vData.get("foreign-timestamp"));
    Utils.parseNameTypeVectorIgnore((List) vData.get("foreign-shared"), arForeignKeys);
  }

  protected void caricaTimestamps(String nomeTabella)
     throws Exception
  {
    mapTimeStamps.clear();

    String sSQL = "SELECT SHARED_KEY, LAST_UPDATE \n"
       + " FROM " + CaleidoSyncService.SYNC_TIMESTAMP_TABLE + "\n"
       + " WHERE TABLE_NAME='" + nomeTabella + "'\n";

    List<Record> lsRecs = DbPeer.executeQuery(sSQL);
    for(Record r : lsRecs)
    {
      String key = r.getValue(1).asString();
      Date ts = r.getValue(2).asUtilDate();
      mapTimeStamps.put(key, ts);
    }
  }

  public boolean haveTs()
  {
    return timeStampForeign != null && !arForeignKeys.isEmpty();
  }

  @Override
  protected void salvaRecord(Map r,
     String tableName, String databaseName, Schema tableSchema,
     Map<String, Integer> lsNotNullFields, SyncContext context)
     throws Exception
  {
    if(haveTs())
      salvaRecordShared(r, tableName, databaseName, tableSchema, lsNotNullFields, context);
    else
      super.salvaRecord(r, tableName, databaseName, tableSchema, lsNotNullFields, context);
  }

  protected void salvaRecordShared(Map r,
     String tableName, String databaseName, Schema tableSchema,
     Map<String, Integer> lsNotNullFields, SyncContext context)
     throws Exception
  {
    SqlTransactAgent.execute(databaseName, (con) -> salvaRecordShared(r,
       tableName, databaseName, tableSchema, lsNotNullFields, con));
  }

  protected void salvaRecordShared(Map r,
     String tableName, String databaseName, Schema tableSchema,
     Map<String, Integer> lsNotNullFields, Connection con)
     throws Exception
  {
    if(arForeignKeys.isEmpty())
      die("Nessuna definizione di chiave primaria per " + tableName + ": aggiornamento non possibile");

    try
    {
      boolean haveAutoTs = isEquNocase(timeStampForeign.first, "auto");
      String key = buildKey(r, arForeignKeys);
      String now = DateTime.formatIsoFull(new Date());

      if(foreignRecordValidator != null)
        if(foreignRecordValidator.slaveValidaRecord(key, r, arFields, con) != 0)
          return;

      HashMap<String, String> valoriUpdate = new HashMap<>();
      HashMap<String, String> valoriSelect = new HashMap<>();

      if(!haveAutoTs)
      {
        valoriUpdate.put(timeStampForeign.first, "'" + now + "'");
        lsNotNullFields.remove(timeStampForeign.first.toUpperCase());
      }

      // reimposta lo stato_rec al valore di non cancellato
      if(delStrategy != null && !delStrategy.sqlUpdateDeleteStatement.isEmpty())
      {
        for(DeleteStrategy.DeleteField f : delStrategy.sqlUpdateDeleteStatement)
        {
          if(isOkStr(f.valueNormal) && findInSchema(tableSchema, f.field.first) != null)
          {
            valoriUpdate.put(f.field.first, convertValue(f.valueNormal, f.field));
            lsNotNullFields.remove(f.field.first.toUpperCase());
          }
        }
      }

      if(!preparaValoriRecord(r, tableName, databaseName, tableSchema, key, now, lsNotNullFields, valoriSelect, valoriUpdate, con))
        return;

      createOrUpdateRecord(con, tableName, valoriUpdate, valoriSelect);

      if(haveAutoTs)
        updateCalTimestamp(tableName, key, now, con);
    }
    catch(SyncIgnoreRecordException e)
    {
      // un adapter o altra ha esplicitamente bloccato l'inserimento di questo record
      if(log.isDebugEnabled())
        log.info(e.getMessage() + " ignore record " + r.toString());
      else
        log.info(e.getMessage());
    }
    catch(SyncErrorException | DatabaseException e)
    {
      // this exception avoids the insert or update on db because the external key reference is violated
      // but it doesn't stop the elaboration of all other records
      log.error(e.getMessage() + " Unable to import the record " + r.toString());
    }
  }

  @Override
  protected boolean isSelect(FieldLinkInfoBean f)
  {
    return arForeignKeys.containsKey(f.foreignField.first) || f.primary;
  }

  protected void updateCalTimestamp(String tableName, String key, String now, Connection con)
     throws DatabaseException
  {
    // aggiornamento tabella dei timestamp; ATTENZIONE: la tabella esiste solo sul db principale
    String sSQL = ""
       + "UPDATE " + CaleidoSyncService.SYNC_TIMESTAMP_TABLE + "\n"
       + "   SET LAST_UPDATE='" + now + "'\n"
       + " WHERE TABLE_NAME='" + tableName + "'\n"
       + "   AND SHARED_KEY='" + key + "'\n";
    if(DbPeer.executeStatement(sSQL, con) == 0)
    {
      sSQL = ""
         + "INSERT INTO " + CaleidoSyncService.SYNC_TIMESTAMP_TABLE + "(TABLE_NAME, SHARED_KEY, LAST_UPDATE)\n"
         + " VALUES('" + tableName + "','" + key + "','" + now + "')\n";
      DbPeer.executeStatement(sSQL, con);
    }
  }
}
