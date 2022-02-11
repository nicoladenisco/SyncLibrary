/*
 *  AgentSharedGenericSlave.java
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
package it.infomed.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.common.*;
import it.infomed.sync.db.DatabaseException;
import it.infomed.sync.db.DbPeer;
import it.infomed.sync.db.SqlTransactAgent;
import it.infomed.sync.sincronizzazione.RuleRunner;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import static org.commonlib5.utils.StringOper.checkTrueFalse;
import static org.commonlib5.utils.StringOper.isEquNocase;
import static org.commonlib5.utils.StringOper.isOkStr;
import org.jdom2.Element;

/**
 * Classe base degli Agent con un concetto di campi condivisi e timestamp.
 *
 * @author Nicola De Nisco
 */
public class AgentSharedGenericSlave extends AgentGenericSlave
{
  protected Pair<String, String> timeStamp;
  protected ArrayMap<String, String> arKeys = new ArrayMap<>();
  protected boolean correct = false;
  protected String correctTableName = null;
  protected Map<String, Date> mapTimeStamps = new HashMap<>();
  protected boolean keysHaveAdapter = false;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element useTs;

    if((useTs = data.getChild("use-timestamps")) != null)
    {
      timeStamp = Utils.parseNameTypeIgnore(useTs.getChild(location));
      correct = checkTrueFalse(useTs.getAttributeValue("correct-" + location), correct);
      correctTableName = useTs.getAttributeValue("correct-table-" + location);
    }

    for(FieldLinkInfoBean f : arFields)
    {
      if(f.shared)
      {
        arKeys.add(f.field);

        // se le shared keys hanno almeno un adapter, richiede una conversione delle chiavi
        if(f.adapter != null)
          keysHaveAdapter = true;
      }
    }
  }

  protected void caricaTimestamps(String nomeTabella)
     throws Exception
  {
    mapTimeStamps.clear();

    String sSQL = "SELECT SHARED_KEY, LAST_UPDATE \n"
       + " FROM " + RuleRunner.SYNC_TIMESTAMP_TABLE + "\n"
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
    return timeStamp != null && !arKeys.isEmpty();
  }

  @Override
  protected void salvaRecord(Map r,
     String tableName, String databaseName,
     Map<String, Integer> lsNotNullFields, SyncContext context)
     throws Exception
  {
    if(haveTs())
      salvaRecordShared(r, tableName, databaseName, lsNotNullFields, context);
    else
      super.salvaRecord(r, tableName, databaseName, lsNotNullFields, context);
  }

  protected void salvaRecordShared(Map r,
     String tableName, String databaseName,
     Map<String, Integer> lsNotNullFields, SyncContext context)
     throws Exception
  {
    SqlTransactAgent.execute(databaseName, (con) -> salvaRecordShared(r,
       tableName, databaseName, lsNotNullFields, context, con));
  }

  protected void salvaRecordShared(Map r,
     String tableName, String databaseName,
     Map<String, Integer> lsNotNullFields, SyncContext context, Connection con)
     throws Exception
  {
    if(arKeys.isEmpty())
      die("Nessuna definizione di chiave per " + tableName + ": aggiornamento non possibile");

    try
    {
      boolean haveAutoTs = isEquNocase(timeStamp.first, "auto");
      String key = buildKey(r, arKeys);
      String now = DateTime.formatIsoFull(new Date());

      if(recordValidator != null)
        if(recordValidator.slaveValidaRecord(key, r, arFields, con) != 0)
          return;

      HashMap<String, String> valoriUpdate = new HashMap<>();
      HashMap<String, String> valoriSelect = new HashMap<>();
      HashMap<String, String> valoriInsert = new HashMap<>();

      if(!haveAutoTs)
      {
        valoriInsert.put(timeStamp.first, "'" + now + "'");
        valoriUpdate.put(timeStamp.first, "'" + now + "'");
        lsNotNullFields.remove(timeStamp.first.toUpperCase());
      }

      preparaValoriNotNull(lsNotNullFields, valoriInsert, now);

      if(!preparaValoriRecord(r,
         tableName, databaseName, key, now,
         valoriSelect, valoriUpdate, valoriInsert,
         con))
        return;

      if(delStrategy != null)
      {
        // converte chiave per shared con adapter
        String convertedKey = keysHaveAdapter ? convertiChiave(
           tableName, databaseName, key, context) : key;

        if(convertedKey != null)
        {
          // reimposta lo stato_rec al valore di non cancellato
          if(!delStrategy.confermaValoriRecord(r, now, key, arKeys,
             valoriSelect, valoriUpdate, valoriInsert, context,
             con))
            return;
        }
      }

      createOrUpdateRecord(con, tableName, valoriUpdate, valoriSelect, valoriInsert);

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
    return arKeys.containsKey(f.field.first) || f.primary;
  }

  protected void updateCalTimestamp(String tableName, String key, String now, Connection con)
     throws DatabaseException
  {
    // aggiornamento tabella dei timestamp; ATTENZIONE: la tabella esiste solo sul db principale
    String sSQL = ""
       + "UPDATE " + RuleRunner.SYNC_TIMESTAMP_TABLE + "\n"
       + "   SET LAST_UPDATE='" + now + "'\n"
       + " WHERE TABLE_NAME='" + tableName + "'\n"
       + "   AND SHARED_KEY='" + key + "'\n";
    if(DbPeer.executeStatement(sSQL, con) == 0)
    {
      sSQL = ""
         + "INSERT INTO " + RuleRunner.SYNC_TIMESTAMP_TABLE + "(TABLE_NAME, SHARED_KEY, LAST_UPDATE)\n"
         + " VALUES('" + tableName + "','" + key + "','" + now + "')\n";
      DbPeer.executeStatement(sSQL, con);
    }
  }

  protected String convertiChiave(
     String tableName, String databaseName, String key,
     SyncContext context)
     throws Exception
  {
    List<String> lsKeys = new ArrayList<>(1);
    lsKeys.add(key);

    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.slaveSharedConvertKeys(tableName, databaseName, lsKeys, f, i, context);
    }

    return lsKeys.isEmpty() ? null : lsKeys.get(0);
  }

  @Override
  protected void caricaTipiColonne(Schema schema)
     throws Exception
  {
    super.caricaTipiColonne(schema);

    if(timeStamp != null && !isOkStr(timeStamp.second) && isEquNocase(timeStamp.first, "AUTO"))
      timeStamp.second = findInSchema(timeStamp.first).type();

    for(Pair<String, String> f : arKeys.getAsList())
    {
      if(!isOkStr(f.second))
      {
        f.second = findInSchema(f.first).type();
      }
    }
  }
}
