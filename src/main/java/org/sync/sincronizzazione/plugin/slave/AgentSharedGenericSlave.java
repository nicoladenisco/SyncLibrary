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
package org.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import com.workingdogs.village.VillageUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;
import org.sync.common.*;
import org.sync.db.DbPeer;
import org.sync.sincronizzazione.RuleRunner;

/**
 * Classe base degli Agent con un concetto di campi condivisi e timestamp.
 *
 * @author Nicola De Nisco
 */
public class AgentSharedGenericSlave extends AgentGenericSlave
{
  protected Pair<String, String> timeStamp;
  protected final ArrayMap<String, String> arKeys = new ArrayMap<>();
  protected boolean correct = false;
  public String correctTableName = null;
  protected final Map<String, Date> mapTimeStamps = new HashMap<>();
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
  protected boolean isSelect(FieldLinkInfoBean f)
  {
    return arKeys.containsKey(f.field.first) || f.primary;
  }

  protected boolean updateCalTimestamp(String key, Date now, Connection con)
     throws SQLException
  {
    // aggiornamento tabella dei timestamp; ATTENZIONE: la tabella esiste solo sul db principale
    String sSQL = ""
       + "UPDATE " + RuleRunner.SYNC_TIMESTAMP_TABLE + " \n"
       + "   SET LAST_UPDATE=? \n"
       + " WHERE TABLE_NAME=? \n"
       + "   AND SHARED_KEY=? \n";

    try (PreparedStatement stmt = con.prepareStatement(sSQL))
    {
      stmt.setTimestamp(1, VillageUtils.cvtTimestamp(now));
      stmt.setString(2, correctTableName);
      stmt.setString(3, key);
      if(stmt.executeUpdate() > 0)
        return true;
    }

    sSQL = ""
       + "INSERT INTO " + RuleRunner.SYNC_TIMESTAMP_TABLE + "(TABLE_NAME, SHARED_KEY, LAST_UPDATE)\n"
       + " VALUES(?,?,?)\n";

    try (PreparedStatement stmt = con.prepareStatement(sSQL))
    {
      stmt.setString(1, correctTableName);
      stmt.setString(2, key);
      stmt.setTimestamp(3, VillageUtils.cvtTimestamp(now));
      if(stmt.executeUpdate() > 0)
        return true;
    }

    return false;
  }

  /**
   * Converte una singola chiave attraverso l'adapter.
   * E' utilizzata quando un campo shared ha anche un adapter.
   * @param tableName
   * @param databaseName
   * @param key
   * @param context
   * @return
   * @throws Exception
   */
  protected String convertiChiave(String key, SyncContext context)
     throws Exception
  {
    List<String> lsKeys = new ArrayList<>(1);
    lsKeys.add(key);

    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.slaveSharedConvertKeys(lsKeys, f, i, context);
    }

    return lsKeys.isEmpty() ? null : lsKeys.get(0);
  }

  /**
   * Converte una lista di chiavi attraverso l'adapter.
   * E' utilizzata quando un campo shared ha anche un adapter.
   * @param tableName
   * @param databaseName
   * @param lsKeys
   * @param context
   * @throws Exception
   */
  protected void convertiChiavi(List<String> lsKeys, SyncContext context)
     throws Exception
  {
    for(int i = 0; i < arKeys.size(); i++)
    {
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
      if(f.adapter != null)
        f.adapter.slaveSharedConvertKeys(lsKeys, f, i, context);
    }
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
        Column col = findInSchema(f.first);
        // reimposta nome per avere il case corretto
        f.first = col.name();
        f.second = col.type();
      }
    }
  }
}
