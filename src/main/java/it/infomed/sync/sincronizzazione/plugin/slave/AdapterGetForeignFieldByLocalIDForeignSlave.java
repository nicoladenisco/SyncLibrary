/*
 *  AdapterGetForeignFieldByLocalIDForeignSlave.java
 *  Creato il May 10, 2018, 6:02:07 PM
 *
 *  Copyright (C) 2018 Informatica Medica s.r.l.
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
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncErrorException;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.plugin.AbstractAdapter;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DbPeer;
import it.infomed.sync.db.SqlTransactAgent;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Nicola De Nisco
 */
public class AdapterGetForeignFieldByLocalIDForeignSlave extends AbstractAdapter
{
  private String localTable, foreignTable, localId, foreignField, localCodice, foreignTipo, foreignCodice, defaultValue;
  private String foreignCondition, foreignConditionValue;
  private HashMap<String, String> mapForeignCodici = new HashMap<>();
  private Map setup;

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
  }

  @Override
  public void setConfig(String nomeAdapter, Map data)
     throws Exception
  {
    setup = data;

    if((localTable = (String) setup.get("local-table")) == null)
      throw new SyncSetupErrorException(0, "adapter/local-table");
    if((localId = (String) setup.get("local-id")) == null)
      throw new SyncSetupErrorException(0, "adapter/local-id");
    if((localCodice = (String) setup.get("local-codice")) == null)
      throw new SyncSetupErrorException(0, "adapter/local-codice");

    if((foreignTable = (String) setup.get("foreign-table")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-table");
    if((foreignField = (String) setup.get("foreign-id")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-id");
    if((foreignCodice = (String) setup.get("foreign-codice")) == null)
      throw new SyncSetupErrorException(0, "adapter/foreign-codice");

    foreignCondition = (String) setup.get("foreign-condition");
    foreignConditionValue = (String) setup.get("foreign-condition-value");

    if((defaultValue = (String) setup.get("defval")) == null)
      throw new SyncSetupErrorException(0, "adapter/defval");
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapForeignCodici.clear();
    if(lsRecs.isEmpty())
      return;

    String[] arallcodici = lsRecs.stream()
       .map((r) -> (String) r.get(field.foreignField.first))
       .filter((s) -> s != null)
       .sorted()
       .distinct()
       .toArray(String[]::new);

    if(arallcodici.length == 0)
      return;

    List<String> lsCodici = new ArrayList<>();
    boolean isNumeric = false;

    //if(Database.isNumeric(Utils.getSqlType(field.foreignField.second)))
    if(SqlTransactAgent.executeReturn((con) -> Database.isNumeric(con, foreignTable, foreignCodice)))
    {
      isNumeric = true;
      for(String codice : arallcodici)
      {
        if(!"0".equals(codice))
          lsCodici.add(codice);
      }
    }
    else
    {
      for(String codice : arallcodici)
      {
        //if(!"0".equals(codice))
        if(null == foreignConditionValue)
          lsCodici.add(codice);
        else
          lsCodici.add(codice + "_" + foreignConditionValue);
      }
    }

    String join = "";
    if(isNumeric)
      join = join(lsCodici.iterator(), ',');
    else
      join = join(lsCodici.iterator(), ',', '\'');

    if(isOkStr(join))
    {
      String sSQL = "SELECT " + foreignField + "," + foreignCodice
         + " FROM " + foreignTable
         + " WHERE " + foreignCodice + " IN (" + join + ")";
      if(foreignCondition != null)
        sSQL += " AND " + foreignCondition + "='" + foreignConditionValue + "'";

      List<Record> lsr = DbPeer.executeQuery(sSQL, dbName);

      for(Record r : lsr)
      {
        String id = r.getValue(1).asString();
        String codice = r.getValue(2).asString();
        mapForeignCodici.put(codice, id);
      }
    }
  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapForeignCodici.clear();
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {

    String val;
    Object tmp;
    if(null == foreignConditionValue)
      tmp = v;
    else
      tmp = v + "_" + foreignConditionValue;

    val = mapForeignCodici.get(okStr(tmp));
    //  return val == null ? defaultValue : val;

    if(val != null)
      return val;
    else
      throw new SyncErrorException("External Referenced Key Violated " + f.foreignField.first);
  }
}
