/*
 *  AdapterGetFieldMax.java
 *  Creato il Nov 28, 2017, 5:04:37 PM
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
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.plugin.AbstractAdapter;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Adapter per generazione valore computando il massimo di un campo trai record che verificano la condizione in input.
 * Questo adapter può essere utilizzato sia come master che come slave.
 * @author N Romano
 */
public class AdapterGetFieldMaxUsingConditionForeignSlave extends AbstractAdapter
{
  protected String tableName, dbName;
  private Element data;
  //private String localTable, foreignTable, localId, foreignField, localCodice, foreignCodice, defaultValue;
  private String condition1, condition2, condition3, condition3Value;
  private HashMap<Integer, String> mapLocalCodici = new HashMap<>();
  private Map setup;
  private Hashtable<String, Integer> progressivi;

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
    progressivi = new Hashtable<String, Integer>();

    if((condition1 = (String) setup.get("condition-field1")) == null)
      throw new SyncSetupErrorException(0, "condition-field1");
    if((condition2 = (String) setup.get("condition-field2")) == null)
      throw new SyncSetupErrorException(0, "condition-field2");
    if((condition3 = (String) setup.get("condition-field3")) == null)
      throw new SyncSetupErrorException(0, "condition-field3");
    if((condition3Value = (String) setup.get("condition-field3-value")) == null)
      throw new SyncSetupErrorException(0, "condition-field3-value");
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = uniqueName;
    this.dbName = dbName;

  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    String cond1, cond3, cond3Value;
    int cond2;
    cond1 = (String) record.get(condition1);
    cond2 = (int) record.get(condition2);
    String sSQL
       = " SELECT " + f.foreignField.first + " FROM " + tableName
       + " WHERE " + condition1 + " = '" + cond1 + "' AND " + condition2 + "='" + cond2 + "'";
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);
    if(lsRecs.isEmpty())
    {
      sSQL
         = "SELECT MAX(" + f.foreignField.first + ") FROM " + tableName
         + " WHERE " + condition1 + " = '" + cond1 + "' AND " + condition3 + "='" + condition3Value + "'";
      lsRecs = DbPeer.executeQuery(sSQL, dbName, true, con);

      if(lsRecs.isEmpty())
        return 1;

      int size = lsRecs.size();

      if("null".equals(lsRecs.get(0).getValue(1).asString()))
        return 1;
      else
        return lsRecs.get(0).getValue(1).asLong() + 1;
    }
    else
      return lsRecs.get(0).getValue(1).asLong();
  }
}
