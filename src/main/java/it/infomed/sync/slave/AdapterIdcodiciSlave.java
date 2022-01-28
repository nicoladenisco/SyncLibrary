/*
 *  AdapterIdcodiciSlave.java
 *  Creato il Nov 25, 2017, 5:13:26 PM
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

import com.workingdogs.village.QueryDataSet2;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import com.workingdogs.village.TableDataSet;
import it.infomed.sync.*;
import it.infomed.sync.plugins.AbstractAdapter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.commonlib5.lambda.LEU;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;
import org.rigel5.db.sql.SqlTransactAgent;

/**
 * Adapter per mappare id attraverso i codici.
 * Questa è l'implemetazione lato slave ovvero il codice viene trasformato nell'id
 * della tabella collegata (ES: sto aggiornado prestazioni, mi arriva il codice
 * della branca che va trasformato nell'id per poter essere inserito in prestazioni,
 * cercando codice nella tabella branche ed estraendone il relativo id).
 * Il master esegue una trasformazione speculare se richiesto.
 *
 * @author Nicola De Nisco
 */
public class AdapterIdcodiciSlave extends AbstractAdapter
{
  private Element data;
  private String table, DB, id, codice, defval;
  private HashMap<String, Integer> mapForeignCodici = new HashMap<>();
  private Map<Integer, String> mapSharedId = new HashMap<>();
  private Map<String, Integer> mapSharedCodice = new HashMap<>();
  private HashtableRpc setup;
  private Schema tableSchema;

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
  }

  @Override
  public void setXML(Location el, Element data)
     throws Exception
  {
    this.data = data;
    setup = Utils.parseParams(data);
    String name = data.getName();

    if(isEquNocaseAny(name, "adapter", el.name() + "-adapter"))
    {
      if((table = setup.getAsString(el.name() + "-table")) == null)
        throw new SyncSetupErrorException(0, "adapter/local-table");
      if((id = setup.getAsString(el.name() + "-id")) == null)
        throw new SyncSetupErrorException(0, "adapter/local-id");
      if((codice = setup.getAsString(el.name() + "-codice")) == null)
        throw new SyncSetupErrorException(0, "adapter/local-codice");
    }

    // parametri opzionali
    defval = setup.getAsString("defval");
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    ASSERT(field.foreignField.second != null, "field.foreignField.second != null");

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

    if(DB != null)
      dbName = DB;

    SqlTransactAgent ta = new SqlTransactAgent(true, dbName)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        if(tableSchema == null)
        {
          TableDataSet ts = new TableDataSet(dbCon, table);
          tableSchema = ts.schema();
        }

        boolean isNumeric = tableSchema.getColumn(codice).isTypeNumeric();
        List<Record> lsr = fetchData(isNumeric, arallcodici, dbCon);

        if(lsr != null)
        {
          for(Record r : lsr)
          {
            int id = r.getValue(1).asInt();
            String codice = r.getValue(2).asString();
            mapForeignCodici.put(codice, id);
          }
        }

        return true;
      }
    };
  }

  protected List<Record> fetchData(boolean isNumeric, String[] arallcodici, Connection con)
     throws Exception
  {
    String join;

    if(isNumeric)
    {
      int val;
      HashSet<Integer> lsCodici = new HashSet<>(arallcodici.length);
      for(String cod : arallcodici)
      {
        if((val = parse(cod, 0)) != 0)
          lsCodici.add(val);
      }
      join = join(lsCodici.iterator(), ',');
    }
    else
    {
      HashSet<String> lsCodici = new HashSet<>(arallcodici.length);
      for(String cod : arallcodici)
      {
        if(isOkStr(cod))
          lsCodici.add(cod);
      }
      join = join(lsCodici.iterator(), ',', '\'');
    }

    if(isOkStr(join))
    {
      String sSQL = "SELECT " + id + "," + codice
         + " FROM " + table
         + " WHERE " + codice + " IN (" + join + ")";

      return QueryDataSet2.fetchAllRecords(con, sSQL);
    }

    return null;
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
    Object val = mapForeignCodici.get(okStr(v));
    if(val != null)
      return val;

    if(defval != null)
      return defval;

    throw new SyncIgnoreRecordException("External Referenced Key Violated " + f.foreignField.first + "=" + v);
  }

  @Override
  public void slaveSharedFetchData(String uniqueName, String dbName, List<Record> lsRecs,
     FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    if(lsRecs.isEmpty())
      return;

    int[] arallid = lsRecs.stream()
       .mapToInt(LEU.rethrowFunctionInt((r) -> r.getValue(field.foreignField.first).asInt()))
       .filter((i) -> i != 0)
       .sorted()
       .distinct()
       .toArray();

    if(arallid.length == 0)
      return;

    String sSQL = "SELECT " + id + "," + codice
       + " FROM " + table
       + " WHERE " + id + " IN (" + join(arallid, ',') + ")";
    if(DB != null)
      dbName = DB;
    List<Record> lsr = SqlTransactAgent.executeReturn((con) -> QueryDataSet2.fetchAllRecords(con, sSQL));

    // carica tutti i codici per gli ID richiesti
    for(Record r : lsr)
    {
      int id = r.getValue(1).asInt();
      String codice = r.getValue(2).asString();
      mapSharedId.put(id, codice);
      mapSharedCodice.put(codice, id);
    }

    // sostrituisce il valore nel record da int (ID) a stringa (CODICE)
    for(Record r : lsRecs)
    {
      int id = r.getValue(field.foreignField.first).asInt();
      if(id != 0)
        r.setValue(field.foreignField.first, mapSharedId.getOrDefault(id, ""));
    }
  }

  @Override
  public void slaveSharedConvertKeys(String uniqueName, String dbName, List<String> parametri,
     FieldLinkInfoBean field, int idxInKeys, SyncContext context)
     throws Exception
  {
    ArrayList<String> newparam = new ArrayList<>(parametri.size());

    for(String s : parametri)
    {
      String[] ss = split(s, '^');
      Integer value = mapSharedCodice.get(ss[idxInKeys]);
      if(value != null)
      {
        ss[idxInKeys] = value.toString();
        newparam.add(join(ss, '^'));
      }
    }

    parametri.clear();
    parametri.addAll(newparam);
  }
}
