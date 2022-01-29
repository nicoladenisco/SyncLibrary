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
package it.infomed.sync.sincronizzazione.plugin.slave;

import com.workingdogs.village.Record;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncIgnoreRecordException;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractAdapter;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DbPeer;
import it.infomed.sync.db.SqlTransactAgent;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.commonlib5.lambda.LEU;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;

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
  private String table, idfield, codicefield;
  private HashMap<String, Integer> mapCodici = new HashMap<>();
  private Map<Integer, String> mapSharedId = new HashMap<>();
  private Map<String, Integer> mapSharedCodice = new HashMap<>();
  private HashtableRpc setup;

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
  }

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    this.data = data;
    setup = Utils.parseParams(data);
    String name = data.getName();

    if(isEquNocaseAny(name, "adapter", location + "adapter"))
    {
      if((table = setup.getAsString(location + "table")) == null)
        throw new SyncSetupErrorException(0, "adapter/" + location + "table");
      if((idfield = setup.getAsString(location + "id")) == null)
        throw new SyncSetupErrorException(0, "adapter/" + location + "id");
      if((codicefield = setup.getAsString(location + "codice")) == null)
        throw new SyncSetupErrorException(0, "adapter/" + location + "codice");
    }
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    ASSERT(field.field.second != null, "field.foreignField.second != null");

    mapCodici.clear();
    if(lsRecs.isEmpty())
      return;

    String[] arallcodici = lsRecs.stream()
       .map((r) -> (String) r.get(field.field.first))
       .filter((s) -> s != null)
       .sorted()
       .distinct()
       .toArray(String[]::new);

    if(arallcodici.length == 0)
      return;

    SqlTransactAgent ta = new SqlTransactAgent(true, dbName)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        boolean isNumeric = Database.isNumeric(dbCon, table, codicefield);
        List<Record> lsr = fetchData(isNumeric, arallcodici, dbCon);

        if(lsr != null)
        {
          for(Record r : lsr)
          {
            int id = r.getValue(1).asInt();
            String codice = r.getValue(2).asString();
            mapCodici.put(codice, id);
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
      for(String codice : arallcodici)
      {
        if((val = parse(codice, 0)) != 0)
          lsCodici.add(val);
      }
      join = join(lsCodici.iterator(), ',');
    }
    else
    {
      HashSet<String> lsCodici = new HashSet<>(arallcodici.length);
      for(String codice : arallcodici)
      {
        if(isOkStr(codice))
          lsCodici.add(codice);
      }
      join = join(lsCodici.iterator(), ',', '\'');
    }

    if(isOkStr(join))
    {
      String sSQL = "SELECT " + idfield + "," + codicefield
         + " FROM " + table
         + " WHERE " + codicefield + " IN (" + join + ")";

      return DbPeer.executeQuery(sSQL, false, con);
    }

    return null;
  }

  @Override
  public void slaveFineValidazione(String uniqueName, String dbName, List<Map> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapCodici.clear();
  }

  @Override
  public Object slaveValidaValore(String key, Map record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    Object val = mapCodici.get(okStr(v));
    if(val != null)
      return val;

    throw new SyncIgnoreRecordException("External Referenced Key Violated " + f.field.first + "=" + v);
  }

  @Override
  public void slaveSharedFetchData(String uniqueName, String dbName, List<Record> lsRecs,
     FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    if(lsRecs.isEmpty())
      return;

    int[] arallid = lsRecs.stream()
       .mapToInt(LEU.rethrowFunctionInt((r) -> r.getValue(field.field.first).asInt()))
       .filter((i) -> i != 0)
       .sorted()
       .distinct()
       .toArray();

    if(arallid.length == 0)
      return;

    String sSQL = "SELECT " + idfield + "," + codicefield
       + " FROM " + table
       + " WHERE " + idfield + " IN (" + join(arallid, ',') + ")";
    List<Record> lsr = DbPeer.executeQuery(sSQL, dbName);

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
      int id = r.getValue(field.field.first).asInt();
      if(id != 0)
        r.setValue(field.field.first, mapSharedId.getOrDefault(id, ""));
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
