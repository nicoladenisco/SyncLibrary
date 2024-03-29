/*
 *  AdapterIdcodiciMaster.java
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
package org.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.Configuration;
import org.commonlib5.lambda.LEU;
import org.commonlib5.utils.ArrayOper;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.common.Utils;
import org.sync.common.plugin.AbstractAdapter;
import org.sync.db.DbPeer;

/**
 * Adapter per mappare id attraverso i codici.
 *
 * @author Nicola De Nisco
 */
public class AdapterIdcodiciMaster extends AbstractAdapter
{
  private Element data;
  private String table, idfield, codicefield;
  private final Map<Integer, String> mapLocalId = new HashMap<>();
  private final Map<Integer, String> mapSharedId = new HashMap<>();
  private final Map<String, Integer> mapSharedCodice = new HashMap<>();
  private HashtableRpc setup;

  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
  }

  @Override
  public String getRole()
  {
    return ROLE_MASTER;
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
  public void masterPreparaValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapLocalId.clear();
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

    if(arallid.length < limiteQueryParametri)
    {
      subQuery1(arallid);
      return;
    }

    // protezione per evitare che la clausola IN diventi troppo lunga
    List<List<Integer>> splitList = ArrayOper.splitList(ArrayOper.asList(arallid), limiteQueryParametri);
    for(List<Integer> list : splitList)
    {
      subQuery1(ArrayOper.toArrayInt(list));
    }
  }

  private void subQuery1(int[] arallid)
     throws Exception
  {
    String sSQL = "SELECT " + idfield + "," + codicefield
       + " FROM " + table
       + " WHERE " + idfield + " IN (" + join(arallid, ',') + ")";
    List<Record> lsr = DbPeer.executeQuery(sSQL, getDbname());

    for(Record r : lsr)
    {
      int id = r.getValue(1).asInt();
      String codice = r.getValue(2).asString();
      mapLocalId.put(id, codice);
    }
  }

  @Override
  public void masterFineValidazione(List<Record> lsRecs,
     List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapLocalId.clear();
  }

  @Override
  public Object masterValidaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    return okStr(mapLocalId.get(v.asInt()));
  }

  @Override
  public void masterSharedFetchData(List<Record> lsRecs, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    mapSharedId.clear();
    mapSharedCodice.clear();
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

    if(arallid.length < limiteQueryParametri)
    {
      subQuery2(arallid);
    }
    else
    {
      List<List<Integer>> splitList = ArrayOper.splitList(ArrayOper.asList(arallid), limiteQueryParametri);
      for(List<Integer> list : splitList)
      {
        subQuery2(ArrayOper.toArrayInt(list));
      }
    }

    // sostrituisce il valore nel record da int (ID) a stringa (CODICE)
    for(Record r : lsRecs)
    {
      int id = r.getValue(field.field.first).asInt();
      if(id != 0)
        r.setValue(field.field.first, mapSharedId.getOrDefault(id, ""));
    }
  }

  private void subQuery2(int[] arallid)
     throws Exception
  {
    String sSQL = "SELECT " + idfield + "," + codicefield
       + " FROM " + table
       + " WHERE " + idfield + " IN (" + join(arallid, ',') + ")";
    List<Record> lsr = DbPeer.executeQuery(sSQL, getDbname());

    // carica tutti i codici per gli ID richiesti
    for(Record r : lsr)
    {
      int id = r.getValue(1).asInt();
      String codice = r.getValue(2).asString();
      mapSharedId.put(id, codice);
      mapSharedCodice.put(codice, id);
    }
  }

  @Override
  public void masterSharedConvertKeys(
     List<String> parametri, FieldLinkInfoBean field, int idxInKeys, SyncContext context)
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

  private String getDbname()
  {
    return ((AgentGenericMaster) parentAgent).databaseName;
  }
}
