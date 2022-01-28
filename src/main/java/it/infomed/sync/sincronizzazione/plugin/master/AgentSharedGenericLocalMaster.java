/*
 *  AgentSharedGenericLocalMaster.java
 *  Creato il Nov 30, 2017, 7:46:12 PM
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
package it.infomed.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.Utils;
import java.util.*;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;

/**
 * Classe base degli Agent con un concetto di campi condivisi e timestamp.
 *
 * @author Nicola De Nisco
 */
public class AgentSharedGenericLocalMaster extends AgentGenericLocalMaster
{
  protected Pair<String, String> timeStampLocal, timeStampForeign;
  protected ArrayMap<String, String> arLocalKeys = new ArrayMap<>();
  protected ArrayMap<String, String> arForeignKeys = new ArrayMap<>();
  protected boolean correctLocal = false;
  protected String correctTableName = null;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(data);

    Element useTs;

    if((useTs = data.getChild("use-timestamps")) != null)
    {
      timeStampLocal = Utils.parseNameTypeIgnore(useTs.getChild("local"));
      timeStampForeign = Utils.parseNameTypeIgnore(useTs.getChild("foreign"));
      correctLocal = checkTrueFalse(useTs.getAttributeValue("correct-local"), correctLocal);
      correctTableName = useTs.getAttributeValue("table");
    }

    for(FieldLinkInfoBean f : arFields)
    {
      if(f.shared)
      {
        arLocalKeys.add(f.localField);
        arForeignKeys.add(f.foreignField);
      }
    }
  }

  @Override
  public void populateConfigForeign(Map hr)
     throws Exception
  {
    super.populateConfigForeign(hr);

    if(timeStampForeign != null)
      hr.put("foreign-timestamp", Utils.createNameTypeMap(timeStampForeign, "name", "type"));

    if(!arForeignKeys.isEmpty())
      hr.put("foreign-shared", Utils.createNameTypeVector(arForeignKeys, "name", "type"));
  }

  public boolean haveTs()
  {
    return timeStampLocal != null && !arLocalKeys.isEmpty();
  }

  @Override
  protected HashtableRpc popolaRecord(Record r, List<FieldLinkInfoBean> arRealFields, HashtableRpc hr)
     throws Exception
  {
    String key = buildKey(r, arLocalKeys);
    if(localRecordValidator != null)
      if(localRecordValidator.masterValidaRecord(key, r, arRealFields) != 0)
        return null;

    for(int i = 0; i < arRealFields.size(); i++)
    {
      FieldLinkInfoBean f = arRealFields.get(i);
      Value valore = r.getValue(f.localField.first);
      if(valore.isNull())
        continue;

      // attenzione: salva il valore con il nome remoto del campo
      hr.put(f.foreignField.first, popolaValore(key, r, valore, f));
    }

    return hr;
  }

  /**
   * Filtra record per i parametri passati.
   * @param lsRecs lista di record in input
   * @param parametri valori delle chiavi da verificare
   * @param context contesto di sincronizzazione
   * @return lista di record corrispondenti alle chiavi
   * @throws Exception
   */
  protected List<Record> filtraRecors(List<Record> lsRecs, List<String> parametri, SyncContext context)
     throws Exception
  {
    if(parametri.isEmpty() || arLocalKeys.isEmpty())
      return Collections.EMPTY_LIST;

    // produce una nuova lista di record controllando i valori delle chiavi
    List<Record> lsRecsFlt = new ArrayList<>();
    HashSet<String> hsPar = new HashSet<>(parametri);

    if(arLocalKeys.size() == 1)
    {
      // versione ottimizzata per chiave singola
      FieldLinkInfoBean f = findField(arLocalKeys.getKeyByIndex(0));
      for(Record r : lsRecs)
      {
        String value = r.getValue(f.localField.first).asString();
        if(hsPar.contains(value))
          lsRecsFlt.add(r);
      }
    }
    else
    {
      for(Record r : lsRecs)
      {
        StringBuilder sbKey = new StringBuilder();
        for(int i = 0; i < arLocalKeys.size(); i++)
        {
          FieldLinkInfoBean f = findField(arLocalKeys.getKeyByIndex(i));
          String value = r.getValue(f.localField.first).asString();

          if(i > 0)
            sbKey.append('^');

          sbKey.append(value);
        }

        if(hsPar.contains(sbKey.toString()))
          lsRecsFlt.add(r);
      }
    }

    return lsRecsFlt;
  }
}
