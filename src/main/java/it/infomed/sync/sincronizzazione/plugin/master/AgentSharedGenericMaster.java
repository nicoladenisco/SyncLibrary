/*
 *  AgentSharedGenericMaster.java
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

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
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
public class AgentSharedGenericMaster extends AgentGenericMaster
{
  protected Pair<String, String> timeStamp;
  protected ArrayMap<String, String> arKeys = new ArrayMap<>();
  protected boolean correct = false;
  public String correctTableName = null;

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
      }
    }
  }

  public boolean haveTs()
  {
    return timeStamp != null && !arKeys.isEmpty();
  }

  @Override
  protected HashtableRpc popolaRecord(Record r, List<FieldLinkInfoBean> arRealFields, HashtableRpc hr)
     throws Exception
  {
    String key = buildKey(r, arKeys);
    if(recordValidator != null)
      if(recordValidator.masterValidaRecord(key, r, arRealFields) != 0)
        return null;

    for(int i = 0; i < arRealFields.size(); i++)
    {
      FieldLinkInfoBean f = arRealFields.get(i);
      Value valore = r.getValue(f.field.first);
      if(valore.isNull())
        continue;

      // attenzione: salva il valore con il nome remoto del campo
      hr.put(f.shareFieldName, popolaValore(key, r, valore, f));
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
    if(parametri.isEmpty() || arKeys.isEmpty())
      return Collections.EMPTY_LIST;

    // produce una nuova lista di record controllando i valori delle chiavi
    List<Record> lsRecsFlt = new ArrayList<>();
    HashSet<String> hsPar = new HashSet<>(parametri);

    if(arKeys.size() == 1)
    {
      // versione ottimizzata per chiave singola
      FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(0));
      for(Record r : lsRecs)
      {
        String value = r.getValue(f.field.first).asString();
        if(hsPar.contains(value))
          lsRecsFlt.add(r);
      }
    }
    else
    {
      for(Record r : lsRecs)
      {
        StringBuilder sbKey = new StringBuilder();
        for(int i = 0; i < arKeys.size(); i++)
        {
          FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
          String value = r.getValue(f.field.first).asString();

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

  @Override
  protected void caricaTipiColonne(Schema schema)
     throws Exception
  {
    super.caricaTipiColonne(schema);

    if(timeStamp != null && !isOkStr(timeStamp.second) && isEquNocase(timeStamp.first, "AUTO"))
    {
      Column col = findInSchema(timeStamp.first);
      // anche nome per correggere il case
      timeStamp.first = col.name();
      timeStamp.second = col.type();
    }

    for(Pair<String, String> f : arKeys.getAsList())
    {
      if(!isOkStr(f.second))
      {
        Column col = findInSchema(f.first);
        // anche nome per correggere il case
        f.first = col.name();
        f.second = col.type();
      }
    }
  }
}
