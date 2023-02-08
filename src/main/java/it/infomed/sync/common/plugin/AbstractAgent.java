/*
 *  AbstractAgent.java
 *  Creato il Nov 27, 2017, 12:02:56 PM
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
package it.infomed.sync.common.plugin;

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.common.*;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.VectorRpc;

/**
 * Classe base di tutti gli agent.
 *
 * @author Nicola De Nisco
 */
public abstract class AbstractAgent extends AbstractPlugin
   implements SyncAgentPlugin
{
  protected SyncRulePlugin parentRule;
  protected SyncDeletePlugin delStrategy;
  protected FilterKeyData filter;
  protected Schema schema;

  @Override
  public SyncRulePlugin getParentRule()
  {
    return parentRule;
  }

  @Override
  public void setParentRule(SyncRulePlugin parentRule)
  {
    this.parentRule = parentRule;
  }

  @Override
  public SyncDeletePlugin getDelStrategy()
  {
    return delStrategy;
  }

  @Override
  public void setDelStrategy(SyncDeletePlugin delStrategy)
  {
    this.delStrategy = delStrategy;
  }

  @Override
  public FilterKeyData getFilter()
  {
    return filter;
  }

  @Override
  public void setFilter(FilterKeyData filter)
  {
    this.filter = filter;
  }

  public Schema getSchema()
  {
    return schema;
  }

  public void setSchema(Schema schema)
  {
    this.schema = schema;
  }

  @Override
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void pianificaAggiornamento(List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public Column findInSchema(String nomeColonna)
     throws Exception
  {
    Column col = schema.findInSchemaIgnoreCaseQuiet(nomeColonna);

    if(col != null)
      return col;

    if(schema.isSingleTable())
      throw new SyncSetupErrorException(String.format(
         "Campo %s non trovato nella tabella %s.",
         nomeColonna, schema.getTableName()));

    throw new SyncSetupErrorException(String.format(
       "Campo %s non trovato nella tabella query.",
       nomeColonna));
  }

  protected String okKey(Object key)
  {
    String rv = okStr(key);
    return rv.isEmpty() ? null : rv.replace('^', '|');
  }

  /**
   * Costruisce la chiave di scambio.
   * @param r record della query
   * @param arKeys array mappa delle chiavi
   * @return la chiave del record
   * @throws Exception
   */
  protected String buildKey(Record r, ArrayMap<String, String> arKeys)
     throws Exception
  {
    ASSERT(arKeys != null && !arKeys.isEmpty(), "arKeys != null && !arKeys.isEmpty()");

    switch(arKeys.size())
    {
      case 0:
        return null;

      case 1:
      {
        // ottimizzazione per campo singolo
        return okKey(r.getValue(arKeys.getKeyByIndex(0)).asString());
      }

      case 2:
      {
        // ottimizzazione per due campi
        String v1 = okKey(r.getValue(arKeys.getKeyByIndex(0)).asString());
        String v2 = okKey(r.getValue(arKeys.getKeyByIndex(1)).asString());
        return v1 == null || v2 == null ? null : v1 + "^" + v2;
      }

      default:
      {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < arKeys.size(); i++)
        {
          if(i > 0)
            sb.append('^');

          String value = okKey(r.getValue(arKeys.getKeyByIndex(i)).asString());
          if(value == null)
            return null;

          sb.append(value);
        }
        return sb.toString();
      }
    }
  }

  /**
   * Costruisce la chiave di scambio.
   * @param record record ottenuti dal server
   * @param arKeys array mappa delle chiavi
   * @return la chiave del record
   * @throws Exception
   */
  protected String buildKey(Map record, ArrayMap<String, String> arKeys)
     throws Exception
  {
    ASSERT(arKeys != null && !arKeys.isEmpty(), "arKeys != null && !arKeys.isEmpty()");

    switch(arKeys.size())
    {
      case 0:
        return null;

      case 1:
      {
        // ottimizzazione per campo singolo
        return okKey(record.get(arKeys.getKeyByIndex(0)));
      }

      case 2:
      {
        // ottimizzazione per due campi
        String v1 = okKey(record.get(arKeys.getKeyByIndex(0)));
        String v2 = okKey(record.get(arKeys.getKeyByIndex(1)));
        return v1 == null || v2 == null ? null : v1 + "^" + v2;
      }

      default:
      {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < arKeys.size(); i++)
        {
          if(i > 0)
            sb.append('^');

          String value = okKey(record.get(arKeys.getKeyByIndex(i)));
          if(value == null)
            return null;

          sb.append(value);
        }
        return sb.toString();
      }
    }
  }

  /**
   * Compila il blocco aggiornamenti lato master.
   * I dati letti dal database (lsRecs) vengono confrontati con le timestamp
   * inivate dal client (parametri) per determinare i records che devono essere aggiornati.
   * In vResult vengono salvate delle stringhe utili al client per determinare le operazioni successive.
   * Ogni stringa è nella forma chiave/operazione dove operazione
   * può essere: NEW per nuovo record, UPDATE per aggiornare un record, DELETE per cancellare
   * il record, UNKNOW per indicare una chiave in parametri inesistente nel db.
   * @param arKeys array delle chiavi di scambio
   * @param parametri coppie chiave/timestampa richieste dal client
   * @param lsRecs lista records letti dal database
   * @param oldTimestamp eventuale ultima timestamp di aggiornamento (può essere null)
   * @param timeStampField nome del campo timestamp nei records
   * @param vResult risultati del match
   * @param context
   * @throws Exception
   */
  protected void compilaBloccoMaster(
     ArrayMap<String, String> arKeys, List<Pair<String, Date>> parametri, List<Record> lsRecs,
     Date oldTimestamp, String timeStampField, VectorRpc vResult, SyncContext context)
     throws Exception
  {
    Date ts;
    HashSet<String> allKeys = new HashSet<>();
    Map<String, Date> parMap = Utils.cvtPair2Map(parametri, new HashMap<>());

    for(Record r : lsRecs)
    {
      String key = buildKey(r, arKeys);
      if(!isOkStr(key))
        continue;

      Date timestamp = timeStampField == null ? null : r.getValue(timeStampField).asUtilDate();
      if(timestamp == null)
        timestamp = new Date();

      boolean deleted = delStrategy == null ? false : delStrategy.queryRecordDeleted(r, context);
      String rv = null;
      allKeys.add(key);

      if((ts = parMap.get(key)) == null)
      {
        // nuovo record lato caleido: non esiste in parametri
        if(!deleted)
        {
          if(oldTimestamp == null || timestamp.after(oldTimestamp))
            rv = key + "/NEW";
        }
      }
      else
      {
        // record già esistente: verifica per timestamp
        if(deleted)
        {
          // record da cancellare (in ogni caso)
          rv = key + "/DELETE";
        }
        else if(timestamp.after(ts))
        {
          // record da aggiornare
          rv = key + "/UPDATE";
        }
      }

      if(rv != null)
        vResult.add(rv);
    }

    // segnala records non presenti nella equivalente tabella di caleido
    for(Pair<String, Date> p : parametri)
    {
      if(!allKeys.contains(p.first))
        vResult.add(p.first + "/UNKNOW");
    }
  }

  /**
   * Compila il blocco aggiornamenti lato slave.
   * Lato slave viene eseguita una query sul db per i records da aggiornare. Per ogni record viene computata
   * una chiave e viene associato un timestamp compilando un elenco da sottoporre al server (parametri).
   * La timestamp può essere inclusa nella query del db, oppure puo essere fornita a parte una mappa,
   * ottenuta in altro modo di chiavi/timestamp da utilizzare nell'elaborazione.
   * @param arKeys array delle chiavi di scambio
   * @param parametri coppie chiave/timestampa da compilare per invio al server
   * @param lsRecs lista records letti dal database
   * @param oldTimestamp eventuale ultima timestamp di aggiornamento (può essere null)
   * @param timeStampField campo della timestamp da cercare nei records
   * @param mapTimeStamps eventuale mappa di timestamp già pronti (può essere null)
   * @throws Exception
   */
  protected void compilaBloccoSlave(
     ArrayMap<String, String> arKeys, List<Pair<String, Date>> parametri,
     List<Record> lsRecs, Date oldTimestamp,
     Pair<String, String> timeStampField, Map<String, Date> mapTimeStamps)
     throws Exception
  {
    if(lsRecs.isEmpty())
      return;

    int tsIndex = -1;
    if(timeStampField != null)
      tsIndex = Utils.getFieldIndex(timeStampField.first, lsRecs.get(0));

    if(tsIndex == -1)
    {
      // la timestamp non è presente: usa eventuale mappa pre caricata per la tabella
      Date timestamp = FAR_DATE;
      for(Record r : lsRecs)
      {
        String key = buildKey(r, arKeys);
        if(!isOkStr(key))
          continue;

        if(mapTimeStamps != null)
          timestamp = mapTimeStamps.getOrDefault(key, FAR_DATE);

        parametri.add(new Pair<>(key, timestamp));
      }
    }
    else
    {
      // la timestamp è presente: la legge e la usa
      for(Record r : lsRecs)
      {
        Date timestamp = r.getValue(tsIndex).asUtilDate();
        String key = buildKey(r, arKeys);

        if(timestamp == null)
          timestamp = FAR_DATE;

        parametri.add(new Pair<>(key, timestamp));
      }
    }
  }

  public String removeZero(Object value)
  {
    String s = okStr(value);
    char[] ar = s.toCharArray();
    for(int i = 0; i < ar.length; i++)
    {
      if(ar[i] != '0')
        return new String(ar, i, ar.length);
    }
    return "";
  }
}
