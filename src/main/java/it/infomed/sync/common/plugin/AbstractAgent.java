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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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

  protected abstract String convertValue(Object valore, Pair<String, String> field, String tableName, Column col, boolean truncZeroes);

  protected abstract String convertNullValue(String now, Pair<String, String> field, Column col);

  public boolean createOrUpdateRecord(Connection con, String tableName,
     Map<String, String> valoriUpdate, Map<String, String> valoriSelect, Map<String, String> valoriInsert)
     throws SQLException
  {
    String sSQL;

    if((sSQL = createUpdateStatement(tableName, valoriUpdate, valoriSelect)) != null)
    {
      try (Statement st = con.createStatement())
      {
        if(st.executeUpdate(sSQL) > 0)
          return true;
      }
    }

    if((sSQL = createInsertStatement(tableName, valoriInsert)) == null)
      return false;

    try (Statement st = con.createStatement())
    {
      if(st.executeUpdate(sSQL) > 0)
        return true;
    }

    return false;
  }

  /**
   * Costruzione di statement SQL.
   * @param tableName nome tabella
   * @param valori mappa dei valori convertiti in stringa
   * @return istruzione SQL
   */
  public String createInsertStatement(String tableName, Map<String, String> valori)
  {
    StringBuilder sb1 = new StringBuilder(512);
    StringBuilder sb2 = new StringBuilder(512);

    for(Map.Entry<String, String> entry : valori.entrySet())
    {
      String key = okStr(entry.getKey());
      String value = okStr(entry.getValue());

      sb1.append(",").append(key);
      sb2.append(",").append(value);
    }

    if(sb1.length() == 0 || sb2.length() == 0)
      return null;

    String sSQL
       = "INSERT INTO " + tableName + "(" + sb1.toString().substring(1) + ")"
       + " VALUES(" + sb2.toString().substring(1) + ")";

    return sSQL;
  }

  /**
   * Costruzione di statement SQL.
   * @param tableName nome tabella
   * @param valoriUpdate valori da aggiornare convertiti in stringa
   * @param valoriSelect valori di selezione convertiti in stringa
   * @return
   */
  public String createUpdateStatement(String tableName,
     Map<String, String> valoriUpdate, Map<String, String> valoriSelect)
  {
    StringBuilder sb1 = new StringBuilder(512);
    StringBuilder sb2 = new StringBuilder(512);

    // ottimizzazione per unico campo da aggiornare
    // questa serve quando il datablock prevede
    // l'aggiornamento di un solo campo
    if(valoriSelect.size() == 1 && valoriUpdate.isEmpty())
      valoriUpdate.putAll(valoriSelect);

    if(valoriUpdate != null && !valoriUpdate.isEmpty())
      mapToString(valoriUpdate, sb1, ",");

    if(valoriSelect != null && !valoriSelect.isEmpty())
      mapToString(valoriSelect, sb2, ") AND (");

    if(sb1.length() == 0)
      return null;

    String sSQL
       = "UPDATE " + tableName
       + " SET " + sb1.toString().substring(1);

    if(sb2.length() > 0)
      sSQL += " WHERE " + sb2.toString().substring(6) + ")";

    return sSQL;
  }

  private void mapToString(Map<String, String> valori, StringBuilder sb, String sep)
  {
    for(Map.Entry<String, String> entry : valori.entrySet())
    {
      String key = okStr(entry.getKey());
      String value = okStr(entry.getValue());

      sb.append(sep).append(key);
      sb.append("=").append(value);
    }
  }

  public Column findInSchema(Schema tableSchema, String nomeColonna)
     throws Exception
  {
    Column col = tableSchema.findInSchemaIgnoreCaseQuiet(nomeColonna);

    if(col != null)
      return col;

    throw new SyncSetupErrorException(String.format(
       "Campo %s non trovato nella tabella %s.",
       nomeColonna, tableSchema.getTableName()));
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

    if(arKeys.size() == 1)
    {
      // ottimizzazione per campo singolo
      return okStrNull(r.getValue(arKeys.getKeyByIndex(0)).asString());
    }

    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < arKeys.size(); i++)
    {
      if(i > 0)
        sb.append('^');

      String value = okStrNull(r.getValue(arKeys.getKeyByIndex(i)).asString());
      if(value == null)
        return null;

      sb.append(value.replace('^', '|'));
    }
    return sb.toString();
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

    if(arKeys.size() == 1)
    {
      // ottimizzazione per campo singolo
      return okStr(record.get(arKeys.getKeyByIndex(0)));
    }

    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < arKeys.size(); i++)
    {
      if(i > 0)
        sb.append('^');

      String value = okStrNull(record.get(arKeys.getKeyByIndex(i)));
      if(value == null)
        return null;

      sb.append(value.replace('^', '|'));
    }
    return sb.toString();
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
        if(timestamp.after(ts))
        {
          // record da aggiornare o cancellare
          rv = deleted ? key + "/DELETE" : key + "/UPDATE";
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
