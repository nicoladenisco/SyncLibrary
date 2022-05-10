/*
 *  AgentTableUpdateSlave.java
 *  Creato il Nov 24, 2017, 7:16:45 PM
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

import com.workingdogs.village.Column;
import com.workingdogs.village.DataSetException;
import com.workingdogs.village.Record;
import com.workingdogs.village.TableDataSet;
import it.infomed.sync.common.*;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DatabaseException;
import it.infomed.sync.db.DbPeer;
import it.infomed.sync.db.SqlTransactAgent;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Adapter di sincronizzazione orientato alle tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentTableUpdateSlave extends AgentSharedGenericSlave
{
  protected Element tblElement;
  protected String tableName;
  protected TableDataSet tds;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    Element tables = data.getChild("tables");
    if(tables == null)
      throw new SyncSetupErrorException(0, "tables");

    if((tblElement = tables.getChild(location)) == null)
      throw new SyncSetupErrorException(0, "tables/" + location);

    if((tableName = okStrNull(tblElement.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "tables/" + location + ":name");

    if(correctTableName == null)
      correctTableName = tableName;

    caricaTipiColonne();
  }

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
  }

  @Override
  public void verificaBlocco(List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    String fkeys = join(arKeys.keySet().iterator(), ",", null);

    String fwhere = "";
    for(Map.Entry<String, String> entry : arKeys.entrySet())
    {
      String nome = entry.getKey();
      String tipo = entry.getValue();

      if("STRING".equals(tipo))
        fwhere += " AND ((" + nome + " IS NOT NULL) AND (" + nome + "<> 'null'))";
      else
        fwhere += " AND (" + nome + " IS NOT NULL)";
    }
    if(!fwhere.isEmpty())
      fwhere = fwhere.substring(5);

    List<Record> lsRecs = haveTs() ? queryWithTimestamp(fkeys, fwhere, oldTimestamp)
                             : queryWithoutTimestamp(fkeys, fwhere);

    if(lsRecs.isEmpty())
      return;

    if(keysHaveAdapter)
    {
      for(int i = 0; i < arKeys.size(); i++)
      {
        FieldLinkInfoBean f = findField(arKeys.getKeyByIndex(i));
        if(f.adapter != null)
          f.adapter.slaveSharedFetchData(lsRecs, f, context);
      }
    }

    if(haveTs())
      compilaBloccoSlave(arKeys, parametri, lsRecs, oldTimestamp, timeStamp, mapTimeStamps);
    else
      compilaBloccoSlave(arKeys, parametri, lsRecs, null, null, null);
  }

  protected List<Record> queryWithTimestamp(String fkeys, String fwhere, Date oldTimestamp)
     throws Exception
  {
    String sSQL;

    switch(timeStamp.first)
    {
      case "AUTO":
        caricaTimestamps(tableName);
      case "NONE":
        sSQL
           = "SELECT " + fkeys
           + "  FROM " + tableName
           + " WHERE " + fwhere;
        break;

      default:
        sSQL
           = "SELECT " + timeStamp.first + "," + fkeys
           + " FROM " + tableName
           + " WHERE " + fwhere;

        if(oldTimestamp != null)
          sSQL += " AND (" + timeStamp.first + " > '" + DateTime.formatIsoFull(oldTimestamp) + "')";

        break;
    }

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterFetch)
        sSQL += " AND (" + sql + ")";

      // se richiesto aggiunge limitazione temporale
      if(filter.timeLimitFetch != 0)
      {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -filter.timeLimitFetch);
        Date firstDate = cal.getTime();
        sSQL += " AND (" + timeStamp.first + " >= '" + DateTime.formatIsoFull(firstDate) + "')";
      }
    }

    return DbPeer.executeQuery(sSQL, databaseName);
  }

  protected List<Record> queryWithoutTimestamp(String fkeys, String fwhere)
     throws Exception
  {
    String sSQL
       = "SELECT " + fkeys
       + "  FROM " + tableName
       + " WHERE " + fwhere;

    if(filter != null)
    {
      // aggiunge clausole del filtro
      for(String sql : filter.sqlFilterFetch)
        sSQL += " AND (" + sql + ")";
    }

    return DbPeer.executeQuery(sSQL, databaseName);
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    Vector v = (Vector) context.getNotNull("records-data");
    if(!v.isEmpty())
      salvaTuttiRecords(v, context);
  }

  @Override
  public void pianificaAggiornamento(List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    int pos;
    List<String> delete = new ArrayList<>();
    List<String> unknow = new ArrayList<>();

    for(String s : vInfo)
    {
      if((pos = s.lastIndexOf('/')) != -1)
      {
        String key = s.substring(0, pos);
        String val = s.substring(pos + 1);

        switch(val)
        {
          case "NEW":
            aggiorna.add(key);
            break;

          case "UPDATE":
            aggiorna.add(key);
            break;

          case "DELETE":
            delete.add(key);
            break;

          case "UNKNOW":
            unknow.add(key);
            break;
        }
      }
    }

    if(delStrategy == null)
      return;

    if(delete.isEmpty() && unknow.isEmpty())
      return;

    if(!delete.isEmpty())
    {
      if(keysHaveAdapter)
        convertiChiavi(delete, context);
      delStrategy.cancellaRecordsPerDelete(delete, arKeys, context);
    }

    if(!unknow.isEmpty())
    {
      if(keysHaveAdapter)
        convertiChiavi(unknow, context);
      delStrategy.cancellaRecordsPerUnknow(unknow, arKeys, context);
    }
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    try (Connection conn = Database.getConnection(databaseName))
    {
      tds = new TableDataSet(conn, tableName);
      schema = tds.schema();

      caricaTipiColonne(schema);

      if(delStrategy != null)
        delStrategy.caricaTipiColonne(databaseName, tableName);
    }

    // rilascia gli oggetti legati alla connessione
    tds.clear();
    tds.setConnection(null);
  }

  /**
   * Popola la lista record ottenuta.
   * @param lsRecs lista dei records
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  protected void salvaTuttiRecords(List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    if(isolateAllRecords)
    {
      try
      {
        salvaTuttiRecordsInternal(lsRecs, context);
      }
      catch(Throwable t)
      {
        log.error("Salvataggio blocco record fallito!", t);
      }
    }
    else
    {
      salvaTuttiRecordsInternal(lsRecs, context);
    }
  }

  protected void salvaTuttiRecordsInternal(List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    prepareForSave(lsRecs, context);

    try
    {
      if(isolateRecord)
      {
        // salva i records isolando le eccezioni per ogni record
        for(Map r : lsRecs)
        {
          try
          {
            salvaRecord(r, context);
          }
          catch(Throwable t)
          {
            log.error(t.getMessage() + " Unable to save the record " + r.toString());
          }
        }
      }
      else
      {
        for(Map r : lsRecs)
          salvaRecord(r, context);
      }
    }
    finally
    {
      clearForSave(lsRecs, context);
    }
  }

  /**
   * Operazioni preliminari per salvataggio dei records.
   * Inizializza validatori e adapter per le successive operazioni di salvataggio record.
   * @param lsRecs
   * @param context
   * @throws Exception
   */
  protected void prepareForSave(List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    if(recordValidator != null)
      recordValidator.slavePreparaValidazione(lsRecs, arFields, context);

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      if(f.adapter != null)
        f.adapter.slavePreparaValidazione(lsRecs, arFields, f, context);
    }
  }

  /**
   * Operazioni di pulizia dopo salvataggio dei records.
   * Notifica a validatori e adapter la fine delle operazioni di salvataggio record.
   * Qui vengono rilasciate eventuali risorse accumulate prima e durante il salvataggio.
   * @param lsRecs
   * @param context
   * @throws Exception
   */
  protected void clearForSave(List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      if(f.adapter != null)
        f.adapter.slaveFineValidazione(lsRecs, arFields, f, context);
    }

    if(recordValidator != null)
      recordValidator.slaveFineValidazione(lsRecs, arFields, context);
  }

  /**
   * Salva il record sul db.
   * ATTENZIONE: ogni record deve essere salvato in una transazione
   * separata altrimenti un errore SQL su un record blocca il salvataggio degli altri.
   * @param r valori del record
   * @param context the value of context
   * @throws Exception
   */
  protected void salvaRecord(Map r, SyncContext context)
     throws Exception
  {
    SqlTransactAgent.execute(databaseName, (con) ->
    {
      if(haveTs())
        salvaRecordShared(r, context, con);
      else
        salvaRecord(r, context, con);
    });
  }

  protected void salvaRecord(Map r, SyncContext context, Connection con)
     throws Exception
  {
    try
    {
      Date now = new Date();

      if(recordValidator != null)
        if(recordValidator.slaveValidaRecord(null, r, arFields, con) != 0)
          return;

      HashMap<String, Object> valoriUpdate = new HashMap<>();
      HashMap<String, Object> valoriSelect = new HashMap<>();
      HashMap<String, Object> valoriInsert = new HashMap<>();

      if(!preparaValoriRecord(r, null,
         valoriSelect, valoriUpdate, valoriInsert,
         con))
        return;

      if(delStrategy != null)
      {
        // reimposta lo stato_rec al valore di non cancellato
        if(!delStrategy.confermaValoriRecord(r, now, null, null,
           valoriSelect, valoriUpdate, valoriInsert, context,
           con))
          return;
      }

      createOrUpdateRecord(con, now, valoriUpdate, valoriSelect, valoriInsert);
    }
    catch(SyncIgnoreRecordException e)
    {
      // un adapter o altra ha esplicitamente bloccato l'inserimento di questo record
      if(log.isDebugEnabled())
        log.debug(e.getMessage() + " ignore record " + r.toString());
      else
        log.info(e.getMessage());
    }
    catch(SyncErrorException | DatabaseException e)
    {
      // this exception avoids the insert or update on db because the external key reference is violated
      // but it doesn't stop the elaboration of all other records
      log.error(e.getMessage() + " Unable to import the record " + r.toString());
    }
  }

  protected void salvaRecordShared(Map r, SyncContext context, Connection con)
     throws Exception
  {
    if(arKeys.isEmpty())
      die("Nessuna definizione di chiave per " + tableName + ": aggiornamento non possibile");

    try
    {
      boolean haveAutoTs = isEquNocase(timeStamp.first, "auto");
      String key = buildKey(r, arKeys);
      Date now = new Date();

      if(recordValidator != null)
        if(recordValidator.slaveValidaRecord(key, r, arFields, con) != 0)
          return;

      HashMap<String, Object> valoriUpdate = new HashMap<>();
      HashMap<String, Object> valoriSelect = new HashMap<>();
      HashMap<String, Object> valoriInsert = new HashMap<>();

      if(!haveAutoTs)
      {
        valoriInsert.put(timeStamp.first, now);
        valoriUpdate.put(timeStamp.first, now);
      }

      if(!preparaValoriRecord(r, key,
         valoriSelect, valoriUpdate, valoriInsert,
         con))
        return;

      if(delStrategy != null)
      {
        // converte chiave per shared con adapter
        String convertedKey = keysHaveAdapter ? convertiChiave(key, context) : key;

        if(convertedKey != null)
        {
          // reimposta lo stato_rec al valore di non cancellato
          if(!delStrategy.confermaValoriRecord(r, now, key, arKeys,
             valoriSelect, valoriUpdate, valoriInsert, context,
             con))
            return;
        }
      }

      createOrUpdateRecord(con, now, valoriUpdate, valoriSelect, valoriInsert);

      if(haveAutoTs)
        updateCalTimestamp(key, now, con);
    }
    catch(SyncIgnoreRecordException e)
    {
      // un adapter o altra ha esplicitamente bloccato l'inserimento di questo record
      if(log.isDebugEnabled())
        log.info(e.getMessage() + " ignore record " + r.toString());
      else
        log.info(e.getMessage());
    }
    catch(SyncErrorException | DatabaseException e)
    {
      // this exception avoids the insert or update on db because the external key reference is violated
      // but it doesn't stop the elaboration of all other records
      log.error(e.getMessage() + " Unable to import the record " + r.toString());
    }
  }

  protected boolean preparaValoriRecord(Map r, String key,
     Map<String, Object> valoriSelect, Map<String, Object> valoriUpdate, Map<String, Object> valoriInsert,
     Connection con)
     throws Exception
  {
    ASSERT(!arFields.isEmpty(), "!arFields.isEmpty()");

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      Object valore = r.get(f.field.first);

      Object rv;
      if(f.adapter != null && (rv = f.adapter.slaveValidaValore(key, r, valore, f, con)) != null)
        valore = rv;

      if(isSelect(f))
        valoriSelect.put(f.field.first, valore);
      else
        valoriUpdate.put(f.field.first, valore);

      valoriInsert.put(f.field.first, valore);
    }

    return true;
  }

  /**
   * Aggiunge un default ragionevole per i campi definiti NOT NULL sulla tabella.
   * @param valoriInsert valori da popolare
   * @param now data/ora di riferimento
   */
  protected void preparaValoriNotNull(Map<String, Object> valoriInsert, Date now)
  {
    // aggiunge campi obbligatori per la tabella
    Column[] columns = schema.getColumns();
    for(int i = 0; i < columns.length; i++)
    {
      Column col = columns[i];
      String campo = col.name();

      if(col.nullAllowed() || valoriInsert.containsKey(campo))
        continue;

      if(col.isNumericValue())
      {
        valoriInsert.put(campo, "0");
      }
      else if(col.isStringValue())
      {
        valoriInsert.put(campo, "''");
      }
      else if(col.isDateValue())
      {
        valoriInsert.put(campo, now);
      }
      else
      {
        valoriInsert.put(campo, "''");
      }
    }
  }

  public void createOrUpdateRecord(Connection con, Date now,
     Map<String, Object> valoriUpdate, Map<String, Object> valoriSelect, Map<String, Object> valoriInsert)
     throws SQLException, DataSetException
  {
    tds.setConnection(con);
    List<Record> lsPrevious = tds.fetchByGenericValues(valoriSelect).fetchAllRecords();

    if(lsPrevious.isEmpty())
    {
      // aggiunge eventuali campi not null
      preparaValoriNotNull(valoriInsert, now);

      Record nuovo = tds.addRecord();
      nuovo.setValues(valoriInsert);
      nuovo.save();
    }
    else
    {
      Record vecchio = lsPrevious.get(0);
      vecchio.setValues(valoriUpdate);
      vecchio.save();
    }
  }
}
