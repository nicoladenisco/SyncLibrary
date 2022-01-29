/*
 *  AgentGenericSlave.java
 *  Creato il Nov 24, 2017, 8:22:01 PM
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
import com.workingdogs.village.Schema;
import it.infomed.sync.common.*;
import it.infomed.sync.common.plugin.AbstractAgent;
import it.infomed.sync.common.plugin.SyncDeletePlugin;
import it.infomed.sync.common.plugin.SyncPluginFactory;
import it.infomed.sync.common.plugin.SyncValidatorPlugin;
import it.infomed.sync.db.Database;
import it.infomed.sync.db.DatabaseException;
import it.infomed.sync.db.SqlTransactAgent;
import java.sql.Connection;
import java.sql.Types;
import java.util.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.NumberUtils;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import static org.commonlib5.utils.StringOper.checkTrueFalse;
import static org.commonlib5.utils.StringOper.okStr;
import static org.commonlib5.utils.StringOper.okStrNull;
import org.jdom2.Element;

/**
 * Classe base degli Agent.
 *
 * @author Nicola De Nisco
 */
abstract public class AgentGenericSlave extends AbstractAgent
{
  protected Element data;
  protected ArrayList<FieldLinkInfoBean> arFields = new ArrayList<>();
  protected Element recordValidatorElement, tableValidatorElement, delStrategyElement;
  protected SyncValidatorPlugin recordValidator, tableValidator;
  protected String dataBlockName;
  // ----------- questi servono solo per lo slave abbinato ----------
  protected String ignoreInEmptyFields;
  protected boolean isolateRecord, isolateAllRecords;

  protected static final Map<String, Map<String, Integer>> cachePrimaryKeys = new HashMap<>();
  protected static final Map<String, Map<String, Integer>> cacheNotEmptyFields = new HashMap<>();

  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
  }

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

    if((dataBlockName = okStrNull(data.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "name");

    Element fields, validators;

    if((fields = data.getChild("fields")) == null)
      throw new SyncSetupErrorException(0, "fields");

    List<Element> lsFields = fields.getChildren("field");
    for(Element ef : lsFields)
      arFields.add(populateField(ef, new FieldLinkInfoBean(), location));

    if((validators = data.getChild("validators")) != null)
    {
      // i validatori non sono obbligatori
      recordValidatorElement = Utils.getChildTestName(validators, location + "-record-validator");
      tableValidatorElement = Utils.getChildTestName(validators, location + "-table-validator");

      // carica eventuali validator
      if(recordValidatorElement != null)
      {
        recordValidator = SyncPluginFactory.getInstance().buildValidator(getRole(),
           okStr(recordValidatorElement.getAttribute("name").getValue()));
        recordValidator.setParentAgent(this);
        recordValidator.setXML(location, recordValidatorElement);
      }

      if(tableValidatorElement != null)
      {
        tableValidator = SyncPluginFactory.getInstance().buildValidator(getRole(),
           okStr(tableValidatorElement.getAttribute("name").getValue()));
        tableValidator.setParentAgent(this);
        tableValidator.setXML(location, tableValidatorElement);
      }
    }

    // legge eventuale delete strategy
    if((delStrategyElement = Utils.getChildTestName(data, "delete-strategy")) == null)
    {
      delStrategy = (SyncDeletePlugin) getParentRule().getDelStrategy().clone();
    }
    else
    {
      String name = delStrategyElement.getAttributeValue("name");
      delStrategy = SyncPluginFactory.getInstance().buildDeleteStrategy(getRole(), name);
    }

    // legge eventuale filtro sql
    if((filter = Utils.parseFilterKeyData(data)) == null)
      filter = getParentRule().getFilter();

    ignoreInEmptyFields = data.getChildText("ignoreInEmptyFields");
    isolateRecord = checkTrueFalse(data.getAttributeValue("isolateRecord"), false);
    isolateAllRecords = checkTrueFalse(data.getAttributeValue("isolateAllRecords"), false);
  }

  private FieldLinkInfoBean populateField(Element ef, FieldLinkInfoBean fi, String location)
     throws Exception
  {
    Element validators;
    populateFieldName(ef, fi, location);

    fi.adapterElement = Utils.getChildTestName(ef, location + "-adapter");
    if((validators = ef.getChild("validators")) != null)
    {
      // i validatori non sono obbligatori
      fi.fieldValidatorElement = Utils.getChildTestName(validators, "validator");
    }

    fi.shared = checkTrueFalse(ef.getAttributeValue("shared"), false);
    fi.primary = checkTrueFalse(ef.getAttributeValue("primary"), false);
    fi.identityOff = checkTrueFalse(ef.getAttributeValue("identityOff"), false);
    fi.truncZeroes = checkTrueFalse(ef.getAttributeValue("truncZeroes"), false);

    if(fi.adapterElement != null)
    {
      fi.adapter = SyncPluginFactory.getInstance().buildAdapter(
         getRole(), okStr(fi.adapterElement.getAttributeValue("name")));
      fi.adapter.setXML(location, fi.adapterElement);
    }

    return fi;
  }

  private void populateFieldName(Element ef, FieldLinkInfoBean fi, String location)
     throws SyncSetupErrorException
  {
    Pair<String, String> uniquename = Utils.parseNameTypeIgnore(ef.getChild("name"));
    if(uniquename != null)
    {
      fi.field = new Pair<>(uniquename.first, uniquename.second);
      fi.shareFieldName = fi.field.first;
      return;
    }

    String tmp = okStrNull(ef.getAttributeValue("name"));
    if(tmp != null)
    {
      fi.field = new Pair<>(tmp, null);
      fi.shareFieldName = fi.field.first;
      return;
    }

    String tmp1 = okStrNull(ef.getAttributeValue(location));
    if(tmp1 != null)
    {
      fi.field = new Pair<>(tmp1, null);
      fi.shareFieldName = okStrNull(ef.getAttributeValue("foreign"));
      return;
    }

    fi.field = Utils.parseNameTypeIgnore(ef.getChild(location));
    fi.shareFieldName = okStrNull(ef.getAttributeValue("foreign"));
  }

  /**
   * Popola la lista record ottenuta.
   * @param tableName nome tabella
   * @param databaseName nome del database
   * @param tableSchema the value of tableSchema
   * @param lsRecs lista dei records
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  protected void salvaTuttiRecords(String tableName, String databaseName, Schema tableSchema,
     List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    if(isolateAllRecords)
    {
      try
      {
        salvaTuttiRecordsInternal(tableName, databaseName, tableSchema, lsRecs, context);
      }
      catch(Throwable t)
      {
        log.error("Salvataggio blocco record fallito!", t);
      }
    }
    else
    {
      salvaTuttiRecordsInternal(tableName, databaseName, tableSchema, lsRecs, context);
    }
  }

  private void salvaTuttiRecordsInternal(String tableName, String databaseName, Schema tableSchema,
     List<Map> lsRecs, SyncContext context)
     throws Exception
  {
    if(tableValidator != null)
      tableValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);
    if(recordValidator != null)
      recordValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      if(f.adapter != null)
        f.adapter.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.fieldValidator != null)
        f.fieldValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);
    }

    Map<String, Integer> lsEmptyFields = cacheNotEmptyFields.get(tableName);
    if(lsEmptyFields == null)
    {
      lsEmptyFields = SqlTransactAgent.executeReturn(databaseName, (con) -> buildNotNullFields(con, tableSchema));
      cacheNotEmptyFields.put(tableName, lsEmptyFields);
    }

    if(isolateRecord)
    {
      // salva i records isolando le eccezioni per ogni record
      for(Map r : lsRecs)
      {
        try
        {
          salvaRecord(r, tableName, databaseName, tableSchema, lsEmptyFields, context);
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
        salvaRecord(r, tableName, databaseName, tableSchema, lsEmptyFields, context);
    }

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      if(f.adapter != null)
        f.adapter.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.fieldValidator != null)
        f.fieldValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
    }

    if(recordValidator != null)
      recordValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
    if(tableValidator != null)
      tableValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
  }

  /**
   * Salva il record sul db.
   * ATTENZIONE: ogni record deve essere salvato in una transazione
   * separata altrimenti un errore SQL su un record blocca il salvataggio degli altri.
   * @param r valori del record
   * @param tableName nome tabella
   * @param databaseName nome del database
   * @param tableSchema the value of tableSchema
   * @param lsNotNullFields lista di campi che non possono essere null
   * @param context the value of context
   * @throws Exception
   */
  protected void salvaRecord(Map r,
     String tableName, String databaseName, Schema tableSchema,
     Map<String, Integer> lsNotNullFields, SyncContext context)
     throws Exception
  {
    SqlTransactAgent.execute(databaseName, (con) -> salvaRecord(r,
       tableName, databaseName, tableSchema, lsNotNullFields, context, con));
  }

  protected void salvaRecord(Map r,
     String tableName, String databaseName, Schema tableSchema,
     Map<String, Integer> lsNotNullFields, SyncContext context, Connection con)
     throws Exception
  {
    try
    {
      String now = DateTime.formatIsoFull(new Date());

      if(recordValidator != null)
        if(recordValidator.slaveValidaRecord(null, r, arFields, con) != 0)
          return;

      HashMap<String, String> valoriUpdate = new HashMap<>();
      HashMap<String, String> valoriSelect = new HashMap<>();
      HashMap<String, String> valoriInsert = new HashMap<>();

      preparaValoriNotNull(lsNotNullFields, valoriInsert, now);

      if(!preparaValoriRecord(r,
         tableName, databaseName, tableSchema, null, now,
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

      createOrUpdateRecord(con, tableName, valoriUpdate, valoriSelect, valoriInsert);
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

  protected boolean isSelect(FieldLinkInfoBean f)
  {
    return false;
  }

  protected boolean preparaValoriRecord(Map r,
     String tableName, String databaseName, Schema tableSchema, String key, String now,
     Map<String, String> valoriSelect, Map<String, String> valoriUpdate, Map<String, String> valoriInsert,
     Connection con)
     throws Exception
  {
    ASSERT(!arFields.isEmpty(), "!arFields.isEmpty()");

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      Column col = findInSchema(tableSchema, f.field.first);
      Object valore = r.get(f.field.first);
      String sqlValue;
      Object rv;

      if(f.adapter != null && (rv = f.adapter.slaveValidaValore(key, r, valore, f, con)) != null)
        valore = rv;
      if(f.fieldValidator != null && (f.fieldValidator.slaveValidaRecord((String) valore, r, arFields, con) > 0))
        return false;

      if(valore == null)
      {
        sqlValue = convertNullValue(now, f.field, col);
      }
      else
      {
        if((sqlValue = convertValue(valore, f.field, tableName, col, f.truncZeroes)) == null)
          return false;
      }

      if(isSelect(f))
        valoriSelect.put(f.field.first, sqlValue);
      else
        valoriUpdate.put(f.field.first, sqlValue);

      valoriInsert.put(f.field.first, sqlValue);
    }

    return true;
  }

  /**
   * Aggiunge un default ragionevole per i campi definiti NOT NULL sulla tabella.
   * @param lsNotNullFields campi not null
   * @param valoriInsert valori da popolare
   * @param now data/ora di riferimento
   */
  protected void preparaValoriNotNull(Map<String, Integer> lsNotNullFields, Map<String, String> valoriInsert, String now)
  {
    if(lsNotNullFields == null || lsNotNullFields.isEmpty())
      return;

    // aggiunge campi obbligatori per la tabella
    for(Map.Entry<String, Integer> entry : lsNotNullFields.entrySet())
    {
      String campo = entry.getKey();
      Integer tipo = entry.getValue();

      if(Database.isNumeric(tipo))
      {
        valoriInsert.put(campo, "0");
      }
      else if(Database.isString(tipo))
      {
        valoriInsert.put(campo, "''");
      }
      else if(Database.isDate(tipo))
      {
        valoriInsert.put(campo, now);
      }
      else
      {
        valoriInsert.put(campo, "''");
      }
    }
  }

  @Override
  protected String convertValue(Object valore, Pair<String, String> field, String tableName, Column col, boolean truncZeroes)
  {
    int tipo = col.typeEnum();
    String s = okStr(valore);

    if(tipo == Types.BIT)
    {
      if(checkTrue(s))
        return "TRUE";
      if(checkFalse(s))
        return "FALSE";

      int val = parse(s, -1);
      if(val != -1)
        return val > 0 ? "TRUE" : "FALSE";

      log.error("Tipo campo non congruente [tabella:colonna:valore] " + tableName + ":" + field.first + ":" + valore);
      return null;
    }

    if(truncZeroes)
      s = removeZero(s);

    if(Database.isNumeric(tipo))
    {
      if(!NumberUtils.isNumber(s))
      {
        log.error("Tipo campo non congruente [tabella:colonna:valore] " + tableName + ":" + field.first + ":" + valore);
        return null;
      }
      return s;
    }

    if(Database.isString(tipo) || Database.isDate(tipo))
    {
      if(isEquAny(s, "NULL", "''"))
        return s;
    }

    return "'" + s.replace("'", "''") + "'";
  }

  @Override
  protected String convertNullValue(String now, Pair<String, String> field, Column col)
  {
    int tipo = col.typeEnum();

    if(Database.isNumeric(tipo))
      return col.nullAllowed() ? "NULL" : "0";

    if(Database.isString(tipo))
      return col.nullAllowed() ? "NULL" : "''";

    if(Database.isDate(tipo))
      return col.nullAllowed() ? "NULL" : "'" + now + "'";

    return "NULL";
  }

  protected FieldLinkInfoBean findField(String nomeCampo)
     throws Exception
  {
    for(FieldLinkInfoBean f : arFields)
    {
      if(isEqu(nomeCampo, f.field.first))
        return f;
    }

    throw new SyncSetupErrorException(String.format(
       "Campo %s non trovato.",
       nomeCampo));
  }

  /**
   * Ritorna un elenco di campi che non possono essere lasciati a NULL.
   * @param con
   * @param tableSchema schema tabella
   * @return array di campi
   * @throws Exception
   */
  protected Map<String, Integer> buildNotNullFields(Connection con, Schema tableSchema)
     throws Exception
  {
    ArrayMap<String, Integer> fldMap = new ArrayMap<>();

    for(int i = 1; i <= tableSchema.numberOfColumns(); i++)
    {
      Column col = tableSchema.column(i);
      if(!col.nullAllowed() && !col.readOnly())
        fldMap.put(col.name().toUpperCase(), col.typeEnum());
    }

    // rimuove i campi che verranno trasferiti dal master allo slave
    for(FieldLinkInfoBean f : arFields)
    {
      fldMap.remove(f.field.first.toUpperCase());
    }

    // rimuove in ogni caso le chiavi primarie: non possono essere messe a zero
    Map<String, Integer> primaryKeys = cachePrimaryKeys.get(tableSchema.tableName());
    if(primaryKeys == null)
    {
      if((primaryKeys = Database.getPrimaryKeys(con, tableSchema.tableName())) != null)
      {
        cachePrimaryKeys.put(tableSchema.getTableName(), primaryKeys);
      }
    }

    if(primaryKeys == null || primaryKeys.isEmpty())
      log.info("ATTENZIONE: nessuna definizione di chiave primaria per " + tableSchema.tableName());

    if(primaryKeys != null)
    {
      for(Map.Entry<String, Integer> entry : primaryKeys.entrySet())
      {
        String primaryKey = entry.getKey();
        fldMap.remove(primaryKey.toUpperCase());
      }
    }

    // rimuove in ogni caso le colonne identity: non possono essere messe a zero
    if(isOkStr(ignoreInEmptyFields))
    {
      List<String> ignoreField = string2List(ignoreInEmptyFields, ",", true);
      for(String fld : ignoreField)
      {
        fldMap.remove(fld.toUpperCase());
      }
    }

    return fldMap;
  }
}
