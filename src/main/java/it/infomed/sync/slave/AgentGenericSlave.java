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
package it.infomed.sync.slave;

import com.workingdogs.village.Column;
import com.workingdogs.village.Schema;
import it.infomed.sync.*;
import it.infomed.sync.plugins.AbstractAgent;
import it.infomed.sync.plugins.SyncPluginFactory;
import it.infomed.sync.plugins.SyncValidatorPlugin;
import java.sql.Connection;
import java.sql.Types;
import java.util.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.NumberUtils;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Classe base degli Agent.
 *
 * @author Nicola De Nisco
 */
abstract public class AgentGenericSlave extends AbstractAgent
{
  protected Element data;
  protected Element recordValidatorElement, tableValidatorElement;

  protected ArrayList<FieldLinkInfoBean> arFields = new ArrayList<>();
  protected String recordValidatorName, tableValidatorName;
  protected SyncValidatorPlugin recordValidator, tableValidator;
  protected static final Map<String, Map<String, Integer>> cachePrimaryKeys = new HashMap<>();
  protected static final Map<String, Map<String, Integer>> cacheNotEmptyFields = new HashMap<>();
  protected String dataBlockName;
  protected String ignoreInEmptyFields;
  protected boolean isolateRecord, isolateAllRecords;

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
  public void setXML(Location el, Element data)
     throws Exception
  {
    this.data = data;

    if((dataBlockName = okStrNull(data.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "name");

    Element fields, validators;

    if((fields = data.getChild("fields")) == null)
      throw new SyncSetupErrorException(0, "fields");

    // verifica per caricamento automatico campi da query
    if(!"all".equals(fields.getAttributeValue("auto")))
    {
      List<Element> lsFields = fields.getChildren("field");
      for(Element ef : lsFields)
        arFields.add(populateField(ef, new FieldLinkInfoBean()));
    }

    if((validators = data.getChild("validators")) != null)
    {
      // i validatori non sono obbligatori
      recordValidatorElement = Utils.getChildTestName(validators, el.name() + "-record-validator");
      tableValidatorElement = Utils.getChildTestName(validators, el.name() + "-table-validator");

      // carica eventuali validator
      if(recordValidatorElement != null)
      {
        recordValidator = SyncPluginFactory.getInstance().buildValidator(
           getRole(), okStr(recordValidatorElement.getAttribute("name").getValue()));
        recordValidator.setParentAgent(this);
        recordValidator.setXML(recordValidatorElement);
      }

      if(localTableValidatorElement != null)
      {
        localTableValidator = SyncPluginFactory.getInstance().buildValidator(
           getRole(), okStr(localTableValidatorElement.getAttribute("name").getValue()));
        localTableValidator.setParentAgent(this);
        localTableValidator.setXML(localTableValidatorElement);
      }
    }

    // legge eventuale delete strategy
    if((delStrategy = Utils.parseDeleteStrategy(data)) == null)
      delStrategy = getParentRule().getDelStrategy();

    // legge eventuale filtro sql
    if((filter = Utils.parseFilterKeyData(data)) == null)
      filter = getParentRule().getFilter();

    ignoreInEmptyFields = data.getChildText("ignoreInEmptyFields");
    isolateRecord = checkTrueFalse(data.getAttributeValue("isolateRecord"), false);
    isolateAllRecords = checkTrueFalse(data.getAttributeValue("isolateAllRecords"), false);
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
    if(foreignTableValidator != null)
      foreignTableValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);
    if(foreignRecordValidator != null)
      foreignRecordValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      if(f.foreignAdapter != null)
        f.foreignAdapter.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.adapter != null)
        f.adapter.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.foreignFieldValidator != null)
        f.foreignFieldValidator.slavePreparaValidazione(tableName, databaseName, lsRecs, arFields, context);
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
      if(f.foreignAdapter != null)
        f.foreignAdapter.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.adapter != null)
        f.adapter.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, f, context);
      if(f.foreignFieldValidator != null)
        f.foreignFieldValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
    }

    if(foreignRecordValidator != null)
      foreignRecordValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
    if(foreignTableValidator != null)
      foreignTableValidator.slaveFineValidazione(tableName, databaseName, lsRecs, arFields, context);
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

      if(foreignRecordValidator != null)
        if(foreignRecordValidator.slaveValidaRecord(null, r, arFields, con) != 0)
          return;

      HashMap<String, String> valoriUpdate = new HashMap<>();
      HashMap<String, String> valoriSelect = new HashMap<>();

      // reimposta lo stato_rec al valore di non cancellato
      if(delStrategy != null && !delStrategy.sqlUpdateDeleteStatement.isEmpty())
      {
        for(DeleteStrategy.DeleteField f : delStrategy.sqlUpdateDeleteStatement)
        {
          if(isOkStr(f.valueNormal) && findInSchema(tableSchema, f.field.first) != null)
          {
            valoriUpdate.put(f.field.first, convertValue(f.valueNormal, f.field));
            lsNotNullFields.remove(f.field.first.toUpperCase());
          }
        }
      }

      if(!preparaValoriRecord(r, tableName, databaseName, tableSchema, null, now, lsNotNullFields, valoriSelect, valoriUpdate, con))
        return;

      createOrUpdateRecord(con, tableName, valoriUpdate, valoriSelect);
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
     Map<String, Integer> lsNotNullFields,
     HashMap<String, String> valoriSelect, HashMap<String, String> valoriUpdate,
     Connection con)
     throws Exception
  {
    ASSERT(!arFields.isEmpty(), "!arFields.isEmpty()");

    for(int i = 0; i < arFields.size(); i++)
    {
      FieldLinkInfoBean f = arFields.get(i);
      Column col = findInSchema(tableSchema, f.foreignField.first);
      Object valore = r.get(f.foreignField.first);
      String sqlValue;
      Object rv;

      if(f.foreignAdapter != null && (rv = f.foreignAdapter.slaveValidaValore(key, r, valore, f, con)) != null)
        valore = rv;
      if(f.adapter != null && (rv = f.adapter.slaveValidaValore(key, r, valore, f, con)) != null)
        valore = rv;
      if(f.foreignFieldValidator != null && (f.foreignFieldValidator.slaveValidaRecord((String) valore, r, arFields, con) > 0))
        return false;

      if(valore == null)
      {
        sqlValue = convertNullValue(now, f.foreignField, col);
      }
      else
      {
        if((sqlValue = convertValue(valore, f.foreignField, tableName, col, f.truncZeroes)) == null)
          return false;
      }

      if(isSelect(f))
        valoriSelect.put(f.foreignField.first, sqlValue);
      else
        valoriUpdate.put(f.foreignField.first, sqlValue);
    }

    // aggiunge campi obbligatori per la tabella
    if(lsNotNullFields != null)
      for(Map.Entry<String, Integer> entry : lsNotNullFields.entrySet())
      {
        String campo = entry.getKey();
        Integer tipo = entry.getValue();

        if(Database.isNumeric(tipo))
        {
          valoriUpdate.put(campo, "0");
        }
        else if(Database.isString(tipo))
        {
          valoriUpdate.put(campo, "''");
        }
        else if(Database.isDate(tipo))
        {
          valoriUpdate.put(campo, now);
        }
        else
        {
          valoriUpdate.put(campo, "''");
        }
      }

    return true;
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
      if(isEqu(nomeCampo, f.foreignField.first))
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
      fldMap.remove(f.foreignField.first.toUpperCase());
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
    if(SU.isOkStr(ignoreInEmptyFields))
    {
      List<String> ignoreField = SU.string2List(ignoreInEmptyFields, ",", true);
      for(String fld : ignoreField)
      {
        fldMap.remove(fld.toUpperCase());
      }
    }

    return fldMap;
  }
}
