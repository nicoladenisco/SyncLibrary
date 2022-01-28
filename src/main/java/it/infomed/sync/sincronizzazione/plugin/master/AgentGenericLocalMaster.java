/*
 *  AgentGenericLocalMaster.java
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
package it.infomed.sync.sincronizzazione.plugin.master;

import com.workingdogs.village.Column;
import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractAgent;
import it.infomed.sync.common.plugin.SyncDeletePlugin;
import it.infomed.sync.common.plugin.SyncPluginFactory;
import it.infomed.sync.common.plugin.SyncValidatorPlugin;
import java.util.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.NumberUtils;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;
import org.rigel5.db.DbUtils;

/**
 * Classe base degli Agent.
 *
 * @author Nicola De Nisco
 */
abstract public class AgentGenericLocalMaster extends AbstractAgent
{
  protected Element data;
  protected ArrayList<FieldLinkInfoBean> arFields = new ArrayList<>();
  protected Element localRecordValidatorElement, foreignRecordValidatorElement,
     localTableValidatorElement, foreignTableValidatorElement, delStrategyElement;
  protected SyncValidatorPlugin localRecordValidator, localTableValidator;
  protected String dataBlockName;
  // ----------- questi servono solo per lo slave abbinato ----------
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
    return ROLE_MASTER;
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
      arFields.add(populateField(ef, new FieldLinkInfoBean()));

    if((validators = data.getChild("validators")) != null)
    {
      // i validatori non sono obbligatori
      localRecordValidatorElement = Utils.getChildTestName(validators, "local-record-validator");
      foreignRecordValidatorElement = Utils.getChildTestName(validators, "foreign-record-validator");
      localTableValidatorElement = Utils.getChildTestName(validators, "local-table-validator");
      foreignTableValidatorElement = Utils.getChildTestName(validators, "foreign-table-validator");

      // carica eventuali validator
      if(localRecordValidatorElement != null)
      {
        localRecordValidator = SyncPluginFactory.getInstance().buildValidator(
           getRole(), okStr(localRecordValidatorElement.getAttribute("name").getValue()));
        localRecordValidator.setParentAgent(this);
        localRecordValidator.setXML(location, localRecordValidatorElement);
      }

      if(localTableValidatorElement != null)
      {
        localTableValidator = SyncPluginFactory.getInstance().buildValidator(
           getRole(), okStr(localTableValidatorElement.getAttribute("name").getValue()));
        localTableValidator.setParentAgent(this);
        localTableValidator.setXML(location, localTableValidatorElement);
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

  private FieldLinkInfoBean populateField(Element ef, FieldLinkInfoBean fi)
     throws Exception
  {
    Element validators;
    populateFieldName(ef, fi);

    fi.localAdapterElement = Utils.getChildTestName(ef, "local-adapter");
    fi.foreignAdapterElement = Utils.getChildTestName(ef, "foreign-adapter");
    fi.adapterElement = Utils.getChildTestName(ef, "adapter");
    if((validators = ef.getChild("validators")) != null)
    {
      // i validatori non sono obbligatori
      fi.foreignFieldValidatorElement = Utils.getChildTestName(validators, "foreign-field-validator");
    }

    fi.shared = checkTrueFalse(ef.getAttributeValue("shared"), false);
    fi.primary = checkTrueFalse(ef.getAttributeValue("primary"), false);
    fi.identityOff = checkTrueFalse(ef.getAttributeValue("identityOff"), false);
    fi.truncZeroes = checkTrueFalse(ef.getAttributeValue("truncZeroes"), false);

    // il parametro <local/> può essere omesso solo se esiste <foreign-adapter/>
    if((fi.localField == null || fi.localField.first == null) && fi.foreignAdapterElement == null)
      throw new SyncSetupErrorException(0, "field/local:name");

    // carica eventuali adapter per il campo
    if(fi.localAdapterElement != null)
    {
      fi.localAdapter = SyncPluginFactory.getInstance().buildAdapter(
         getRole(), okStr(fi.localAdapterElement.getAttributeValue("name")));
      fi.localAdapter.setXML(location, fi.localAdapterElement);
    }

    if(fi.adapterElement != null)
    {
      fi.adapter = SyncPluginFactory.getInstance().buildAdapter(
         getRole(), okStr(fi.adapterElement.getAttributeValue("name")));
      fi.adapter.setXML(location, fi.adapterElement);
    }

    return fi;
  }

  private void populateFieldName(Element ef, FieldLinkInfoBean fi)
     throws SyncSetupErrorException
  {
    Pair<String, String> uniquename = Utils.parseNameTypeIgnore(ef.getChild("name"));
    if(uniquename != null)
    {
      fi.localField = new Pair<>(uniquename.first, uniquename.second);
      fi.foreignField = new Pair<>(uniquename.first, uniquename.second);
      return;
    }

    String tmp = okStrNull(ef.getAttributeValue("name"));
    if(tmp != null)
    {
      fi.localField = new Pair<>(tmp, null);
      fi.foreignField = new Pair<>(tmp, null);
      return;
    }

    String tmp1 = okStrNull(ef.getAttributeValue("local"));
    String tmp2 = okStrNull(ef.getAttributeValue("foreign"));
    if(tmp1 != null && tmp2 != null)
    {
      fi.localField = new Pair<>(tmp1, null);
      fi.foreignField = new Pair<>(tmp2, null);
      return;
    }

    fi.localField = Utils.parseNameTypeIgnore(ef.getChild("local"));
    fi.foreignField = Utils.parseNameTypeThrow(ef.getChild("foreign"), "field/foreign:name");
  }

  @Override
  public void populateConfigForeign(Map hr)
     throws Exception
  {
    hr.put("dataBlockName", dataBlockName);

    VectorRpc v = new VectorRpc();
    hr.put("fields", v);

    for(FieldLinkInfoBean f : arFields)
    {
      HashtableRpc h = new HashtableRpc();
      v.add(h);

      h.put("field", Utils.createNameTypeMap(f.foreignField, "name", "type"));
      h.put("shared", f.shared);
      h.put("primary", f.primary);
      h.put("identityOff", f.identityOff);
      h.put("truncZeroes", f.truncZeroes);

      if(f.foreignAdapterElement != null)
      {
        h.put("foreign-adapter", okStr(f.foreignAdapterElement.getAttributeValue("name")));
        h.put("foreign-adapter-data", Utils.parseParams(f.foreignAdapterElement));
      }

      if(f.adapterElement != null)
      {
        h.put("adapter", okStr(f.adapterElement.getAttributeValue("name")));
        HashtableRpc ha = new HashtableRpc();
        f.adapter.populateConfigForeign(ha);
        h.put("adapter-data", ha);
      }

      if(f.foreignFieldValidatorElement != null)
      {
        h.put("foreign-field-validator", okStr(f.foreignFieldValidatorElement.getAttributeValue("name")));
        h.put("foreign-field-validator-data", Utils.parseParams(foreignRecordValidatorElement));
      }
    }

    if(foreignRecordValidatorElement != null)
    {
      hr.put("record-validator", okStr(foreignRecordValidatorElement.getAttributeValue("name")));
      hr.put("record-validator-data", Utils.parseParams(foreignRecordValidatorElement));
    }

    if(foreignTableValidatorElement != null)
    {
      hr.put("table-validator", okStr(foreignTableValidatorElement.getAttributeValue("name")));
      hr.put("table-validator-data", Utils.parseParams(foreignTableValidatorElement));
    }

    if(delStrategyElement != null)
    {
      String name = delStrategyElement.getAttributeValue("name");
      HashMap params = new HashMap();
      delStrategy.populateConfigForeign(params);

      hr.put("delete-strategy-name", name);
      hr.put("delete-strategy-data", params);
    }

    if(filter != null)
      Utils.formatFilterKeyData(filter, hr);

    hr.put("ignoreInEmptyFields", ignoreInEmptyFields);
    hr.put("isolateRecord", isolateRecord);
    hr.put("isolateAllRecords", isolateAllRecords);
  }

  /**
   * Popola la lista record ottenuta.
   * @param uniqueName nome tabella o altro identificatore unico
   * @param dbName
   * @param lsRecs lista dei records
   * @param arRealFields
   * @param vResult vettore da popolare
   * @param context contesto dell'aggiornamento
   * @throws Exception
   */
  protected void popolaTuttiRecords(String uniqueName, String dbName, List<Record> lsRecs,
     List<FieldLinkInfoBean> arRealFields, VectorRpc vResult, SyncContext context)
     throws Exception
  {
    if(localRecordValidator != null)
      localRecordValidator.masterPreparaValidazione(uniqueName, dbName, lsRecs, arRealFields, context);

    for(int i = 0; i < arRealFields.size(); i++)
    {
      FieldLinkInfoBean f = arRealFields.get(i);
      if(f.localAdapter != null)
        f.localAdapter.masterPreparaValidazione(uniqueName, dbName, lsRecs, arRealFields, f, context);
      if(f.adapter != null)
        f.adapter.masterPreparaValidazione(uniqueName, dbName, lsRecs, arRealFields, f, context);
    }

    for(Record r : lsRecs)
    {
      HashtableRpc recVals = popolaRecord(r, arRealFields, new HashtableRpc());

      if(recVals != null)
        vResult.add(recVals);
    }

    for(int i = 0; i < arRealFields.size(); i++)
    {
      FieldLinkInfoBean f = arRealFields.get(i);
      if(f.localAdapter != null)
        f.localAdapter.masterFineValidazione(uniqueName, dbName, lsRecs, arRealFields, f, context);
      if(f.adapter != null)
        f.adapter.masterFineValidazione(uniqueName, dbName, lsRecs, arRealFields, f, context);
    }

    if(localRecordValidator != null)
      localRecordValidator.masterFineValidazione(uniqueName, dbName, lsRecs, arRealFields, context);
  }

  protected HashtableRpc popolaRecord(Record r, List<FieldLinkInfoBean> arRealFields, HashtableRpc hr)
     throws Exception
  {
    if(localRecordValidator != null)
      if(localRecordValidator.masterValidaRecord(null, r, arRealFields) != 0)
        return null;

    for(int i = 0; i < arRealFields.size(); i++)
    {
      FieldLinkInfoBean f = arRealFields.get(i);
      Value valore = r.getValue(f.localField.first);
      if(valore.isNull())
        continue;

      // attenzione: salva il valore con il nome remoto del campo
      hr.put(f.foreignField.first, popolaValore(null, r, valore, f));
    }

    return hr;
  }

  protected Object popolaValore(String key, Record r, Value v, FieldLinkInfoBean f)
     throws Exception
  {
    Object rv;
    if(f.localAdapter != null && (rv = f.localAdapter.masterValidaValore(key, r, v, f)) != null)
      return rv;
    if(f.adapter != null && (rv = f.adapter.masterValidaValore(key, r, v, f)) != null)
      return rv;

    return Utils.mapVillageToObject(v);
  }

  protected FieldLinkInfoBean findField(String nomeCampo)
  {
    for(FieldLinkInfoBean f : arFields)
    {
      if(isEqu(nomeCampo, f.localField.first))
        return f;
    }
    return null;
  }

  @Override
  protected String convertValue(Object valore, Pair<String, String> field, String tableName, Column col, boolean truncZeroes)
  {
    int tipo = col.typeEnum();
    String s = okStr(valore);

    if(truncZeroes)
      s = removeZero(s);

    if(DbUtils.isNumeric(tipo))
    {
      if(!NumberUtils.isNumber(s))
      {
        log.error("Tipo campo non congruente [tabella:colonna:valore] " + tableName + ":" + field.first + ":" + valore);
        return null;
      }
      return s;
    }

    if(DbUtils.isString(tipo) || DbUtils.isDate(tipo))
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

    if(DbUtils.isNumeric(tipo))
      return col.nullAllowed() ? "NULL" : "0";

    if(DbUtils.isString(tipo))
      return col.nullAllowed() ? "NULL" : "''";

    if(DbUtils.isDate(tipo))
      return col.nullAllowed() ? "NULL" : "'" + now + "'";

    return "NULL";
  }
}
