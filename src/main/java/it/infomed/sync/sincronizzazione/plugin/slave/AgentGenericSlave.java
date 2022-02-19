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
import java.util.*;
import org.apache.commons.configuration.Configuration;
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
  protected ArrayList<FieldLinkInfoBean> arFields = new ArrayList<>();
  protected Element recordValidatorElement, delStrategyElement;
  protected SyncValidatorPlugin recordValidator;
  protected String dataBlockName, databaseName;
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
    return ROLE_SLAVE;
  }

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    this.data = data;

    if((dataBlockName = okStrNull(data.getAttributeValue("name"))) == null)
      throw new SyncSetupErrorException(0, "name");

    databaseName = okStr(data.getAttributeValue("database-name"), getParentRule().getDatabaseName());

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

      // carica eventuali validator
      if(recordValidatorElement != null)
      {
        recordValidator = SyncPluginFactory.getInstance().buildValidator(getRole(),
           okStr(recordValidatorElement.getAttribute("name").getValue()));
        recordValidator.setParentAgent(this);
        recordValidator.setXML(location, recordValidatorElement);
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
    populateFieldName(ef, fi, location);

    fi.adapterElement = Utils.getChildTestName(ef, location + "-adapter");

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

  protected boolean isSelect(FieldLinkInfoBean f)
  {
    return false;
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

  protected void caricaTipiColonne(Schema schema)
     throws Exception
  {
    this.schema = schema;

    for(FieldLinkInfoBean f : arFields)
    {
      if(!isOkStr(f.field.second))
      {
        Column col = findInSchema(f.field.first);
        // reimposta nome per avere il case corretto
        f.field.first = col.name();
        f.field.second = col.type();
      }
    }
  }
}
