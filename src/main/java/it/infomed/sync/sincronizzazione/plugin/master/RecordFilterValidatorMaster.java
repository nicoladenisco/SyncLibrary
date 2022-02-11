/*
 *  RecordFilterValidatorMaster.java
 *  Creato il 29-ott-2021, 10.23.13
 *
 *  Copyright (C) 2021 Informatica Medica s.r.l.
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
import it.infomed.sync.common.FieldFilterBean;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractValidator;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.StringOper;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;

/**
 * Filtra record in ingresso in base a valori predefiniti.
 *
 * @author Nicola De Nisco
 */
public class RecordFilterValidatorMaster extends AbstractValidator
{
  private Element data;
  private List<FieldFilterBean> arFiltri = new ArrayList<>();
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

    String tmp;
    if((tmp = setup.getAsString("num")) == null)
    {
      FieldFilterBean fb = new FieldFilterBean();
      if((fb.fieldName = setup.getAsString("field")) == null)
        return;

      fb.cmp = setup.getAsString("cmp", "EQ");

      if((tmp = setup.getAsString("value")) == null)
        return;

      fb.values.addAll(StringOper.string2List(tmp, ",", true));

      if(fb.values.isEmpty())
        return;

      arFiltri.add(fb);
    }
    else
    {
      int num = parse(tmp, 0);
      for(int i = 0; i < num; i++)
      {
        String pref = i + "_";

        FieldFilterBean fb = new FieldFilterBean();
        if((fb.fieldName = setup.getAsString(pref + "field")) == null)
          continue;

        fb.cmp = setup.getAsString(pref + "cmp", "EQ");

        if((tmp = setup.getAsString(pref + "value")) == null)
          continue;

        fb.values.addAll(StringOper.string2List(tmp, ",", true));

        if(fb.values.isEmpty())
          continue;

        arFiltri.add(fb);
      }
    }
  }

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName,
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void masterFineValidazione(String uniqueName, String dbName,
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception
  {
    for(FieldFilterBean fb : arFiltri)
    {
      FieldLinkInfoBean fieldInfo = arFields.stream()
         .filter((ff) -> isEqu(fb.fieldName, ff.field.first))
         .findFirst().orElse(null);

      if(fieldInfo == null)
      {
        log.debug("Filtro scartato: campo " + fb.fieldName + " inesistente.");
        continue;
      }

      if(!fb.testRecord(r, fieldInfo.field.second, true))
      {
        // record bloccato dal filtro
        return 1;
      }
    }

    return 0;
  }
}
