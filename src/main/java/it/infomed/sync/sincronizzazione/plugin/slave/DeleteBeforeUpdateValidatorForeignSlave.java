/*
 *  DeleteBeforeUpdateValidatorForeignSlave.java
 *  Creato il May 20, 2020, 7:31:13 PM
 *
 *  Copyright (C) 2020 Informatica Medica s.r.l.
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

import com.workingdogs.village.Record;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.plugin.AbstractValidator;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;

/**
 * Cancella il contenuto di una tabella prima dell'inizio dell'aggiornamento.
 * Nessuna verifica di integrità referenziale.
 * Una semplice DELETE con eventualmente dei parametri.
 * Questo validatore ha senso solo per uno slave e di conseguenza solo i
 * metodi slave sono implementati.
 *
 * @author Nicola De Nisco
 */
public class DeleteBeforeUpdateValidatorForeignSlave extends AbstractValidator
{
  protected String filter;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    filter = data.getAttributeValue("filter");
  }

  @Override
  public void populateConfigForeign(Map context)
     throws Exception
  {
    context.put("filter", filter);
  }

  @Override
  public void setConfig(String nomeAdapter, Map vData)
     throws Exception
  {
    filter = okStr(vData.get("filter"));
  }

  @Override
  public void masterFineValidazione(String tableName, String dbName,
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void slaveFineValidazione(String tableName, String dbName,
     List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName,
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public int masterValidaRecord(String key, Record r, List<FieldLinkInfoBean> arFields)
     throws Exception
  {
    return 0;
  }

  @Override
  public void slavePreparaValidazione(String uniqueName, String dbName,
     List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    String sSQL = "DELETE FROM " + uniqueName;
    if(isOkStr(filter))
      sSQL += " WHERE " + filter;

    DbPeer.executeStatement(sSQL, dbName);
  }

  @Override
  public int slaveValidaRecord(String key, Map record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception
  {
    return 0;
  }
}
