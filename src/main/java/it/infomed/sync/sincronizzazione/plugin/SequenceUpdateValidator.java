/*
 *  SequenceUpdateValidator.java
 *  Creato il Jan 31, 2020, 2:44:53 PM
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
package it.infomed.sync.sincronizzazione.plugin;

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
 * Reimposta sequenza al valore massimo di un campo.
 * Questo validatore di tabella può essere utlizzato sia come master che come slave.
 * @author Nicola De Nisco
 */
public class SequenceUpdateValidator extends AbstractValidator
{
  protected String field, sequence;
  //  "SELECT pg_catalog.setval('stp.valori_normali_valori_normali_id_seq', 1, false);";

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    field = data.getAttributeValue("field");
    sequence = data.getAttributeValue("sequence");
  }

  @Override
  public void masterFineValidazione(String tableName, String dbName,
     List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    setSequence(tableName, dbName);
  }

  @Override
  public void slaveFineValidazione(String tableName, String dbName,
     List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
    setSequence(tableName, dbName);
  }

  protected void setSequence(String tableName, String dbName)
     throws Exception
  {
    long val = getMax(tableName, dbName, field);
    String sSQL = "SELECT pg_catalog.setval('" + sequence + "', " + val + ", false);";
    DbPeer.executeQuery(sSQL, dbName);
  }

  protected long getMax(String tableName, String dbName, String fieldName)
     throws Exception
  {
    String sSQL = "SELECT MAX(" + fieldName + ") FROM " + tableName;
    List<Record> lsRecs = DbPeer.executeQuery(sSQL, dbName, true);
    return lsRecs.isEmpty() ? 1 : lsRecs.get(0).getValue(1).asLong() + 1;
  }

  @Override
  public void masterPreparaValidazione(String uniqueName, String dbName, List<Record> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
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
  public void slavePreparaValidazione(String uniqueName, String dbName, List<Map> lsRecs, List<FieldLinkInfoBean> arFields, SyncContext context)
     throws Exception
  {
  }

  @Override
  public int slaveValidaRecord(String key, Map record, List<FieldLinkInfoBean> arFields, Connection con)
     throws Exception
  {
    return 0;
  }
}
