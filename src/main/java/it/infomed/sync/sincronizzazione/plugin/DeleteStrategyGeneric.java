/*
 *  DeleteStrategy.java
 *  Creato il Jan 31, 2020, 6:40:20 PM
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

import com.workingdogs.village.Column;
import com.workingdogs.village.DataSetException;
import com.workingdogs.village.Record;
import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractAgent;
import it.infomed.sync.common.plugin.AbstractDelete;
import it.infomed.sync.db.DbPeer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Informazioni sulla logica di cancellazione.
 * La logica di cancellazione viene specificata a livello di regola
 * oppure in override a livello di datablock.
 *
 * <pre>
 *     [delete-strategy name="generic"]
 *       [unknow sameasdelete="true" /]
 *       [delete type="logical"]
 *         [field name="STATO_REC" normal="0" delete="10" /]
 *       [/delete]
 *     [/delete-strategy]
 * </pre>
 *
 * La cancellazione viene implementata solo dagli slave.
 * I parametri con 'Delete' vengono utilizzati quando il master segnala 'key/DELETE'
 * ovvero quando il record è stato cancellato logicamente nel master.
 * I parametri con 'Unknow' vengono utilizzati quando il master segnala 'key/UNKNOW'
 * a indicare un record che non è presente nel master.
 * L'attributo del tag unknow sameasdelete="true" significa che il comportamento di
 * unknow deve essere lo stesso di delete.
 * <br>
 * tipo... può essere nothing, logical, phisical
 * sqlUpdate...Statement sono la componente set di una istruzione UPDATE
 * costruita internamente dal sincronizzatore.
 * sqlGeneric...Statement sono delle istruzioni SQL complete eseguite
 * per cancellare; in queste istruzioni la macro ${key} si espande nella
 * chiave da cancellare (nomecampo=valore and nomecampo=valore).
 * <br>
 * entrambi gli attibuti sql-update e sql possono apparire più volte.
 *
 * @author Nicola De Nisco
 */
public class DeleteStrategyGeneric extends AbstractDelete
{
  public static class DeleteField implements Cloneable
  {
    public Pair<String, String> field;
    public String valueNormal, valueDelete;

    @Override
    public Object clone()
       throws CloneNotSupportedException
    {
      DeleteField d = (DeleteField) super.clone();
      d.field = new Pair<>(field.first, field.second);
      d.valueDelete = valueDelete;
      d.valueNormal = valueNormal;
      return d;
    }
  }

  public String tipoUnknow = "nothing";
  public final List<DeleteField> sqlUpdateUnknowStatement = new ArrayList<>();
  public final List<String> sqlGenericUnknowStatement = new ArrayList<>();

  public String tipoDelete = "nothing";
  public final List<DeleteField> sqlUpdateDeleteStatement = new ArrayList<>();
  public final List<String> sqlGenericDeleteStatement = new ArrayList<>();

  private String databaseNameForeign, tableNameForeign;

  @Override
  public Object clone()
     throws CloneNotSupportedException
  {
    DeleteStrategyGeneric dg = (DeleteStrategyGeneric) super.clone();

    dg.sqlUpdateDeleteStatement.clear();
    dg.sqlUpdateUnknowStatement.clear();

    for(DeleteField df : sqlUpdateDeleteStatement)
      dg.sqlUpdateDeleteStatement.add((DeleteField) df.clone());

    for(DeleteField df : sqlUpdateUnknowStatement)
      dg.sqlUpdateUnknowStatement.add((DeleteField) df.clone());

    return dg;
  }

  @Override
  public void setXML(String location, Element eds)
     throws Exception
  {
    {
      Element edel = eds.getChild("delete");
      if(edel != null)
      {
        tipoDelete = okStr(edel.getAttributeValue("type"), "logical");
        List<Element> lsSQL = edel.getChildren("sql");
        for(Element e1 : lsSQL)
        {
          String s = e1.getTextTrim();
          if(!s.isEmpty())
            sqlGenericDeleteStatement.add(s);
        }
        List<Element> lsSQLupd = edel.getChildren("field");
        for(Element e2 : lsSQLupd)
        {
          DeleteField f = new DeleteField();
          f.field = Utils.parseNameTypeThrow(e2, "delete-strategy:delete:field");
          f.valueNormal = e2.getAttributeValue("normal");
          f.valueDelete = e2.getAttributeValue("delete");
          sqlUpdateDeleteStatement.add(f);
        }
      }
    }

    {
      Element eunk = eds.getChild("unknow");
      if(eunk != null)
      {
        if("true".equals(eunk.getAttributeValue("sameasdelete")))
        {
          // copia i valori di delete in unknow
          tipoUnknow = tipoDelete;
          sqlGenericUnknowStatement.addAll(sqlGenericDeleteStatement);
          sqlUpdateUnknowStatement.addAll(sqlUpdateDeleteStatement);
        }
        else
        {
          tipoUnknow = okStr(eunk.getAttributeValue("type"), "logical");
          List<Element> lsSQL = eunk.getChildren("sql");
          for(Element e1 : lsSQL)
          {
            String s = e1.getTextTrim();
            if(!s.isEmpty())
              sqlGenericUnknowStatement.add(s);
          }
          List<Element> lsSQLupd = eunk.getChildren("field");
          for(Element e2 : lsSQLupd)
          {
            DeleteField f = new DeleteField();
            f.field = Utils.parseNameTypeThrow(e2, "delete-strategy:delete:field");
            f.valueNormal = e2.getAttributeValue("normal");
            f.valueDelete = e2.getAttributeValue("delete");
            sqlUpdateUnknowStatement.add(f);
          }
        }
      }
    }

  }

  @Override
  public void caricaTipiColonne(String databaseName, String tableName)
     throws Exception
  {
    this.databaseNameForeign = databaseName;
    this.tableNameForeign = tableName;

    for(DeleteField f : sqlUpdateDeleteStatement)
    {
      if(!isOkStr(f.field.second))
      {
        f.field.second = findInSchema(f.field.first).type();
      }
    }
    for(DeleteField f : sqlUpdateUnknowStatement)
    {
      if(!isOkStr(f.field.second))
      {
        f.field.second = findInSchema(f.field.first).type();
      }
    }
  }

  protected Column findInSchema(String nomeColonna)
     throws Exception
  {
    AbstractAgent agent = (AbstractAgent) getParentAgent();
    return agent.findInSchema(nomeColonna);
  }

  @Override
  public void cancellaRecordsPerDelete(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    cancellazioneLogica(lsKeys, databaseNameForeign, tableNameForeign, arForeignKeys,
       sqlUpdateDeleteStatement, sqlGenericDeleteStatement,
       context);
  }

  @Override
  public void cancellaRecordsPerUnknow(List<String> lsKeys, Map<String, String> arForeignKeys, SyncContext context)
     throws Exception
  {
    cancellazioneLogica(lsKeys, databaseNameForeign, tableNameForeign, arForeignKeys,
       sqlUpdateUnknowStatement, sqlGenericUnknowStatement,
       context);
  }

  protected void cancellazioneLogica(List<String> lsKeys,
     String databaseNameForeign, String tableNameForeign, Map<String, String> arForeignKeys,
     List<DeleteField> sqlUpdate, List<String> sqlGeneric,
     SyncContext context)
     throws Exception
  {
    if(lsKeys.isEmpty())
      return;

    String updateStm = join(sqlUpdate,
       (f) -> f.field.first + "=" + Utils.convertValue(f.valueDelete, f.field),
       ",", null);

    switch(arForeignKeys.size())
    {
      case 0:
        return;

      case 1:
        // versione ottimizzata per chiave singola
        Map.Entry<String, String> fkey = arForeignKeys.entrySet().iterator().next();
        String nomeCampo = fkey.getKey();
        String tipoCampo = fkey.getValue();
        boolean numeric = Utils.isNumeric(tipoCampo);

        if(!updateStm.isEmpty())
        {
          String where = "";
          if(numeric)
            where = join(lsKeys.iterator(), ',');
          else
            where = join(lsKeys.iterator(), ',', '\'');

          String sSQL
             = "UPDATE " + tableNameForeign + "\n"
             + "   SET " + updateStm + "\n"
             + " WHERE " + nomeCampo + " IN (" + where + ")";
          DbPeer.executeStatement(sSQL, databaseNameForeign);
        }

        if(!sqlGeneric.isEmpty())
        {
          for(String key : lsKeys)
          {
            String sWhere = nomeCampo + "=" + (numeric ? key : "'" + key + "'");

            for(String s : sqlGeneric)
            {
              if(s.isEmpty())
                continue;

              s = s.replace("${key}", sWhere);
              DbPeer.executeStatement(s, databaseNameForeign);
            }
          }
        }
        break;

      default:
        // versione generica per chiavi multiple
        for(String key : lsKeys)
        {
          cancellaRecord(key, updateStm, databaseNameForeign, tableNameForeign, arForeignKeys,
             sqlUpdate, sqlGeneric, context);
        }
        break;
    }
  }

  protected void cancellaRecord(String key, String updateStm,
     String databaseNameForeign, String tableNameForeign, Map<String, String> arForeignKeys,
     List<DeleteField> sqlUpdate, List<String> sqlGeneric,
     SyncContext context)
  {
    try
    {
      String sWhere = buildWhere(key, arForeignKeys, context);
      if(sWhere == null)
        return;

      if(!updateStm.isEmpty())
      {
        String sSQL
           = "UPDATE " + tableNameForeign + "\n"
           + "   SET " + updateStm + "\n"
           + " WHERE " + sWhere;
        DbPeer.executeStatement(sSQL, databaseNameForeign);
      }

      if(!sqlGeneric.isEmpty())
      {
        for(String s : sqlGeneric)
        {
          if(s.isEmpty())
            continue;

          s = s.replace("${key}", sWhere);
          DbPeer.executeStatement(s, databaseNameForeign);
        }
      }
    }
    catch(Exception ex)
    {
      log.error("Errore in cancellazione record " + key, ex);
    }
  }

  @Override
  public boolean confermaValoriRecord(Map r, String now,
     String key, Map<String, String> arKeys,
     Map<String, String> valoriSelect, Map<String, String> valoriUpdate, Map<String, String> valoriInsert,
     SyncContext context, Connection con)
     throws Exception
  {
    // reimposta lo stato_rec al valore di non cancellato
    for(DeleteField f : sqlUpdateDeleteStatement)
    {
      if(isOkStr(f.valueNormal) && findInSchema(f.field.first) != null)
      {
        valoriInsert.put(f.field.first, Utils.convertValue(f.valueNormal, f.field));
        valoriUpdate.put(f.field.first, Utils.convertValue(f.valueNormal, f.field));
      }
    }

    return true;
  }

  @Override
  public boolean queryRecordDeleted(Record r, SyncContext context)
     throws Exception
  {
    try
    {
      DeleteField f = sqlUpdateDeleteStatement.get(0);
      String val = r.getValue(f.field.first).asString();
      return isEqu(f.valueDelete, val);
    }
    catch(DataSetException e)
    {
      return false;
    }
  }
}
