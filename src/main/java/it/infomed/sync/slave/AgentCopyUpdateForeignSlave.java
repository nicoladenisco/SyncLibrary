/*
 *  AgentCopyUpdateForeignSlave.java
 *  Creato il 9-lug-2021, 18.51.40
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
package it.infomed.sync.slave;

import com.workingdogs.village.Column;
import com.workingdogs.village.Schema;
import it.infomed.sync.FieldLinkInfoBean;
import it.infomed.sync.SyncContext;
import it.infomed.sync.SyncSetupErrorException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.commonlib5.utils.Pair;

/**
 * Adapter di copia completa tabelle.
 * In effetti è molto semplice: preleva tutti i records dal master e
 * dopo aver cancellato tutti records locali inserisce quelli ricevuti.
 * La delete strategy viene ignorata.
 * Se presenti prima del salvataggio records vengono eseguite una serie
 * di statement SQL, generlmente per cancellare il contenuto in modo selettivo.
 * Se non sono presenti una DELETE FROM nome_tabella viene eseguita in alternativa.
 * @author Nicola De Nisco
 */
public class AgentCopyUpdateForeignSlave extends AgentSharedGenericForeignSlave
{
  protected String tableName, databaseName;
  protected Schema tableSchema;
  protected List statements;

  @Override
  public void setConfig(String nomeAgent, Map vData)
     throws Exception
  {
    super.setConfig(nomeAgent, vData);

    tableName = (String) vData.get("foreign-table-name");
    databaseName = (String) vData.get("foreign-table-database");

    // statements da eseguire al momento della cancellazione
    statements = (List) vData.get("sql-statements");

    caricaTipiColonne();
    delStrategy = null;
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
    // non fa nulla: come se la tabella slave fosse vuota
    // questo provocherà il trasferimento di tutte le chiavi dal master
  }

  @Override
  public void pianificaAggiornamento(List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    int pos;

    // analizza le chiavi del master in modo molto semplice (dovrebbero essere tutte NEW)
    // qui siamo interessati solo ai nuovi records: il contenuto attuale verrà cancellato
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
        }
      }
    }
  }

  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    if(statements == null || statements.isEmpty())
    {
      // cancella il contenuto attuale della tabella
      String delSQL = "DELETE FROM " + tableName;
      DbPeer.executeStatement(delSQL, databaseName);
    }
    else
    {
      for(Object sql : statements)
      {
        String sSQL = okStrNull(sql);
        if(sSQL != null)
          DbPeer.executeStatement(sSQL, databaseName);
      }
    }

    Vector v = (Vector) context.getNotNull("records-data");
    if(!v.isEmpty())
      salvaTuttiRecords(tableName, databaseName, tableSchema, v, context);
  }

  /**
   * Determina i tipi colonne utilizzando le informazioni di runtime del database.
   * @throws Exception
   */
  protected void caricaTipiColonne()
     throws Exception
  {
    tableSchema = Database.schemaTable(databaseName, tableName);

    if(tableSchema == null)
      throw new SyncSetupErrorException(String.format(
         "Tabella %s non trovata nel database %s.", tableName, databaseName));

    if(timeStampForeign != null && !isOkStr(timeStampForeign.second) && isEquNocase(timeStampForeign.first, "AUTO"))
      timeStampForeign.second = findInSchema(timeStampForeign.first).type();

    for(FieldLinkInfoBean f : arFields)
    {
      if(!isOkStr(f.foreignField.second))
      {
        f.foreignField.second = findInSchema(f.foreignField.first).type();
      }
    }

    for(Pair<String, String> f : arForeignKeys.getAsList())
    {
      if(!isOkStr(f.second))
      {
        f.second = findInSchema(f.first).type();
      }
    }
  }

  protected Column findInSchema(String nomeColonna)
     throws Exception
  {
    return findInSchema(tableSchema, nomeColonna);
  }
}
