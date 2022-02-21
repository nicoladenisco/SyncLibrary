/*
 *  AgentCopyUpdateSlave.java
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
package it.infomed.sync.sincronizzazione.plugin.slave;

import it.infomed.sync.common.SyncContext;
import it.infomed.sync.db.DbPeer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Adapter di copia completa tabelle.
 * In effetti è molto semplice: preleva tutti i records dal master e
 * dopo aver cancellato tutti records locali inserisce quelli ricevuti.
 * La delete strategy viene ignorata.
 * Se presenti prima del salvataggio records vengono eseguite una serie
 * di statement SQL, generalmente per cancellare il contenuto in modo selettivo.
 * Se non sono presenti, una DELETE FROM nome_tabella viene eseguita in alternativa.
 * <pre>
 *    [sql-statements]
 *      [statement]....[/statement]
 *    [/sql-statements]
 * </pre>
 * @author Nicola De Nisco
 */
public class AgentCopyUpdateSlave extends AgentTableUpdateSlave
{
  protected Element sqlStatements;
  protected final List<String> statements = new ArrayList<>();

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    sqlStatements = data.getChild("sql-statements");
    List<Element> lsStatements = sqlStatements.getChildren("statement");
    for(Element estat : lsStatements)
    {
      String sql = okStrNull(estat.getText());
      if(sql != null)
        statements.add(sql);
    }

    delStrategy = null;
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
      salvaTuttiRecords(v, context);
  }
}
