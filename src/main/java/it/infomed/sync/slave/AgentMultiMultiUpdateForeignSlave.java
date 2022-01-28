/*
 *  AgentMultiMultiUpdateForeignSlave.java
 *  Creato il 9-lug-2021, 17.05.15
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

import it.infomed.sync.SyncContext;
import it.infomed.sync.db.DbPeer;
import java.util.List;
import java.util.Vector;

/**
 * Adapter specializzato per una relazione molti a molti.
 * Le relazioni molti a molti sono ottenute attraverso una tabella
 * che collega i relativi record di altre due tabelle.
 * Per una corretta sincronizzazione vanno prima sincronizzate le due tabelle
 * relative e per ultima la tabella di collegamento.
 * Questo agent è specializzato per la tabella di collegamento.
 * In effetti è molto semplice: preleva tutti i records dal master e
 * dopo aver cancellato tutti records locali inserisce quelli ricevuti.
 * La delete strategy viene ignorata.
 * @author Nicola De Nisco
 */
public class AgentMultiMultiUpdateForeignSlave extends AgentCopyUpdateForeignSlave
{
  @Override
  public void aggiornaBlocco(List<String> parametri, SyncContext context)
     throws Exception
  {
    // cancella il contenuto attuale della tabella
    String delSQL = "DELETE FROM " + tableNameForeign;
    DbPeer.executeStatement(delSQL, databaseNameForeign);

    Vector v = (Vector) context.getNotNull("records-data");
    if(!v.isEmpty())
      salvaTuttiRecords(tableNameForeign, databaseNameForeign, tableSchema, v, context);
  }
}
