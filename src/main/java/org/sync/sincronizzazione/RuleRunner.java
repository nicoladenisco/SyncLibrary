/*
 *  RuleRunner.java
 *  Creato il Mar 14, 2020, 10:34:04 AM
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
package org.sync.sincronizzazione;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.SimpleTimer;
import org.commonlib5.utils.StringOper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.sync.SyncClientInterface;
import org.sync.common.RecordKeysData;
import org.sync.common.RecordsDataBlock;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.common.plugin.RecordTimestampData;
import org.sync.common.plugin.SyncPluginFactory;
import org.sync.common.plugin.SyncRulePlugin;
import org.sync.db.Database;
import org.sync.db.DatabaseException;
import org.sync.db.DbPeer;

/**
 * Esecutore di regole di sincronizzazione.
 * Questa classe va eseguita dal client che chiede di sincronizzarsi con un server.
 * Il trasporto fra client e server avviene attraverso un implementazione di SyncClientInterface.
 *
 * @author Nicola De Nisco
 */
public abstract class RuleRunner
{
  public static final String SYNC_TIMESTAMP_TABLE = "CAL_SYNC_TS";
  private static final Log log = LogFactory.getLog(RuleRunner.class);
  //
  protected Date oldTimestamp;
  protected SyncClientInterface remote;
  protected boolean initalized = false;

  protected void init()
     throws Exception
  {
    if(!initalized)
    {
      // verifica esistenza tabella di appoggio e creazione eventuale
      String[] dbNames = Database.getDBNames();
      for(String dbName : dbNames)
      {
        if(!DbPeer.existTable(SYNC_TIMESTAMP_TABLE, dbName))
          creaTabellaSupporto(dbName);
      }
      initalized = true;
    }
  }

  public void doRun(String regola, SyncClientInterface client, Map<String, String> extraFilter)
  {
    try
    {
      this.remote = client;

      init();
      SyncContext context = new SyncContext();
      context.put("extraFilter", extraFilter);
      Element xmlSetup = client.acquisiciStrategia(regola, context);

      String ruleName = context.getAsString("rule-name", regola);
      String roleType = context.getAsString("your-role", "slave");
      String ruleType = context.getAsString("rule-type", "generic");

      SyncRulePlugin rule = SyncPluginFactory.getInstance().buildRule(RoleType.fromString(roleType), ruleType);
      rule.setXML(LocationType.FOREIGN, xmlSetup);

      try
      {
        rule.beginRumbleRule(context);
        client.beginRumbleRule(regola, context);

        switch(rule.getRole())
        {
          case MASTER:
            playRoleMaster(ruleName, rule, context);
            break;

          case SLAVE:
            playRoleSlave(ruleName, rule, context);
            break;
        }
      }
      catch(Throwable t)
      {
        log.error("Errore fatale: fine operazioni in playRole.", t);
      }
      finally
      {
        rule.endRumbleRule(context);
        client.endRumbleRule(regola, context);
      }
    }
    catch(Throwable t)
    {
      log.error("Errore fatale: fine operazioni.", t);
    }
  }

  protected void playRoleMaster(String nomeRegola, SyncRulePlugin rule, SyncContext context)
     throws Exception
  {
    SimpleTimer st = new SimpleTimer();
    log.info("Inizio esecuzione regola master " + nomeRegola);

    List<String> lsBlocks = rule.getListaBlocchi();
    for(String nomeBlocco : lsBlocks)
    {
      log.info("Elaborazione blocco " + nomeBlocco);

      // crea un context locale per azzerare il context ad ogni iterazione
      SyncContext ctx = (SyncContext) context.clone();
      RecordTimestampData parametri = new RecordTimestampData();
      RecordKeysData aggiorna = new RecordKeysData();

      // interroga il server per i candidati per l'aggiornamento
      remote.verificaBlocco(nomeRegola, nomeBlocco, parametri, oldTimestamp, aggiorna, ctx);
      debugLog(nomeBlocco, "Verifica slave richiede %d record.", parametri.size());

      // elabora dati dei candidati
      rule.verificaBlocco(nomeBlocco, parametri, oldTimestamp, aggiorna, ctx);
      parametri.clear();

      if(aggiorna.isEmpty())
      {
        debugLog(nomeBlocco, "Verifica master non richiede record.");
        continue;
      }

      // pianifica la richiesta dati per l'aggiornamento
      remote.pianificaAggiornamento(nomeRegola, nomeBlocco, aggiorna, ctx);

      if(aggiorna.isEmpty())
      {
        debugLog(nomeBlocco, "Pianificazione slave non richiede record.");
        continue;
      }

      // elabora i dati dei record da aggiornare
      RecordsDataBlock lsRecs = new RecordsDataBlock();
      rule.aggiornaBlocco(nomeBlocco, aggiorna, lsRecs, ctx);

      if(lsRecs.isEmpty())
        debugLog(nomeBlocco, "Il master ha prodotto i dati richiesti.");
      else
        debugLog(nomeBlocco, "Il master ha prodotto %d record.", lsRecs.size());

      // invia i dati al server per l'aggiornamento
      remote.aggiornaBlocco(nomeRegola, nomeBlocco, aggiorna, lsRecs, ctx);
      debugLog(nomeBlocco, "I dati prodotti dal master sono stati inviati.");
    }

    log.info("Fine esecuzione regola master " + nomeRegola + ". Eseguita in " + st.getElapsed() + " millisecondi.");
  }

  protected void playRoleSlave(String nomeRegola, SyncRulePlugin rule, SyncContext context)
     throws Exception
  {
    SimpleTimer st = new SimpleTimer();
    log.info("Inizio esecuzione regola slave " + nomeRegola);

    List<String> lsBlocks = rule.getListaBlocchi();
    for(String nomeBlocco : lsBlocks)
    {
      log.info("Elaborazione blocco " + nomeBlocco);

      // crea un context locale per azzerare il context ad ogni iterazione
      SyncContext ctx = (SyncContext) context.clone();
      RecordTimestampData parametri = new RecordTimestampData();
      RecordKeysData aggiorna = new RecordKeysData();

      // prepera dati per l'interrogazione al server
      rule.verificaBlocco(nomeBlocco, parametri, null, aggiorna, ctx);
      debugLog(nomeBlocco, "Verifica slave sottopone %d record al master.", parametri.size());

      // interroga il server con i candidati per l'aggiornamento
      remote.verificaBlocco(nomeRegola, nomeBlocco, parametri, oldTimestamp, aggiorna, ctx);
      parametri.clear();

      if(aggiorna.isEmpty())
      {
        debugLog(nomeBlocco, "Verifica master non richiede aggiornamento record.");
        continue;
      }

      debugLog(nomeBlocco, "Verifica master risponde con %d record da aggiornare/inserire/cancellare.", aggiorna.size());

      // pianifica la richiesta dati per l'aggiornamento
      rule.pianificaAggiornamento(nomeBlocco, aggiorna, ctx);

      if(aggiorna.isEmpty())
      {
        debugLog(nomeBlocco, "Pianificazione slave non richiede record.");
        continue;
      }

      debugLog(nomeBlocco, "Pianificazione slave richiede %d record al master per aggiornare/inserire.", aggiorna.size());

      // interroga il remote per i dati dei record da aggiornare
      RecordsDataBlock lsRecs = new RecordsDataBlock();
      remote.aggiornaBlocco(nomeRegola, nomeBlocco, aggiorna, lsRecs, ctx);

      if(lsRecs.isEmpty())
        debugLog(nomeBlocco, "Il master ha prodotto i dati richiesti ma nessun record è stato prodotto.");
      else
        debugLog(nomeBlocco, "Il master ha prodotto %d record per aggiornare/inserire.", lsRecs.size());

      // applica aggiornamento al data block; i dati sono nel ctx
      rule.aggiornaBlocco(nomeBlocco, aggiorna, lsRecs, ctx);
      debugLog(nomeBlocco, "I dati prodotti dal master sono stati salvati.");
    }

    log.info("Fine esecuzione regola slave " + nomeRegola + ". Eseguita in " + st.getElapsed() + " millisecondi.");
  }

  private void debugLog(String nomeBlocco, String fmt, Object... args)
  {
    if(log.isDebugEnabled())
    {
      String msg = String.format(fmt, args);
      log.debug(nomeBlocco + " : " + msg);
    }
  }

  protected void creaTabellaSupporto(String dbName)
     throws DatabaseException
  {
    // primo tentativo: sintassi Microsoft SQL server
    try
    {
      String sSQL = "CREATE TABLE " + SYNC_TIMESTAMP_TABLE + " (\n"
         + "  TABLE_NAME VARCHAR (155), \n"
         + "  SHARED_KEY VARCHAR (155), \n"
         + "  LAST_UPDATE DATETIME, \n"
         + "  CONSTRAINT " + SYNC_TIMESTAMP_TABLE + "_PKEY PRIMARY KEY (TABLE_NAME,SHARED_KEY)\n"
         + ")\n";

      DbPeer.createTimestampTable(sSQL, dbName);
      return;
    }
    catch(DatabaseException e)
    {
      String error = e.getMessage().toLowerCase(Locale.ROOT);
      if(!(error.contains("datetime") && (error.contains("does not exist") || error.contains("non esiste"))))
        throw e;
    }

    // seconto tentativo: sintassi Postgres SQL server
    try
    {
      String sSQL = "CREATE TABLE " + SYNC_TIMESTAMP_TABLE + " (\n"
         + "  TABLE_NAME VARCHAR (155), \n"
         + "  SHARED_KEY VARCHAR (155), \n"
         + "  LAST_UPDATE TIMESTAMP, \n"
         + "  CONSTRAINT " + SYNC_TIMESTAMP_TABLE + "_PKEY PRIMARY KEY (TABLE_NAME,SHARED_KEY)\n"
         + ")\n";

      DbPeer.createTimestampTable(sSQL, dbName);
      return;
    }
    catch(DatabaseException e)
    {
      if(!e.getMessage().contains("type \"datetime\" does not exist"))
        throw e;
    }

    throw new DatabaseException("Generic error create timestamp table.");
  }

  public Date getOldTimestamp()
  {
    return oldTimestamp;
  }

  public void setOldTimestamp(Date oldTimestamp)
  {
    this.oldTimestamp = oldTimestamp;
  }

  public Element findPlugin(String application, String target, Document doc)
     throws Exception
  {
    // cerca una regola controllando gli attributi
    // application="diamante" master="caleido" target="all"
    Optional<Element> rule = doc.getRootElement().getChildren("rule").stream()
       .filter((e) -> StringOper.isEquAny(e.getAttributeValue("application"), "any", application)
          && StringOper.isEquAny(e.getAttributeValue("target"), "any", target))
       .findFirst();

    if(!rule.isPresent())
      throw new SyncSetupErrorException(String.format("Nessuna regola corrisponde ai parametri."));

    return rule.get();
  }

  public Element findPlugin(String nomeRegola, Document doc)
     throws Exception
  {
    // cerca una regola controllando gli attributi
    // application="diamante" master="caleido" target="all"
    Optional<Element> rule = doc.getRootElement().getChildren("rule").stream()
       .filter((e) -> StringOper.isEqu(nomeRegola, e.getAttributeValue("name")))
       .findFirst();

    if(!rule.isPresent())
      throw new SyncSetupErrorException(String.format("La regola %s non esiste; rivedere setup.", nomeRegola));

    return rule.get();
  }
}
