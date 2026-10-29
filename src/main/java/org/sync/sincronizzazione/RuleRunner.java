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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.SimpleTimer;
import org.commonlib5.utils.StringOper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.sync.SyncClientInterface;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.common.plugin.SyncPluginFactory;
import org.sync.common.plugin.SyncRulePlugin;
import org.sync.db.Database;
import org.sync.db.DatabaseException;
import org.sync.db.DbPeer;

/**
 * Esecutore di regole di sincronizzazione.
 *
 * @author Nicola De Nisco
 */
public abstract class RuleRunner
{
  public static final String SYNC_TIMESTAMP_TABLE = "CAL_SYNC_TS";
  private static final Log log = LogFactory.getLog(RuleRunner.class);
  //
  protected Date oldTimestamp;
  protected SyncClientInterface client;
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
      this.client = client;

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

      List<Pair<String, Date>> parametri = new ArrayList<>();

      // interroga il server per i candidati per l'aggiornamento
      client.verificaBlocco(nomeRegola, nomeBlocco, parametri, oldTimestamp, ctx);
      log.debug("Verifica slave richiede " + parametri.size() + " record.");

      // elabora dati dei candidati
      rule.verificaBlocco(nomeBlocco, parametri, oldTimestamp, ctx);
      parametri.clear();

      // recupera risposta del server
      List<String> vInfo = (List<String>) ctx.get("records-data");
      if(vInfo.isEmpty())
      {
        log.debug("Verifica master non richiede record.");
        continue;
      }

      ctx.remove("records-data");

      // pianifica la richiesta dati per l'aggiornamento
      client.pianificaAggiornamento(nomeRegola, nomeBlocco, vInfo, ctx);
      List<String> aggiorna = (List<String>) ctx.get("records-data");
      if(aggiorna.isEmpty())
      {
        log.debug("Pianificazione slave non richiede record.");
        continue;
      }

      // elabora i dati dei record da aggiornare
      rule.aggiornaBlocco(nomeBlocco, aggiorna, ctx);

      List v = (List) context.get("records-data");
      if(v == null)
        log.debug("Il master ha prodotto i dati richiesti.");
      else
        log.debug("Il master ha prodotto " + v.size() + " record.");

      // invia i dati al server per l'aggiornamento; i dati sono nel ctx
      client.aggiornaBlocco(nomeRegola, nomeBlocco, aggiorna, ctx);

      log.debug("I dati prodotti dal master sono stati inviati.");
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

      List<String> aggiorna = new ArrayList<>();
      List<Pair<String, Date>> parametri = new ArrayList<>();

      // prepera dati per l'interrogazione al server
      rule.verificaBlocco(nomeBlocco, parametri, null, ctx);
      log.debug("Verifica slave sottopone " + parametri.size() + " record al master.");

      // interroga il server con i candidati per l'aggiornamento
      client.verificaBlocco(nomeRegola, nomeBlocco, parametri, oldTimestamp, ctx);
      parametri.clear();

      // recupera risposta del server
      List<String> vInfo = (List<String>) ctx.get("records-data");
      ctx.remove("records-data");
      if(vInfo.isEmpty())
      {
        log.debug("Verifica master non richiede aggiornamento record.");
        continue;
      }

      log.debug("Verifica master risponde con " + vInfo.size() + " record da aggiornare/inserire/cancellare.");

      // pianifica la richiesta dati per l'aggiornamento
      rule.pianificaAggiornamento(nomeBlocco, aggiorna, vInfo, ctx);

      if(aggiorna.isEmpty())
      {
        log.debug("Pianificazione slave non richiede record.");
        continue;
      }

      log.debug("Pianificazione slave richiede " + aggiorna.size() + " record al master per aggiornare/inserire.");

      // interroga il server per i dati dei record da aggiornare
      client.aggiornaBlocco(nomeRegola, nomeBlocco, aggiorna, ctx);

      List v = (List) ctx.get("records-data");
      if(v == null)
        log.debug("Il master ha prodotto i dati richiesti ma nessun record è stato prodotto.");
      else
        log.debug("Il master ha prodotto " + v.size() + " record per aggiornare/inserire.");

      // applica aggiornamento al data block; i dati sono nel ctx
      rule.aggiornaBlocco(nomeBlocco, aggiorna, ctx);

      log.debug("I dati prodotti dal master sono stati salvati.");
    }

    log.info("Fine esecuzione regola slave " + nomeRegola + ". Eseguita in " + st.getElapsed() + " millisecondi.");
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
