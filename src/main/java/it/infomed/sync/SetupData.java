/*
 *  SetupData.java
 *  Creato il 2-mag-2016, 10.40.06
 *
 *  Copyright (C) 2016 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package it.infomed.sync;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import it.infomed.sync.db.Database;
import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;
import org.commonlib5.utils.*;

/**
 * Dati di setup per CaleidoSyncClient.
 *
 * @author Nicola De Nisco
 */
public class SetupData
{
  /** flags */
  public static boolean debug, simulazione;
  /** proprietà del file di versione */
  public static Properties versionProperties = new Properties();
  /** proprietà persistenti su file separato non presenti in file di configurazione */
  public static PropertyManager pm = new PropertyManager();
  /** directory di storage locale */
  public static File appDir;
  /** server xml-rpc per caleido */
  public static String serverXmlRpc;
  /** porta xml-rcp per caleido */
  public static int portXmlRpc;
  /** url completa xml-rpc per caleido */
  public static String urlXmlRpc;
  /** regola di sincronizzazione da attivare */
  public static String syncRule;
  /** configurazione interna */
  public static Configuration cfg;
  /** filtri attivati dallo slave */
  public static Map<String, String> extraFilter = new HashMap<>();
  //
  private static Log log = LogFactory.getLog(SetupData.class);

  public static void initialize(String[] args)
  {
    try
    {
      // imposta directory storage locale
      appDir = OsIdent.getAppDirectory("CaleidoSyncClient");

      LongOptExt longopts[] = new LongOptExt[]
      {
        new LongOptExt("help", LongOpt.NO_ARGUMENT, null, 'h', "visualizza questo messaggio ed esce"),
        new LongOptExt("server", LongOpt.REQUIRED_ARGUMENT, null, 's', "specifica il nome del server Caleido"),
        new LongOptExt("port-xmlrpc", LongOpt.REQUIRED_ARGUMENT, null, 'x', "specifica porta XML-RPC per Caleido"),
        new LongOptExt("url-xmlrpc", LongOpt.REQUIRED_ARGUMENT, null, 'U', "specifica url comleta XML-RPC per Caleido"),
        new LongOptExt("debug", LongOpt.NO_ARGUMENT, null, 'd', "abilita funzioni di debugging"),
        new LongOptExt("dry-run", LongOpt.NO_ARGUMENT, null, 'e', "simulazione: nessuna scrittura su db"),
        new LongOptExt("rule", LongOpt.REQUIRED_ARGUMENT, null, 'r', "regola di sincronizzazione da attivare"),
        new LongOptExt("filter", LongOpt.REQUIRED_ARGUMENT, null, 'f', "filtro chiave=valore; può essere ripetuto più volte"),
      };

      loadAllProperties();

      String optString = LongOptExt.getOptstring(longopts);
      Getopt g = new Getopt("CaleidoSyncClient", args, optString, longopts);
      g.setOpterr(false); // We'll do our own error handling

      int c;
      while((c = g.getopt()) != -1)
      {
        switch(c)
        {
          case 'h':
            help(longopts);
            return;

          case 'd':
            debug = true;
            break;

          case 'e':
            simulazione = true;
            break;

          case 'r':
            syncRule = g.getOptarg();
            break;

          case 's':
            serverXmlRpc = g.getOptarg();
            break;

          case 'x':
            if((portXmlRpc = SU.parse(g.getOptarg(), 0)) <= 0 || portXmlRpc > 65535)
              throw new Exception("Errore nel valore della porta specificata.");
            break;

          case 'U':
            urlXmlRpc = g.getOptarg();
            break;

          case 'f':
            String[] parts = g.getOptarg().split("=");
            if(parts.length >= 2)
              extraFilter.put(parts[0], parts[1]);
            break;
        }
      }

      if(!debug)
      {
        // invio log su file se NON siamo in debug (in questo caso a console)
        String logFile = appDir.getAbsolutePath() + File.separator + "calsync.log";
        log.info("Ulteriori messaggi di log inviati nel file " + logFile);

        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(new FileAppender(
           new PatternLayout("%d [%t] %-5p %c{1} - %m%n"),
           logFile, false));
      }

      // invia la command line alla log per eventuali tracce d'errore
      for(int i = 0; i < args.length; i++)
      {
        String arg = args[i];
        log.info(String.format("Argomento %d = %s", i, arg));
      }

      int ii = g.getOptind();
      int ni = args.length - ii;
      Database.DatabaseInfo di = new Database.DatabaseInfo();
      di.driver = "net.sourceforge.jtds.jdbc.Driver";

      switch(ni)
      {
        case 1:
          // il solo parametro è il file di setup xml
          Database.readFromXML(new File(args[ii]));
          break;

        case 4:
          di.driver = SU.okStrNull(args[ii++]);
        case 3:
          di.jdbcUrl = SU.okStrNull(args[ii++]);
          di.user = SU.okStrNull(args[ii++]);
          di.password = SU.okStrNull(args[ii++]);
          Database.addDatabaseInfo("diamante", di);
          break;
      }
    }
    catch(Exception ex)
    {
      log.error("Errore fatale inizializzazione setup.", ex);
      System.exit(-1);
    }
  }

  public static void loadAllProperties()
     throws ConfigurationException, IOException
  {
    try (InputStream strProperties = SetupData.class.getResourceAsStream(
       "/it/infomed/sync/resources/CaleidoSyncClient.properties"))
    {
      if(strProperties != null)
        versionProperties.load(strProperties);
    }

    URL setupUrl = SetupData.class.getResource(
       "/it/infomed/sync/resources/setup.properties");
    cfg = (Configuration) new PropertiesConfiguration(setupUrl);

    try
    {
      File fprop = new File(SetupData.appDir, "private.properties");
      pm.load(fprop);
    }
    catch(FileNotFoundException e)
    {
      log.info("Prorieta locali non presenti; proseguo con i default.");
    }
    catch(Exception e)
    {
      log.error("Errore non fatale leggendo private.properties: " + e.getMessage());
    }
  }

  public static void shutdown()
  {
    try
    {
      File fprop = new File(SetupData.appDir, "private.properties");
      pm.save(fprop);
    }
    catch(Exception e)
    {
      log.error("Errore non fatale scrivendo private.properties", e);
    }
  }

  public static void help(LongOptExt longopts[])
  {
    String appVersion = versionProperties.getProperty("Application.version");

    System.out.printf(
       "CaleidoSyncClient - ver. %s\n"
       + "Applicativo per la sincronizzazione base dati fra Caleido e altri applicativi.\n"
       + "modo d'uso:\n"
       + "  calsync [-h] [fileconf.xml][[jdbc driver] jdbcUrl dbuser dbpassword]\n", appVersion);

    for(LongOptExt l : longopts)
    {
      System.out.println(l.getHelpMsg());
    }

    System.out.println();
    System.exit(0);
  }
}
