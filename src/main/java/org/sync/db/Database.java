/*
 *  Database.java
 *  Creato il Nov 27, 2017, 3:06:41 PM
 *
 *  Copyright (C) 2017 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package org.sync.db;

import com.workingdogs.village.QueryDataSet;
import com.workingdogs.village.Schema;
import com.workingdogs.village.TableDataSet;
import com.workingdogs.village.VillageUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

/**
 * Interfaccia verso il database SQL.
 *
 * @author Nicola De Nisco
 */
public class Database
{
  private static String defaultDB = null;
  private static Map<String, DatabaseInfo> mapInfo = new HashMap<>();
  /** the log */
  protected static final Log log = LogFactory.getLog(Database.class);
  //
  public static final String[] TABLES_FILTER = new String[]
  {
    "TABLE"
  };
  public static final String[] VIEWS_FILTER = new String[]
  {
    "VIEW"
  };

  public static String getDefaultDB()
  {
    return defaultDB;
  }

  public static String[] getDBNames()
  {
    Object[] keysAr = mapInfo.keySet().toArray();
    return Arrays.copyOf(keysAr, keysAr.length, String[].class);
  }

  public static void setDefaultDB(String defaultDB)
  {
    Database.defaultDB = defaultDB;
  }

  public static Connection getConnection()
     throws DatabaseException
  {
    return getConnection(defaultDB);
  }

  public static Connection getConnection(String dbName)
     throws DatabaseException
  {
    DatabaseInfo info = mapInfo.get(dbName);
    if(info == null)
      throw new DatabaseException(String.format("Database %s non registrato: rivedere setup.", dbName));

    try
    {
      return openDatabase(info);
    }
    catch(Exception e)
    {
      throw new DatabaseException(e);
    }
  }

  public static void closeConnection(Connection con)
  {
    if(con == null)
      return;

    try
    {
      con.close();
    }
    catch(SQLException ex)
    {
      log.error("", ex);
    }
  }

  /**
   * Aggiunge informazioni per un database.
   * @param dbName nome di riferimento (come richiesto da operazioni)
   * @param driver
   * @param jdbcUrl
   * @param user
   * @param password
   * @throws ClassNotFoundException
   */
  public static void addDatabaseInfo(String dbName, String driver, String jdbcUrl, String user, String password)
     throws ClassNotFoundException
  {
    DatabaseInfo info = new DatabaseInfo();
    info.driver = driver;
    info.jdbcUrl = jdbcUrl;
    info.user = user;
    info.password = password;
    addDatabaseInfo(dbName, info);
  }

  /**
   * Aggiunge informazioni per un database.
   * @param dbName nome di riferimento (come richiesto da operazioni)
   * @param info
   * @throws ClassNotFoundException
   */
  public static void addDatabaseInfo(String dbName, DatabaseInfo info)
     throws ClassNotFoundException
  {
    Class.forName(info.driver);
    mapInfo.put(dbName, info);
  }

  /**
   * Aggiunge informazioni per un database.
   * Versione specifica per Diamante.
   * @param dbName nome di riferimento (come richiesto da operazioni)
   * @param diaDatabase
   * @param diaIstanza
   * @param diaIndirizzo
   * @param diaPorta
   * @param user
   * @param password
   * @throws ClassNotFoundException
   */
  public static void addDatabaseInfoDiamante(String dbName,
     String diaDatabase, String diaIstanza, String diaIndirizzo, int diaPorta,
     String user, String password)
     throws ClassNotFoundException
  {
    DatabaseInfo info = new DatabaseInfo();
    info.driver = "net.sourceforge.jtds.jdbc.Driver";

    if(diaIstanza.isEmpty())
      info.jdbcUrl = "jdbc:jtds:sqlserver://" + diaIndirizzo + ":" + diaPorta + "/" + diaDatabase;
    else
      info.jdbcUrl
         = "jdbc:jtds:sqlserver://" + diaIndirizzo + ":" + diaPorta + "/" + diaDatabase + ";instance=" + diaIstanza;

    info.user = user;
    info.password = password;
    addDatabaseInfo(dbName, info);
  }

  /**
   * Legge settaggio database da file XML.
   * Se nessun database è marcato come database di default
   * imposta il default al primo dell'elenco.
   * @param fxml file XML con il setup da leggere
   * @throws Exception
   */
  public static void readFromXML(File fxml)
     throws Exception
  {
    log.info("Leggo settaggio db dal file " + fxml.getAbsolutePath());
    SAXBuilder builder = new SAXBuilder();
    Document doc = builder.build(fxml);
    defaultDB = null;

    List<Element> lsdb = doc.getRootElement().getChildren("database");
    for(Element e : lsdb)
    {
      String dbName = e.getAttributeValue("name");
      boolean defconn = StringOper.checkTrueFalse(e.getAttributeValue("default"), false);

      DatabaseInfo info = new DatabaseInfo();
      info.driver = e.getChildTextTrim("driver");
      info.jdbcUrl = e.getChildTextTrim("jdbc-url");
      info.user = e.getChildTextTrim("user");
      info.password = e.getChildTextTrim("password");

      if(info.driver.isEmpty() || info.jdbcUrl.isEmpty())
        throw new Exception("Manca driver o url nella dichiarazione di database.");

      addDatabaseInfo(dbName, info);

      if(defconn || defaultDB == null)
        defaultDB = dbName;
    }
  }

  private static Connection openDatabase(DatabaseInfo info)
     throws Exception
  {
    return DriverManager.getConnection(info.jdbcUrl, info.user, info.password);
  }

  public static class DatabaseInfo
  {
    public String driver, jdbcUrl, user, password;
  }

  /**
   * Lista delle viste di un database.
   * @param con connessione al db
   * @return lista di tutte le viste presenti (schema di default)
   * @throws Exception
   */
  public static List<String> getAllViews(Connection con)
     throws Exception
  {
    DatabaseMetaData databaseMetaData = con.getMetaData();
    ArrayList<String> viewNames = new ArrayList<String>();
    try (ResultSet rSet = databaseMetaData.getTables(null, null, null, VIEWS_FILTER))
    {
      while(rSet.next())
      {
        if(rSet.getString("TABLE_TYPE").equals("VIEW"))
        {
          String tableName = rSet.getString("TABLE_NAME");
          viewNames.add(tableName);
        }
      }
    }

    return viewNames;
  }

  /**
   * Lista delle tabelle di un database.
   * @param con connessione al db
   * @return lista di tutte le tabelle presenti (schema di default)
   * @throws Exception
   */
  public static List<String> getAllTables(Connection con)
     throws Exception
  {
    DatabaseMetaData databaseMetaData = con.getMetaData();
    ArrayList<String> tableNames = new ArrayList<String>();
    try (ResultSet rSet = databaseMetaData.getTables(null, null, null, TABLES_FILTER))
    {
      while(rSet.next())
      {
        if(rSet.getString("TABLE_TYPE").equals("TABLE"))
        {
          String schema = rSet.getString("TABLE_SCHEM");
          String tableName = rSet.getString("TABLE_NAME");
          if(!isSchemaPublic(schema))
            tableNames.add(schema + "." + tableName);
          else
            tableNames.add(tableName);
        }
      }
    }

    return tableNames;
  }

  /**
   * Ritorna il tipo per una colonna di tabella.
   * La ricerca del nome tabella e nome colonna è case insensitive.
   * @param con connessione al db
   * @param nomeTabella nome della tabella (eventualmente con schema)
   * @param nomeColonna nome della colonna
   * @return int {@code =>} SQL type from java.sql.Types 0=non trovato
   * @throws Exception
   */
  public static int getTipoColonna(Connection con, String nomeTabella, String nomeColonna)
     throws Exception
  {
    Integer rv = scanTabelleColonne(con, nomeTabella, nomeColonna, Database::getTipoColonna);
    return rv == null ? 0 : rv;
  }

  /**
   * Ritorna i tipi delle colonne di una tabella.
   * La ricerca del nome tabella è case insensitive.
   * @param con connessione al db
   * @param nomeTabella nome della tabella (eventualmente con schema)
   * @return int {@code =>} SQL type from java.sql.Types 0=non trovato
   * @throws Exception
   */
  public static ArrayMap<String, Integer> getTipiColonne(Connection con, String nomeTabella)
     throws Exception
  {
    return scanTabelleColonne(con, nomeTabella, null, Database::getTipiColonne);
  }

  /**
   * Ritorna le colonne della chiave primaria di una tabella.
   * La ricerca del nome tabella è case insensitive.
   * @param con connessione al db
   * @param nomeTabella nome della tabella (eventualmente con schema)
   * @return int {@code =>} SQL type from java.sql.Types 0=non trovato
   * @throws Exception
   */
  public static ArrayMap<String, Integer> getPrimaryKeys(Connection con, String nomeTabella)
     throws Exception
  {
    return scanTabelleColonne(con, nomeTabella, null, Database::getPrimaryKeys);
  }

  /**
   * Verifica esistenza tabella nel db.
   * La ricerca del nome tabella è case insensitive.
   * @param con connessione al db
   * @param nomeTabella nome della tabella
   * @return vero se esiste
   * @throws Exception
   */
  public static boolean existTable(Connection con, String nomeTabella)
     throws Exception
  {
    Boolean rv = scanTabelleColonne(con, nomeTabella, null,
       (dbCon, nomeSchema1, nomeTabella1, nomeColonna1) -> Boolean.TRUE);
    return rv == null ? false : rv;
  }

  @FunctionalInterface
  public interface ScanColumn<T>
  {
    public T scan(Connection con, String nomeSchema, String nomeTabella, String nomeColonna)
       throws Exception;
  }

  /**
   * Funzione generica di scansione colonne.
   * La ricerca del nome tabella è case insensitive.
   * @param <T> il tipo tornato da sfun
   * @param con connessione al db
   * @param nomeTabella nome della tabella (eventualmente con schema)
   * @param nomeColonna nome della colonna
   * @param sfun funzione lambda per la scansione dei campi della tabella individuata
   * @return int {@code =>} SQL type from java.sql.Types 0=non trovato
   * @throws Exception
   */
  public static <T> T scanTabelleColonne(Connection con, String nomeTabella, String nomeColonna, ScanColumn<T> sfun)
     throws Exception
  {
    String nomeSchema = null;
    int pos = nomeTabella.indexOf('.');
    if(pos != -1)
    {
      nomeSchema = nomeTabella.substring(0, pos);
      nomeTabella = nomeTabella.substring(pos + 1);
    }

    try (ResultSet rSet = con.getMetaData().getTables(null, null, null, TABLES_FILTER))
    {
      while(rSet.next())
      {
        if(rSet.getString("TABLE_TYPE").equals("TABLE"))
        {
          String schema = rSet.getString("TABLE_SCHEM");
          String tableName = rSet.getString("TABLE_NAME");

          if(nomeSchema == null && (schema == null || isSchemaPublic(schema)))
          {
            if(StringOper.isEquNocase(nomeTabella, tableName))
              return sfun.scan(con, schema, tableName, nomeColonna);
          }
          else
          {
            if(StringOper.isEquNocase(nomeSchema, schema)
               && StringOper.isEquNocase(nomeTabella, tableName))
              return sfun.scan(con, schema, tableName, nomeColonna);
          }
        }
      }
    }

    return null;
  }

  /**
   * Ritorna vero se lo schema è lo schema di default.
   * In Postgres o Mysql si chiama public, in MSSql si chiama dbo, ecc.
   * @param nomeSchema nome da testare
   * @return vero se è lo schema di default del db
   */
  public static boolean isSchemaPublic(String nomeSchema)
  {
    return StringOper.isEquNocaseAny(nomeSchema, "public", "dbo");
  }

  public static Integer getTipoColonna(Connection con,
     String nomeSchema, String nomeTabella, String nomeColonna)
     throws SQLException
  {
    try (ResultSet rs = con.getMetaData().getColumns(con.getCatalog(), nomeSchema, nomeTabella, null))
    {
      while(rs.next())
      {
        String cn = rs.getString("COLUMN_NAME");
        if(StringOper.isEquNocase(cn, nomeColonna))
          return rs.getInt("DATA_TYPE");
      }
    }
    return 0;
  }

  public static ArrayMap<String, Integer> getTipiColonne(Connection con,
     String nomeSchema, String nomeTabella, String nomeColonna)
     throws SQLException
  {
    ArrayMap<String, Integer> rv = new ArrayMap<>();

    try (ResultSet rs = con.getMetaData().getColumns(con.getCatalog(), nomeSchema, nomeTabella, null))
    {
      while(rs.next())
      {
        String cn = rs.getString("COLUMN_NAME");
        int tipo = rs.getInt("DATA_TYPE");
        rv.add(new Pair<>(cn, tipo));
      }
    }

    return rv;
  }

  public static ArrayMap<String, Integer> getPrimaryKeys(Connection con,
     String nomeSchema, String nomeTabella, String nomeColonna)
     throws SQLException
  {
    ArrayMap<String, Integer> rv = new ArrayMap<>();

    try (ResultSet rs = con.getMetaData().getPrimaryKeys(con.getCatalog(), nomeSchema, nomeTabella))
    {
      while(rs.next())
      {
        String cn = rs.getString("COLUMN_NAME");
        int seq = rs.getInt("KEY_SEQ");
        rv.add(new Pair<>(cn, seq));
      }
    }

    return rv;
  }

  /**
   * Verifica una colonna per tipo numerico.
   * @param con connessione al db
   * @param nomeTabella nome della tabella (eventualmente con schema)
   * @param nomeColonna nome della colonna
   * @return vero se la colonna è numerica
   * @throws Exception
   */
  public static boolean isNumeric(Connection con, String nomeTabella, String nomeColonna)
     throws Exception
  {
    int tipo = getTipoColonna(con, nomeTabella, nomeColonna);
    return isNumeric(tipo);
  }

  /**
   * Verifica per tipo numerico.
   * @param sqlType tipo sql (Types)
   * @return vero se la colonna è numerica
   */
  public static boolean isNumeric(int sqlType)
  {
    switch(sqlType)
    {
      case Types.BIT:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return true;
    }

    return false;
  }

  public static boolean isString(int sqlType)
  {
    switch(sqlType)
    {
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
      case Types.CLOB:
        return true;
    }
    return false;
  }

  public static boolean isDate(int sqlType)
  {
    switch(sqlType)
    {
      case Types.DATE:
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return true;
    }
    return false;
  }

  public static Schema schemaQuery(Connection con, String sSQL)
     throws Exception
  {
    QueryDataSet qds = null;
    try
    {
      qds = new QueryDataSet(con, sSQL);
      return qds.schema();
    }
    finally
    {
      VillageUtils.close(qds);
    }
  }

  public static Schema schemaTable(Connection con, String nomeTabella)
     throws Exception
  {
    TableDataSet tds = null;
    try
    {
      tds = new TableDataSet(con, nomeTabella);
      return tds.schema();
    }
    finally
    {
      VillageUtils.close(tds);
    }
  }

  public static Schema schemaTable(String nomeDatabase, String nomeTabella)
     throws Exception
  {
    try (Connection con = getConnection(nomeDatabase))
    {
      return schemaTable(con, nomeTabella);
    }
  }

  public static Schema schemaTable(String nomeTabella)
     throws Exception
  {
    try (Connection con = getConnection())
    {
      return schemaTable(con, nomeTabella);
    }
  }
}
