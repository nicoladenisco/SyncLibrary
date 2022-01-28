/*
 *  DbPeer.java
 *  Creato il Nov 26, 2017, 6:51:13 PM
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
package it.infomed.sync.db;

import com.workingdogs.village.DataSet;
import com.workingdogs.village.DataSetException;
import com.workingdogs.village.QueryDataSet;
import com.workingdogs.village.Record;
import it.infomed.sync.SetupData;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.StringOper;

/**
 * Interfaccia con il database.
 *
 * @author Nicola De Nisco
 */
public class DbPeer
{
  /** Constant criteria key to reference ORDER BY columns. */
  public static final String ORDER_BY = "ORDER BY";

  /**
   * Constant criteria key to remove Case Information from
   * search/ordering criteria.
   */
  public static final String IGNORE_CASE = "IgNOrE cAsE";

  /** Classes that implement this class should override this value. */
  public static final String TABLE_NAME = "TABLE_NAME";

  /** the log */
  protected static final Log log = LogFactory.getLog(DbPeer.class);

  public static void throwTorqueException(Exception e)
     throws DatabaseException
  {
    if(e instanceof DatabaseException)
    {
      throw (DatabaseException) e;
    }
    else
    {
      throw new DatabaseException(e);
    }
  }

  /**
   * Convenience method that uses straight JDBC to delete multiple
   * rows. Village throws an Exception when multiple rows are
   * deleted.
   *
   * @param con A Connection.
   * @param table The table to delete records from.
   * @param column The column in the where clause.
   * @param value The value of the column.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static void deleteAll(
     Connection con,
     String table,
     String column,
     int value)
     throws DatabaseException
  {
    StringBuilder query = new StringBuilder();
    query.append("DELETE FROM ")
       .append(table)
       .append(" WHERE ")
       .append(column)
       .append(" = ")
       .append(value);

    executeStatement(query.toString(), con);
  }

  /**
   * Convenience method that uses straight JDBC to delete multiple
   * rows. Village throws an Exception when multiple rows are
   * deleted. This method attempts to get the default database from
   * the pool.
   *
   * @param table The table to delete records from.
   * @param column The column in the where clause.
   * @param value The value of the column.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static void deleteAll(String table, String column, int value)
     throws DatabaseException
  {
    Connection con = null;
    try
    {
      // Get a connection to the db.
      con = Database.getConnection(Database.getDefaultDB());
      deleteAll(con, table, column, value);
    }
    finally
    {
      Database.closeConnection(con);
    }
  }

  /**
   * Utility method which executes a given sql statement. This
   * method should be used for select statements only. Use
   * executeStatement for update, insert, and delete operations.
   *
   * @param queryString A String with the sql statement to execute.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(String queryString)
     throws DatabaseException
  {
    return executeQuery(queryString, Database.getDefaultDB(), false);
  }

  /**
   * Utility method which executes a given sql statement. This
   * method should be used for select statements only. Use
   * executeStatement for update, insert, and delete operations.
   *
   * @param queryString A String with the sql statement to execute.
   * @param dbName The database to connect to.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(String queryString, String dbName)
     throws DatabaseException
  {
    return executeQuery(queryString, dbName, false);
  }

  /**
   * Method for performing a SELECT. Returns all results.
   *
   * @param queryString A String with the sql statement to execute.
   * @param dbName The database to connect to.
   * @param singleRecord Whether or not we want to select only a
   * single record.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(
     String queryString,
     String dbName,
     boolean singleRecord)
     throws DatabaseException
  {
    return executeQuery(queryString, 0, -1, dbName, singleRecord);
  }

  /**
   * Method for performing a SELECT. Returns all results.
   *
   * @param queryString A String with the sql statement to execute.
   * @param dbName The database to connect to.
   * @param singleRecord Whether or not we want to select only a
   * @param con A Connection.
   * single record.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(
     String queryString,
     String dbName,
     boolean singleRecord,
     Connection con)
     throws DatabaseException
  {
    return executeQuery(queryString, 0, -1, singleRecord, con);
  }

  /**
   * Method for performing a SELECT. Returns all results.
   *
   * @param queryString A String with the sql statement to execute.
   * @param singleRecord Whether or not we want to select only a
   * single record.
   * @param con A Connection.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(
     String queryString,
     boolean singleRecord,
     Connection con)
     throws DatabaseException
  {
    return executeQuery(queryString, 0, -1, singleRecord, con);
  }

  /**
   * Method for performing a SELECT.
   *
   * @param queryString A String with the sql statement to execute.
   * @param start The first row to return.
   * @param numberOfResults The number of rows to return.
   * @param dbName The database to connect to.
   * @param singleRecord Whether or not we want to select only a
   * single record.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(
     String queryString,
     int start,
     int numberOfResults,
     String dbName,
     boolean singleRecord)
     throws DatabaseException
  {
    Connection con = null;
    List results = null;
    try
    {
      con = Database.getConnection(dbName);
      // execute the query
      results = executeQuery(
         queryString,
         start,
         numberOfResults,
         singleRecord,
         con);
    }
    finally
    {
      Database.closeConnection(con);
    }
    return results;
  }

  /**
   * Method for performing a SELECT. Returns all results.
   *
   * @param queryString A String with the sql statement to execute.
   * @param start The first row to return.
   * @param numberOfResults The number of rows to return.
   * @param singleRecord Whether or not we want to select only a
   * single record.
   * @param con A Connection.
   * @return List of Record objects.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List executeQuery(
     String queryString,
     int start,
     int numberOfResults,
     boolean singleRecord,
     Connection con)
     throws DatabaseException
  {
    QueryDataSet qds = null;
    List results = Collections.EMPTY_LIST;
    try
    {
      // execute the query
//      long startTime = System.currentTimeMillis();
      qds = new QueryDataSet(con, queryString);

//      if(log.isDebugEnabled())
//      {
//        log.debug("Elapsed time="
//           + (System.currentTimeMillis() - startTime) + " ms");
//      }
      results = getSelectResults(
         qds, start, numberOfResults, singleRecord);
    }
    catch(DataSetException | SQLException e)
    {
      log.error(queryString);
      throwTorqueException(e);
    }
    finally
    {
      VillageUtils.close(qds);
    }
    return results;
  }

  /**
   * Returns all records in a QueryDataSet as a List of Record
   * objects. Used for functionality like util.LargeSelect.
   *
   * @see #getSelectResults(QueryDataSet, int, int, boolean)
   * @param qds the QueryDataSet
   * @return a List of Record objects
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List getSelectResults(QueryDataSet qds)
     throws DatabaseException
  {
    return getSelectResults(qds, 0, -1, false);
  }

  /**
   * Returns all records in a QueryDataSet as a List of Record
   * objects. Used for functionality like util.LargeSelect.
   *
   * @see #getSelectResults(QueryDataSet, int, int, boolean)
   * @param qds the QueryDataSet
   * @param singleRecord
   * @return a List of Record objects
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List getSelectResults(QueryDataSet qds, boolean singleRecord)
     throws DatabaseException
  {
    return getSelectResults(qds, 0, -1, singleRecord);
  }

  /**
   * Returns numberOfResults records in a QueryDataSet as a List
   * of Record objects. Starting at record 0. Used for
   * functionality like util.LargeSelect.
   *
   * @see #getSelectResults(QueryDataSet, int, int, boolean)
   * @param qds the QueryDataSet
   * @param numberOfResults
   * @param singleRecord
   * @return a List of Record objects
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static List getSelectResults(
     QueryDataSet qds,
     int numberOfResults,
     boolean singleRecord)
     throws DatabaseException
  {
    List results = null;
    if(numberOfResults != 0)
    {
      results = getSelectResults(qds, 0, numberOfResults, singleRecord);
    }
    return results;
  }

  /**
   * Returns numberOfResults records in a QueryDataSet as a List
   * of Record objects. Starting at record start. Used for
   * functionality like util.LargeSelect.
   *
   * @param qds The <code>QueryDataSet</code> to extract results
   * from.
   * @param start The index from which to start retrieving
   * <code>Record</code> objects from the data set.
   * @param numberOfResults The number of results to return (or
   * <code> -1</code> for all results).
   * @param singleRecord Whether or not we want to select only a
   * single record.
   * @return A <code>List</code> of <code>Record</code> objects.
   * @throws DatabaseException If any <code>Exception</code> occurs.
   */
  public static List getSelectResults(
     QueryDataSet qds,
     int start,
     int numberOfResults,
     boolean singleRecord)
     throws DatabaseException
  {
    List results = null;
    try
    {
      if(numberOfResults < 0)
      {
        results = new ArrayList();
        qds.fetchRecords();
      }
      else
      {
        results = new ArrayList(numberOfResults);
        qds.fetchRecords(start, numberOfResults);
      }

      int startRecord = 0;

      //Offset the correct number of records
      if(start > 0 && numberOfResults <= 0)
      {
        startRecord = start;
      }

      // Return a List of Record objects.
      for(int i = startRecord; i < qds.size(); i++)
      {
        Record rec = qds.getRecord(i);
        results.add(rec);
      }

      if(results.size() > 1 && singleRecord)
      {
        handleMultipleRecords(qds);
      }
    }
    catch(Exception e)
    {
      throwTorqueException(e);
    }
    return results;
  }

  /**
   * Utility method which executes a given sql statement. This
   * method should be used for update, insert, and delete
   * statements. Use executeQuery() for selects.
   *
   * @param statementString A String with the sql statement to execute.
   * @return The number of rows affected.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static int executeStatement(String statementString)
     throws DatabaseException
  {
    return executeStatement(statementString, Database.getDefaultDB());
  }

  /**
   * Utility method which executes a given sql statement. This
   * method should be used for update, insert, and delete
   * statements. Use executeQuery() for selects.
   *
   * @param statementString A String with the sql statement to execute.
   * @param dbName Name of database to connect to.
   * @return The number of rows affected.
   * @throws DatabaseException Any exceptions caught during processing will be
   * rethrown wrapped into a DatabaseException.
   */
  public static int executeStatement(String statementString, String dbName)
     throws DatabaseException
  {
    if(SetupData.simulazione)
    {
      log.info(statementString);
      return 0;
    }

    Connection con = null;
    int rowCount = -1;
    try
    {
      con = Database.getConnection(dbName);
      rowCount = executeStatement(statementString, con);
    }
    finally
    {
      Database.closeConnection(con);
    }
    return rowCount;
  }

  /**
   * Utility method which executes a given sql statement. This
   * method should be used for update, insert, and delete
   * statements. Use executeQuery() for selects.
   *
   * @param statementString A String with the sql statement to execute.
   * @param con A Connection.
   * @return The number of rows affected.
   * @throws DatabaseException Any exceptions caught during processing will be rethrown wrapped into a
   * DatabaseException.
   */
  public static int executeStatement(String statementString, Connection con)
     throws DatabaseException
  {
    if(SetupData.simulazione)
    {
      log.info(statementString);
      return 0;
    }

    try (Statement statement = con.createStatement())
    {
      if(SetupData.debug)
        log.debug(statementString);

      return statement.executeUpdate(statementString);
    }
    catch(SQLException e)
    {
      throw new DatabaseException(e);
    }
  }

  public static int createTimestampTable(String statementString)
     throws DatabaseException
  {
    return createTimestampTable(statementString, Database.getDefaultDB());
  }

  public static int createTimestampTable(String statementString, String dbName)
     throws DatabaseException
  {
    Connection con = null;
    int rowCount = -1;

    try
    {
      dbName = StringOper.isOkStr(dbName) ? dbName : Database.getDefaultDB();
      con = Database.getConnection(dbName);
      try (Statement statement = con.createStatement())
      {
        rowCount = statement.executeUpdate(statementString);
      }
      catch(SQLException e)
      {
        throw new DatabaseException(e);
      }
    }
    finally
    {
      Database.closeConnection(con);
    }

    return rowCount;
  }

  /**
   * If the user specified that (s)he only wants to retrieve a
   * single record and multiple records are retrieved, this method
   * is called to handle the situation. The default behavior is to
   * throw an exception, but subclasses can override this method as
   * needed.
   *
   * @param ds The DataSet which contains multiple records.
   * @throws DatabaseException Couldn't handle multiple records.
   */
  protected static void handleMultipleRecords(DataSet ds)
     throws DatabaseException
  {
    throw new DatabaseException("Criteria expected single Record and "
       + "Multiple Records were selected");
  }

  public static ArrayMap<String, Integer> getTipiColonne(String nomeTabella, String dbName)
     throws Exception
  {
    Connection con = null;
    ArrayMap<String, Integer> rv = null;
    try
    {
      con = Database.getConnection(dbName);
      rv = Database.getTipiColonne(con, nomeTabella);
    }
    finally
    {
      Database.closeConnection(con);
    }
    return rv;
  }

  public static ArrayMap<String, Integer> getTipiColonne(String nomeTabella)
     throws Exception
  {
    return getTipiColonne(nomeTabella, Database.getDefaultDB());
  }

  /**
   * Verifica esistenza tabella nel db.
   * La ricerca del nome tabella è case insensitive.
   * @param nomeTabella nome della tabella
   * @param dbName nome del database
   * @return vero se esiste
   * @throws Exception
   */
  public static boolean existTable(String nomeTabella, String dbName)
     throws Exception
  {
    Connection con = null;
    boolean rv = false;
    try
    {
      con = Database.getConnection(dbName);
      rv = Database.existTable(con, nomeTabella);
    }
    finally
    {
      Database.closeConnection(con);
    }
    return rv;
  }

  /**
   * Verifica esistenza tabella nel db.
   * La ricerca del nome tabella è case insensitive.
   * @param nomeTabella nome della tabella
   * @return vero se esiste
   * @throws Exception
   */
  public static boolean existTable(String nomeTabella)
     throws Exception
  {
    return existTable(nomeTabella, Database.getDefaultDB());
  }
}
