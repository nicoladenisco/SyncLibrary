package org.sync.db;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Gestore di transazioni.
 *
 * @author Nicola De Nisco
 */
abstract public class SqlTransactAgent
{
  private static Log log = LogFactory.getLog(SqlTransactAgent.class);

  public boolean executed = false;
  public HashMap<String, Object> tcontext = new HashMap<>();
  public Object[] tparams = null;
  public String dbName = null;
  public static HashSet<Long> thids = new HashSet<>();
  private boolean useTransaction = true;

  public interface executor
  {
    public void execute(Connection con)
       throws Exception;
  }

  public interface executorContext
  {
    public void execute(Connection con, Map<String, Object> context)
       throws Exception;
  }

  public interface executorNoconn
  {
    public void execute()
       throws Exception;
  }

  public interface executorContextNoconn
  {
    public void execute(Map<String, Object> context)
       throws Exception;
  }

  public interface executorReturn<T>
  {
    public T execute(Connection con)
       throws Exception;
  }

  public interface executorReturnNoconn<T>
  {
    public T execute()
       throws Exception;
  }

  public SqlTransactAgent()
  {
  }

  /**
   * Costruttore con possibiltà di esecuzione immediata.
   * @param execute se vero esegue immediatamente la transazione (chiama runNow())
   * @throws Exception
   */
  public SqlTransactAgent(boolean execute)
     throws Exception
  {
    if(execute)
      runNow();
  }

  /**
   * Costruttore con possibiltà di esecuzione immediata.
   * @param execute se vero esegue immediatamente la transazione (chiama runNow())
   * @param context mappa di chiave/volere per usi nella funzione ridefinita
   * @throws Exception
   */
  public SqlTransactAgent(boolean execute, Map<String, Object> context)
     throws Exception
  {
    this.tcontext.putAll(context);
    if(execute)
      runNow();
  }

  /**
   * Costruttore con possibiltà di esecuzione immediata.
   * @param execute se vero esegue immediatamente la transazione (chiama runNow())
   * @param params parametri; saranno disponibile all'interno della funzione ridefinita nell'array tparams
   * @throws Exception
   */
  public SqlTransactAgent(boolean execute, Object... params)
     throws Exception
  {
    this.tparams = params;
    if(execute)
      runNow();
  }

  public SqlTransactAgent(boolean execute, String dbName, Object... params)
     throws Exception
  {
    this.tparams = params;
    this.dbName = dbName;
    if(execute)
      runNow(dbName);
  }

  public void runNow()
     throws Exception
  {
    Connection dbCon = null;

    try
    {
      dbCon = Database.getConnection(dbName);

      if(useTransaction)
        runTransaction(dbCon);
      else
        runSimple(dbCon);
    }
    finally
    {
      Database.closeConnection(dbCon);
    }
  }

  public void runNow(String dbName)
     throws Exception
  {
    Connection dbCon = null;

    try
    {
      dbCon = Database.getConnection(dbName);

      if(useTransaction)
        runTransaction(dbCon);
      else
        runSimple(dbCon);
    }
    finally
    {
      Database.closeConnection(dbCon);
    }
  }

  protected void runTransaction(Connection dbCon)
     throws Exception
  {
    boolean prevAutoCommitState = true;
    long thid = Thread.currentThread().getId();
    if(thids.contains(thid))
      throw new Exception("Thread già in transazione: " + Thread.currentThread());

    try
    {
      prevAutoCommitState = dbCon.getAutoCommit();
      dbCon.setAutoCommit(false);

      // esegue comandi all'interno della transazione
      thids.add(thid);
      executed = run(dbCon, true);
      thids.remove(thid);

      if(executed)
        dbCon.commit();
      else
        dbCon.rollback();
    }
    catch(Exception ex)
    {
      thids.remove(thid);
      executed = false;

      try
      {
        if(dbCon != null)
          dbCon.rollback();
      }
      catch(SQLException sQLException)
      {
        // questa eccezione viene ignorata
        // essendo piu' importante riportare
        // quella che ha abortito la transazione
        log.error("Fatal in rollback transaction.", sQLException);
      }

      throw ex;
    }
    finally
    {
      if(dbCon != null)
        dbCon.setAutoCommit(prevAutoCommitState);
    }
  }

  protected void runSimple(Connection dbCon)
     throws Exception
  {
    // esegue comandi senza transazione
    executed = run(dbCon, false);
  }

  /**
   * Funzione da ridefinire in classi derivate:
   * eseguire le operazioni richieste; se viene
   * sollevata una eccezione viene effettuata una rollback
   * e quindi risollevata l'eccezione.
   * Se ritorna true viene effettuato un commit
   * @param dbCon
   * @param transactionSupported
   * @return
   * @throws java.lang.Exception
   */
  public abstract boolean run(Connection dbCon, boolean transactionSupported)
     throws Exception;

  public static Map<String, Object> execute(executor exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(true, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        ((executor) tparams[0]).execute(dbCon);
        return true;
      }
    };

    return ta.tcontext;
  }

  public static Map<String, Object> execute(String dbName, executor exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(true, dbName, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        ((executor) tparams[0]).execute(dbCon);
        return true;
      }
    };

    return ta.tcontext;
  }

  public static Map<String, Object> execute(executorContext exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(true, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        ((executorContext) tparams[0]).execute(dbCon, tcontext);
        return true;
      }
    };

    return ta.tcontext;
  }

  public static Map<String, Object> execute(Connection con, executorNoconn exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(false, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        ((executorNoconn) tparams[0]).execute();
        return true;
      }
    };

    ta.runTransaction(con);
    return ta.tcontext;
  }

  public static Map<String, Object> execute(Connection con, executorContextNoconn exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(false, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        ((executorContextNoconn) tparams[0]).execute(tcontext);
        return true;
      }
    };

    ta.runTransaction(con);
    return ta.tcontext;
  }

  public static <T> T executeReturn(executorReturn<T> exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(true, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        tcontext.put("rv", ((executorReturn<T>) tparams[0]).execute(dbCon));
        return true;
      }
    };

    return (T) ta.tcontext.get("rv");
  }

  public static <T> T executeReturn(String dbName, executorReturn<T> exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(true, dbName, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        tcontext.put("rv", ((executorReturn<T>) tparams[0]).execute(dbCon));
        return true;
      }
    };

    return (T) ta.tcontext.get("rv");
  }

  public static <T> T executeReturn(Connection con, executorReturnNoconn<T> exec)
     throws Exception
  {
    SqlTransactAgent ta = new SqlTransactAgent(false, exec)
    {
      @Override
      public boolean run(Connection dbCon, boolean transactionSupported)
         throws Exception
      {
        tcontext.put("rv", ((executorReturnNoconn<T>) tparams[0]).execute());
        return true;
      }
    };

    ta.runTransaction(con);
    return (T) ta.tcontext.get("rv");
  }
}
