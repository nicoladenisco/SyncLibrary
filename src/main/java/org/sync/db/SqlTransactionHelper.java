/*
 * Copyright (C) 2021 Nicola De Nisco
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.sync.db;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Utilità per eseguire operazioni read/write senza usare lambda.
 * Questa versione usa una generica connessione sql fornita nel costruttore.
 *
 * @author Nicola De Nisco
 */
public class SqlTransactionHelper implements Closeable
{
  protected Connection dbCon = null;
  protected DatabaseMetaData md = null;

  public SqlTransactionHelper()
     throws DatabaseException
  {
    this.dbCon = Database.getConnection();
  }

  public SqlTransactionHelper(String dbName)
     throws DatabaseException
  {
    this.dbCon = Database.getConnection(dbName);
  }

  public SqlTransactionHelper(Connection dbCon)
  {
    this.dbCon = dbCon;
  }

  public Connection getConnection()
     throws Exception
  {
    md = dbCon.getMetaData();

    // disattiva autocommit se attivo
    if(md.supportsTransactions() && dbCon.getAutoCommit())
      dbCon.setAutoCommit(false);

    return dbCon;
  }

  public void commit()
     throws Exception
  {
    dbCon.commit();
  }

  public void rollback()
     throws Exception
  {
    dbCon.rollback();
  }

  @Override
  public void close()
     throws IOException
  {
    try
    {
      dbCon.rollback();
      dbCon.close();
    }
    catch(SQLException ex)
    {
      throw new IOException(ex);
    }
  }

  public DatabaseMetaData getDatabaseMetaData()
  {
    return md;
  }
}
