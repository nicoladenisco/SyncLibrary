/*
 * Copyright (C) 2026 Nicola De Nisco
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
package org.sync.sincronizzazione;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rigel5.SetupHolder;
import org.sync.db.Database;

/**
 * Test RuleRunner.
 *
 * @author Nicola De Nisco
 */
public class RuleRunnerTest
{
  private static final String DATABASE_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
  private static final String DATABASE_URL = "jdbc:derby:/tmp/test-sync-library.db";

  public RuleRunnerTest()
  {
  }

  @BeforeClass
  public static void setUpClass()
     throws Exception
  {
    Database.addDatabaseInfo("test", DATABASE_DRIVER, DATABASE_URL + ";create=true", "", "");
    Database.setDefaultDB("test");

    SetupHolder.setQueryBuilderClassName("DerbyQueryBuilder");
  }

  @AfterClass
  public static void tearDownClass()
     throws Exception
  {
  }

  @Before
  public void setUp()
  {
    try
    {
      try(Connection con = Database.getConnection())
      {
        List<String> allTables = Database.getAllTables(con);
        if(!allTables.stream().anyMatch((s) -> "origin_siti".equalsIgnoreCase(s)))
          creaDatabaseTest(con);
      }
    }
    catch(Exception ex)
    {
      ex.printStackTrace();
    }
  }

  @After
  public void tearDown()
  {
    try
    {
      try
      {
        DriverManager.getConnection(DATABASE_URL + ";shutdown=true");
      }
      catch(SQLException e)
      {
        if(!"08006".equals(e.getSQLState()))
        {
          throw e;
        }
      }
    }
    catch(Exception ex)
    {
      ex.printStackTrace();
    }
  }

  @Test
  public void testDoRun()
     throws Exception
  {
    System.out.println("doRun");

    SyncClientTest client = new SyncClientTest();
    SyncServerTest server = new SyncServerTest();
    SyncClientTransport transport = new SyncClientTransport(server, client);

    client.init();
    server.init();

    String regola = "setup";
    client.doRun(regola, transport, Collections.EMPTY_MAP);
    System.out.println("Test completato");
  }

  private void creaDatabaseTest(Connection con)
     throws Exception
  {
    try(InputStream is = getClass().getResourceAsStream("/testdb.sql");
       BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))
    {
      Database.executeSqlScriptByLine(con, r, false);
    }

    Database.executeFileCsvResource("origin_siti", "siti_id", "/siti.csv", con);
    Database.executeFileCsvResource("origin_reparti", "reparti_id", "/reparti.csv", con);
    Database.executeFileCsvResource("origin_stampanti", "stampanti_id", "/stampanti.csv", con);
    Database.executeFileCsvResource("origin_tipietichette", "tipietichette_id", "/tipietichette.csv", con);
    Database.executeFileCsvResource("origin_zone", "zone_id", "/zone.csv", con);
  }
}
