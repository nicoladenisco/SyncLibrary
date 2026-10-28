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

import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.SyncSetupErrorException;
import org.sync.common.plugin.AbstractDataPool;
import org.sync.db.Database;

/**
 * Test data pool.
 *
 * @author Nicola De Nisco
 */
public class TestDataPool extends AbstractDataPool
{
  @Override
  public RoleType getRole()
  {
    return RoleType.MASTER;
  }

  @Override
  public void setXML(FunctionType location, Element data)
     throws Exception
  {
  }

  @Override
  public Pair<List<Record>, Schema> getDatiVerifica(
     String dataBlockName, String poolData, Date oldTimestamp,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception
  {
    try(Connection con = Database.getConnection())
    {
      return getData(poolData, oldTimestamp, arFields, extraFilter, con);
    }
  }

  @Override
  public Pair<List<Record>, Schema> getDatiAggiorna(
     String dataBlockName, String poolData, ArrayMap<String, String> arKeys, List<String> parametri,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception
  {
    try(Connection con = Database.getConnection())
    {
      return getData(poolData, null, arFields, extraFilter, con);
    }
  }

  @Override
  public void clearPool()
  {
  }

  private Pair<List<Record>, Schema> getData(String poolData, Date oldTimestamp,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter, Connection con)
     throws Exception
  {
    switch(poolData.toLowerCase())
    {
      case "tipietichette":
      {
        String sSQL
           = "SELECT T.*,T.codice codtipoeti\n"
           + "  FROM origin_tipietichette T\n"
           + " WHERE (T.stato_rec IS NULL OR T.stato_rec < 10)";

        return query(sSQL, extraFilter, con);
      }

      default:
      {
        throw new SyncSetupErrorException("bocco non presente nel pool: " + poolData);
      }
    }
  }
}
