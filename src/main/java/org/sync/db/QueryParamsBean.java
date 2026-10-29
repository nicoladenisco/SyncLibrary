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
package org.sync.db;

import com.workingdogs.village.QueryDataSetMacro;
import com.workingdogs.village.Record;
import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bean per mantenere i parametri di una query.
 *
 * @author Nicola De Nisco
 */
public class QueryParamsBean implements Serializable, Cloneable
{
  private String select, from, where, orderby, groupby, having;
  private List<String> extraWhere = new ArrayList<>();
  private List<Object> substituctions = new ArrayList<>();
  private Map<String, Object> valuesMap = new HashMap<>();

  public QueryParamsBean()
  {
    clear();
  }

  public void clear()
  {
    select = from = where = orderby = groupby = having = "";
    extraWhere.clear();
    substituctions.clear();
    valuesMap.clear();
  }

  @Override
  public Object clone()
     throws CloneNotSupportedException
  {
    QueryParamsBean other = (QueryParamsBean) super.clone();
    other.extraWhere = new ArrayList<>(extraWhere);
    other.substituctions = new ArrayList<>(substituctions);
    other.valuesMap = new HashMap<>(valuesMap);
    return other;
  }

  public String getSelect()
  {
    return select;
  }

  public void setSelect(String select)
  {
    this.select = select;
  }

  public String getFrom()
  {
    return from;
  }

  public void setFrom(String from)
  {
    this.from = from;
  }

  public String getWhere()
  {
    return where;
  }

  public void setWhere(String where)
  {
    this.where = where;
  }

  public String getOrderby()
  {
    return orderby;
  }

  public void setOrderby(String orderby)
  {
    this.orderby = orderby;
  }

  public String getGroupby()
  {
    return groupby;
  }

  public void setGroupby(String groupby)
  {
    this.groupby = groupby;
  }

  public String getHaving()
  {
    return having;
  }

  public void setHaving(String having)
  {
    this.having = having;
  }

  public void addWhere(String exwhere)
  {
    if(where.isEmpty())
    {
      where = exwhere;
      return;
    }

    this.extraWhere.add(exwhere);
  }

  public void addSubstituction(Object substituction)
  {
    this.substituctions.add(substituction);
  }

  public void addValues(String queryPart, Object value)
  {
    this.valuesMap.put(queryPart, value);
  }

  public List<String> getExtraWhere()
  {
    return extraWhere;
  }

  public List<Object> getSubstituctions()
  {
    return substituctions;
  }

  public Map<String, ? extends Object> getValuesMap()
  {
    return valuesMap;
  }

  public String makeSQLstring()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ").append(select).append("\n");
    sb.append("FROM ").append(from).append("\n");

    if(!where.isEmpty())
      sb.append("WHERE (").append(where).append(")\n");

    for(String ew : extraWhere)
    {
      sb.append(" AND (").append(ew).append(")\n");
    }

    if(!groupby.isEmpty())
      sb.append("GROUP BY ").append(groupby).append("\n");
    if(!having.isEmpty())
      sb.append("HAVING ").append(having).append("\n");
    if(!orderby.isEmpty())
      sb.append("ORDER BY ").append(orderby).append("\n");

    return sb.toString();
  }

  public QueryDataSetMacro buildQueryDataset(Connection con)
     throws Exception
  {
    return new QueryDataSetMacro(con, makeSQLstring(), valuesMap);
  }

  public List<Record> fetchAllRecords(Connection con)
     throws Exception
  {
    return QueryDataSetMacro.fetchAllRecords(con, makeSQLstring(), valuesMap);
  }
}
