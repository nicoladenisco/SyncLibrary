/*
 *  AbstractDataPool.java
 *  Creato il Mar 12, 2020, 11:39:24 AM
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
package org.sync.common.plugin;

import com.workingdogs.village.QueryDataSetMacro;
import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.Pair;

/**
 * Classe base di tutti i data pool.
 *
 * @author Nicola De Nisco
 */
public abstract class AbstractDataPool extends AbstractPlugin
   implements SyncPoolPlugin
{
  protected SyncRulePlugin parentRule;

  @Override
  public SyncRulePlugin getParentRule()
  {
    return parentRule;
  }

  @Override
  public void setParentRule(SyncRulePlugin parentRule)
  {
    this.parentRule = parentRule;
  }

  @Override
  public boolean haveNativeFilter()
  {
    return false;
  }

  protected Pair<List<Record>, Schema> query(String sSQL, Map<String, String> params, Connection con)
     throws Exception
  {
    try(QueryDataSetMacro qds = new QueryDataSetMacro(con, sSQL, params))
    {
      return new Pair<>(qds.fetchAllRecords(), qds.schema());
    }
  }
}
