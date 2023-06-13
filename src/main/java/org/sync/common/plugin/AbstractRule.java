/*
 *  AbstractRule.java
 *  Creato il Nov 27, 2017, 12:04:04 PM
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
package org.sync.common.plugin;

import org.sync.common.FilterKeyData;
import org.sync.common.SyncContext;
import java.util.Date;
import java.util.List;
import org.commonlib5.utils.Pair;

/**
 * Classe base di tutti i gestore di regole.
 *
 * @author Nicola De Nisco
 */
public class AbstractRule extends AbstractPlugin
   implements SyncRulePlugin
{
  protected SyncDeletePlugin delStrategy;
  protected FilterKeyData filter;
  protected String databaseName;

  @Override
  public String getDatabaseName()
  {
    return databaseName;
  }

  @Override
  public SyncDeletePlugin getDelStrategy()
  {
    return delStrategy;
  }

  @Override
  public void setDelStrategy(SyncDeletePlugin delStrategy)
  {
    this.delStrategy = delStrategy;
  }

  @Override
  public FilterKeyData getFilter()
  {
    return filter;
  }

  @Override
  public void setFilter(FilterKeyData filter)
  {
    this.filter = filter;
  }

  @Override
  public void verificaBlocco(String nome, List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void aggiornaBlocco(String nome, List<String> parametri, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<String> getListaBlocchi()
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void pianificaAggiornamento(String nome, List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SyncPoolPlugin getPool(String poolName)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void beginRumbleRule(SyncContext context)
     throws Exception
  {
  }

  @Override
  public void endRumbleRule(SyncContext context)
     throws Exception
  {
  }
}
