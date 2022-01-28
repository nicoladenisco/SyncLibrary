/*
 *  RuleLocalMasterXmlRpc.java
 *  Creato il Nov 24, 2017, 7:13:57 PM
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
package it.infomed.sync.master;

import it.infomed.sync.Location;
import it.infomed.sync.SyncContext;
import it.infomed.sync.Utils;
import it.infomed.sync.plugins.AbstractRule;
import it.infomed.sync.plugins.SyncAgentPlugin;
import it.infomed.sync.plugins.SyncPoolPlugin;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Plugin di sincronizzazione per Diamante via XML-RPC.
 * Versione caleido/master.
 *
 * @author Nicola De Nisco
 */
public class RuleMaster extends AbstractRule
{
  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
  }

  @Override
  public String getRole()
  {
    return ROLE_MASTER;
  }

  @Override
  public void setXML(Location el, Element rule)
     throws Exception
  {
    this.rule = rule;

    // legge eventuale delete strategy
    setDelStrategy(Utils.parseDeleteStrategy(rule));

    // legge eventuale filtro sql
    setFilter(Utils.parseFilterKeyData(rule));

    createDataPools(el);
    createDataBlocks(el);
  }

  @Override
  public void verificaBlocco(String nome, List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    SyncAgentPlugin agent = agentMap.get(nome);
    agent.verificaBlocco(parametri, oldTimestamp, context);
  }

  @Override
  public void aggiornaBlocco(String nome, List<String> parametri, SyncContext context)
     throws Exception
  {
    SyncAgentPlugin agent = agentMap.get(nome);
    agent.aggiornaBlocco(parametri, context);
  }

  @Override
  public SyncPoolPlugin getPool(String poolName)
     throws Exception
  {
    return poolMap.get(poolName);
  }

  @Override
  public void beginRumbleRule(SyncContext context)
     throws Exception
  {
    poolMap.forEach((nome, pool) -> pool.clearPool());
  }

  @Override
  public void endRumbleRule(SyncContext context)
     throws Exception
  {
    poolMap.forEach((nome, pool) -> pool.clearPool());
  }
}
