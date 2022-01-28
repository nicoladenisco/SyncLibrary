/*
 *  RuleForeignSlaveXmlRpc.java
 *  Creato il Nov 26, 2017, 4:11:03 PM
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
package it.infomed.sync.slave;

import it.infomed.sync.Location;
import it.infomed.sync.SyncContext;
import it.infomed.sync.Utils;
import it.infomed.sync.plugins.AbstractRule;
import it.infomed.sync.plugins.SyncAgentPlugin;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Plugin di sincronizzazione per Diamante via XML-RPC.
 * Versione client/slave.
 *
 * @author Nicola De Nisco
 */
public class RuleSlave extends AbstractRule
{
  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
  }

  @Override
  public String getRole()
  {
    return ROLE_SLAVE;
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
  public void pianificaAggiornamento(String nome, List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    SyncAgentPlugin agent = agentMap.get(nome);
    agent.pianificaAggiornamento(aggiorna, vInfo, context);
  }

  @Override
  public List<String> getListaBlocchi()
     throws Exception
  {
    return arBlocchi;
  }
}
