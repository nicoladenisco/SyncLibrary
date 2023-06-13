/*
 *  RuleSlave.java
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
package org.sync.sincronizzazione.plugin.slave;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;
import org.sync.common.SyncContext;
import org.sync.common.SyncSetupErrorException;
import org.sync.common.Utils;
import org.sync.common.plugin.AbstractRule;
import org.sync.common.plugin.SyncAgentPlugin;
import org.sync.common.plugin.SyncPluginFactory;
import org.sync.db.Database;

/**
 * Plugin di sincronizzazione per Diamante via XML-RPC.
 * Versione client/slave.
 *
 * @author Nicola De Nisco
 */
public class RuleSlave extends AbstractRule
{
  protected Element rule, delStrategyElement;
  protected HashMap<String, SyncAgentPlugin> agentMap = new HashMap<>();
  protected ArrayList<String> arBlocchi = new ArrayList<>();

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
  public void setXML(String location, Element rule)
     throws Exception
  {
    this.rule = rule;
    databaseName = okStr(rule.getAttributeValue("database-name"), Database.getDefaultDB());

    // legge eventuale delete strategy
    if((delStrategyElement = Utils.getChildTestName(rule, "delete-strategy")) == null)
    {
      delStrategy = SyncPluginFactory.getInstance().buildDeleteStrategy(getRole(), "generic");
    }
    else
    {
      String name = delStrategyElement.getAttributeValue("name");
      delStrategy = SyncPluginFactory.getInstance().buildDeleteStrategy(getRole(), name);
    }

    // legge eventuale filtro sql
    setFilter(Utils.parseFilterKeyData(rule));

    createDataBlocks(location);
  }

  protected void createDataBlocks(String location)
     throws Exception
  {
    Element blocks = rule.getChild("data-blocks");
    List<Element> lsData = blocks.getChildren("data");

    for(Element data : lsData)
    {
      if(checkTrueFalse(data.getAttributeValue("ignore"), false))
        continue;

      String nomeBlocco = data.getAttributeValue("name");
      String nomeAgent = data.getAttributeValue("agent");

      if(agentMap.containsKey(nomeBlocco))
        throw new SyncSetupErrorException(String.format("Data block %s duplicato nel file XML.", nomeBlocco));

      SyncAgentPlugin agent = SyncPluginFactory.getInstance().buildAgent(getRole(), nomeAgent);
      agent.setParentRule(this);
      agent.setXML(location, data);
      agentMap.put(nomeBlocco, agent);
    }
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
