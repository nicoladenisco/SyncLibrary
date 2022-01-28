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
package it.infomed.sync.sincronizzazione.plugin.master;

import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractRule;
import it.infomed.sync.common.plugin.SyncAgentPlugin;
import it.infomed.sync.common.plugin.SyncPluginFactory;
import it.infomed.sync.common.plugin.SyncPoolPlugin;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.Pair;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;

/**
 * Plugin di sincronizzazione per Diamante via XML-RPC.
 * Versione caleido/master.
 *
 * @author Nicola De Nisco
 */
public class RuleLocalMasterXmlRpc extends AbstractRule
{
  protected Element rule, delStrategyElement;
  protected HashMap<String, SyncPoolPlugin> poolMap = new HashMap<>();
  protected HashMap<String, SyncAgentPlugin> agentMap = new HashMap<>();

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
  public void setXML(String location, Element rule)
     throws Exception
  {
    this.rule = rule;

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

    createDataPools();
    createDataBlocks();
  }

  protected void createDataPools()
     throws Exception
  {
    Element pools = rule.getChild("data-pools");
    if(pools == null)
      return;

    Element master = pools.getChild("master");
    if(master == null)
      return;

    List<Element> lsPool = master.getChildren("pool");

    for(Element pool : lsPool)
    {
      if(checkTrueFalse(pool.getAttributeValue("ignore"), false))
        continue;

      String nomeBlocco = pool.getAttributeValue("name");
      String nomeAgent = pool.getAttributeValue("agent");

      if(poolMap.containsKey(nomeBlocco))
        throw new SyncSetupErrorException(String.format("Pool %s duplicato nel file XML.", nomeBlocco));

      SyncPoolPlugin agent = SyncPluginFactory.getInstance().buildPool(getRole(), nomeAgent);
      agent.setParentRule(this);
      agent.setXML(location, pool);
      poolMap.put(nomeBlocco, agent);
    }
  }

  protected void createDataBlocks()
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
  public void populateConfigForeign(Map context)
     throws Exception
  {
    Element blocks = rule.getChild("data-blocks");
    List<Element> lsData = blocks.getChildren("data");
    VectorRpc vData = new VectorRpc();

    for(Element data : lsData)
    {
      if(checkTrueFalse(data.getAttributeValue("ignore"), false))
        continue;

      String nomeBlocco = data.getAttributeValue("name");
      String nomeAgent = data.getAttributeValue("agent");

      SyncAgentPlugin agent = agentMap.get(nomeBlocco);
      ASSERT(agent != null, "agent != null");

      HashtableRpc hr = new HashtableRpc();
      hr.put("name", nomeBlocco);
      hr.put("agent", nomeAgent);
      vData.add(hr);

      agent.populateConfigForeign(hr);
    }

    context.put("rule-name", rule.getAttributeValue("name"));
    context.put("rule-type", rule.getAttributeValue("type"));
    context.put("your-role", ROLE_SLAVE);
    context.put("data-blocks", vData);

    if(delStrategyElement != null)
    {
      String name = delStrategyElement.getAttributeValue("name");
      HashMap params = new HashMap();
      delStrategy.populateConfigForeign(params);

      context.put("delete-strategy-name", name);
      context.put("delete-strategy-data", params);
    }

    if(filter != null)
      Utils.formatFilterKeyData(filter, context);
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
