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
package it.infomed.sync.sincronizzazione.plugin.slave;

import it.infomed.sync.common.SyncContext;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.Utils;
import it.infomed.sync.common.plugin.AbstractRule;
import it.infomed.sync.common.plugin.DeleteStrategyNothing;
import it.infomed.sync.common.plugin.SyncAgentPlugin;
import it.infomed.sync.common.plugin.SyncPluginFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.Pair;

/**
 * Plugin di sincronizzazione per Diamante via XML-RPC.
 * Versione client/slave.
 *
 * @author Nicola De Nisco
 */
public class RuleForeignSlaveXmlRpc extends AbstractRule
{
  protected String nomeRegola, delStrategyName;
  protected ArrayList<String> arBlocchi = new ArrayList<>();
  protected HashMap<String, SyncAgentPlugin> agentMap = new HashMap<>();

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
  public void setConfig(String nomeRegola, Map context)
     throws Exception
  {
    this.nomeRegola = nomeRegola;
    arBlocchi.clear();
    agentMap.clear();

    // legge eventuale delete strategy
    if((delStrategyName = (String) context.get("delete-strategy-name")) == null)
    {
      delStrategy = new DeleteStrategyNothing();
    }
    else
    {
      delStrategy = SyncPluginFactory.getInstance().buildDeleteStrategy(getRole(), delStrategyName);
      delStrategy.setConfig(delStrategyName, (Map) context.get("delete-strategy-data"));
    }

    // legge eventuale filtro sql
    setFilter(Utils.parseFilterKeyData(context));

    Vector vData = (Vector) context.get("data-blocks");
    context.remove("data-blocks");

    for(int i = 0; i < vData.size(); i++)
    {
      Hashtable ht = (Hashtable) vData.get(i);
      String nomeBlocco = (String) ht.get("name");
      String nomeAgent = (String) ht.get("agent");

      if(agentMap.containsKey(nomeBlocco))
        throw new SyncSetupErrorException(String.format("Data block %s duplicato nel file XML.", nomeBlocco));

      SyncAgentPlugin agent = SyncPluginFactory.getInstance().buildAgent(getRole(), nomeAgent);
      agent.setParentRule(this);
      agent.setConfig(nomeAgent, ht);
      agentMap.put(nomeBlocco, agent);
      arBlocchi.add(nomeBlocco);
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
