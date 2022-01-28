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
package it.infomed.sync.plugins;

import it.infomed.sync.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

import static org.commonlib5.utils.StringOper.checkTrueFalse;

/**
 * Classe base di tutti i gestore di regole.
 *
 * @author Nicola De Nisco
 */
public class AbstractRule extends AbstractPlugin
   implements SyncRulePlugin
{
  protected Element rule;
  protected String nomeRegola;
  protected ArrayList<String> arBlocchi = new ArrayList<>();
  protected HashMap<String, SyncAgentPlugin> agentMap = new HashMap<>();
  protected HashMap<String, SyncPoolPlugin> poolMap = new HashMap<>();

  protected DeleteStrategy delStrategy;
  protected FilterKeyData filter;

  @Override
  public DeleteStrategy getDelStrategy()
  {
    return delStrategy;
  }

  @Override
  public void setDelStrategy(DeleteStrategy delStrategy)
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

  protected void createDataPools(Location el)
     throws Exception
  {
    Element pools = rule.getChild("data-pools");
    if(pools == null)
      return;

    Element location = pools.getChild(el.name());
    if(location == null)
      return;

    List<Element> lsPool = location.getChildren("pool");

    for(Element pool : lsPool)
    {
      if(checkTrueFalse(pool.getAttributeValue("ignore"), false))
        continue;

      String nomeBlocco = pool.getAttributeValue("name");
      String nomeAgent = pool.getAttributeValue("agent");

      if(poolMap.containsKey(nomeBlocco))
        throw new SyncSetupErrorException(I.I("Pool %s duplicato nel file XML.", nomeBlocco));

      SyncPoolPlugin agent = SyncPluginFactory.getInstance().buildPool(getRole(), nomeAgent);
      agent.setParentRule(this);
      agent.setXML(el, pool);
      poolMap.put(nomeBlocco, agent);
    }
  }

  protected void createDataBlocks(Location el)
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
        throw new SyncSetupErrorException(I.I("Data block %s duplicato nel file XML.", nomeBlocco));

      SyncAgentPlugin agent = SyncPluginFactory.getInstance().buildAgent(getRole(), nomeAgent);
      agent.setParentRule(this);
      agent.setXML(el, data);
      agentMap.put(nomeBlocco, agent);
    }
  }

  @Override
  public void setXML(Location el, Element data)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
