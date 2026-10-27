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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;
import org.sync.common.SyncContext;
import org.sync.common.plugin.SyncPluginFactory;
import org.sync.common.plugin.SyncRulePlugin;

/**
 * Test sincronizzazione lato server.
 *
 * @author Nicola De Nisco
 */
public class SyncServerTest
{
  protected Document doc = null;
  protected Configuration cfg = null;
  protected HashMap<String, SyncRulePlugin> ruleMap = new HashMap<>();

  public void init()
     throws Exception
  {
    cfg = getConfiguration();

    try(InputStream is = this.getClass().getResourceAsStream("/sincronizzazione.xml"))
    {
      SAXBuilder builder = new SAXBuilder();
      doc = builder.build(is);
    }

    // inizializza factory dei vari plugin
    SyncPluginFactory.getInstance().configure(cfg);
  }

  protected Configuration getConfiguration()
     throws Exception
  {
    URL resource = this.getClass().getResource("/config.properties");
    if(resource == null)
      throw new IOException("Risorsa config.properties non trovata.");

    Configurations configs = new Configurations();
    FileBasedConfigurationBuilder<PropertiesConfiguration> builder = configs.fileBasedBuilder(PropertiesConfiguration.class, resource);

    return builder.getConfiguration();
  }

  public void verificaBlocco(String nomeRegola, String nome,
     List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    ASSERT(plugin != null, "plugin != null - " + nomeRegola);
    plugin.verificaBlocco(nome, parametri, oldTimestamp, context);
  }

  public void aggiornaBlocco(String nomeRegola, String nome, List<String> parametri, SyncContext context)
     throws Exception
  {
    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    ASSERT(plugin != null, "plugin != null - " + nomeRegola);
    plugin.aggiornaBlocco(nome, parametri, context);
  }

  public void pianificaAggiornamento(String nomeRegola, String nome,
     List<String> aggiorna, List<String> vInfo, SyncContext context)
     throws Exception
  {
    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    ASSERT(plugin != null, "plugin != null - " + nomeRegola);
    plugin.pianificaAggiornamento(nome, aggiorna, vInfo, context);
  }

  public int inizioEsecuzioneRegola(String nomeRegola, SyncContext context)
     throws Exception
  {
    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    ASSERT(plugin != null, "plugin != null - " + nomeRegola);
    plugin.beginRumbleRule(context);
    return 0;
  }

  public int fineEsecuzioneRegola(String nomeRegola, SyncContext context)
     throws Exception
  {
    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    ASSERT(plugin != null, "plugin != null - " + nomeRegola);
    plugin.endRumbleRule(context);
    return 0;
  }

  protected void ASSERT(boolean b, String msg)
  {
    if(!b)
      throw new RuntimeException("ASSERTION FAILED: " + msg);
  }

  protected void die(String msg)
  {
    throw new RuntimeException(msg);
  }

  public static class CU extends StringOper
  {
  }
}
