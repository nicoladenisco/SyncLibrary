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
import java.util.HashMap;
import java.util.Optional;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.sync.common.SyncContext;
import org.sync.common.plugin.SyncPluginFactory;
import org.sync.common.plugin.SyncRulePlugin;

/**
 * Test sincronizzazione lato client.
 *
 * @author Nicola De Nisco
 */
public class SyncClientTest extends RuleRunner
{
  protected Document doc = null;
  protected Configuration cfg = null;
  protected HashMap<String, SyncRulePlugin> ruleMap = new HashMap<>();

  @Override
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
    super.init();
  }

  private Configuration getConfiguration()
     throws Exception
  {
    URL resource = this.getClass().getResource("/config.properties");
    if(resource == null)
      throw new IOException("Risorsa config.properties non trovata.");

    Configurations configs = new Configurations();
    FileBasedConfigurationBuilder<PropertiesConfiguration> builder = configs.fileBasedBuilder(PropertiesConfiguration.class, resource);

    return builder.getConfiguration();
  }

  public Element acquisiciStrategia(String nomeRegola, SyncContext context)
     throws Exception
  {
    return findPlugin(nomeRegola, "");
  }

  protected Element findPlugin(String application, String target)
     throws Exception
  {
    // cerca una regola controllando gli attributi
    // application="diamante" master="caleido" target="all"
    Optional<Element> rule = doc.getRootElement().getChildren("rule").stream()
       .filter((e) -> SyncServerTest.CU.isEquAny(e.getAttributeValue("application"), "any", application)
          && SyncServerTest.CU.isEquAny(e.getAttributeValue("target"), "any", target))
       .findFirst();

    if(!rule.isPresent())
      die(String.format("Nessuna regola corrisponde ai parametri."));

    return rule.get();
  }

  protected synchronized SyncRulePlugin findPlugin(String nomeRegola)
     throws Exception
  {
    // cerca una regola controllando gli attributi
    // application="diamante" master="caleido" target="all"
    Optional<Element> rule = doc.getRootElement().getChildren("rule").stream()
       .filter((e) -> SyncServerTest.CU.isEqu(nomeRegola, e.getAttributeValue("name")))
       .findFirst();

    if(!rule.isPresent())
      die(String.format("La regola %s non esiste; rivedere setup.", nomeRegola));

    Element re = rule.get();

    SyncRulePlugin plugin = ruleMap.get(nomeRegola);
    if(plugin != null)
      return plugin;

    String ruleType = SyncServerTest.CU.okStr(re.getAttributeValue("type"));
    String roleType = SyncServerTest.CU.okStr(re.getAttributeValue("caleido-role"));
    plugin = SyncPluginFactory.getInstance().buildRule(roleType, ruleType);
    plugin.setXML("master", rule.get());

    ruleMap.put(nomeRegola, plugin);
    return plugin;
  }

  private void die(String format)
  {
    throw new RuntimeException(format);
  }
}
