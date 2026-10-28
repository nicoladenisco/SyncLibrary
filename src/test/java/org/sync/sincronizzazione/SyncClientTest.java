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
    if(!initalized)
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
    Element e = findPlugin(nomeRegola, doc);

    // nel test il server è master il client è slave
    context.put("rule-name", nomeRegola);
    context.put("your-role", "slave");
    context.put("rule-type", "generic");

    return e;
  }
}
