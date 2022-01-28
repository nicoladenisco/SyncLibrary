/*
 * SyncPluginFactory.java
 *
 * Created on 15 Luglio 2008, 15:40
 *
 * Copyright (C) Informatica Medica s.r.l.
 */
package it.infomed.sync.common.plugin;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.ClassOper;
import org.commonlib5.utils.StringOper;

/**
 * Factory dei plugin di sincronizzazione workstation refertazione.
 *
 * @author Nicola De Nisco
 */
public class SyncPluginFactory
{
  /** Logging */
  private static Log log = LogFactory.getLog(SyncPluginFactory.class);

  private String basePath = null;
  private String[] vPaths = null;
  private Configuration cfg = null;
  private static SyncPluginFactory theInstance = new SyncPluginFactory();

  private SyncPluginFactory()
  {
    basePath = ClassOper.getClassPackage(this.getClass());
  }

  public static SyncPluginFactory getInstance()
  {
    return theInstance;
  }

  public void configure(Configuration cfg)
  {
    this.cfg = cfg;
    vPaths = cfg.getStringArray("classpath");
  }

  private String getSetupSegment(String type, String role, String nome)
  {
    return "sync.plugin." + type + "." + role + "." + nome;
  }

  private <T extends SyncPlugin> T buildGeneric(String type, String role, String nome, Class<T> cls)
     throws Exception
  {
    if((nome = StringOper.okStrNull(nome)) == null)
      return null;

    String sseg = getSetupSegment(type, role, nome);
    String className = StringOper.okStrNull(cfg.getString(sseg + ".classname"));
    if(className == null)
      throw new Exception("Plugin " + type + "." + role + "." + nome + " sconosciuto.");

    T sp = (T) loadClass(role, className);
    Configuration cfgLocal = cfg.subset(sseg);
    sp.configure(cfgLocal);
    return sp;
  }

  public SyncRulePlugin buildRule(String role, String nome)
     throws Exception
  {
    return buildGeneric("rule", role, nome, SyncRulePlugin.class);
  }

  public SyncPoolPlugin buildPool(String role, String nome)
     throws Exception
  {
    return buildGeneric("pool", role, nome, SyncPoolPlugin.class);
  }

  public SyncAgentPlugin buildAgent(String role, String nome)
     throws Exception
  {
    return buildGeneric("agent", role, nome, SyncAgentPlugin.class);
  }

  public SyncAdapterPlugin buildAdapter(String role, String nome)
     throws Exception
  {
    return buildGeneric("adapter", role, nome, SyncAdapterPlugin.class);
  }

  public SyncValidatorPlugin buildValidator(String role, String nome)
     throws Exception
  {
    return buildGeneric("validator", role, nome, SyncValidatorPlugin.class);
  }

  public SyncDeletePlugin buildDeleteStrategy(String role, String nome)
     throws Exception
  {
    return buildGeneric("delete", role, nome, SyncDeletePlugin.class);
  }

  private Object loadClass(String role, String className)
     throws Exception
  {
    try
    {
      Class cl;

      if((cl = ClassOper.loadClass(role + "." + className, basePath, vPaths)) != null)
        return cl.newInstance();
      if((cl = ClassOper.loadClass(className, basePath, vPaths)) != null)
        return cl.newInstance();
    }
    catch(Throwable t)
    {
      log.error("", t);
    }

    throw new Exception(String.format("Classe %s non trovata o non istanziabile.", className));
  }

  /**
   * Ritorna elenco delle path per la ricerca della classe del plugin.
   * @return array delle path
   */
  public String[] getBasePaths()
  {
    return vPaths;
  }

  /**
   * Imposta elenco delle path per la ricerca della classe del plugin.
   * @param vPaths array delle path
   */
  public void setBasePaths(String[] vPaths)
  {
    this.vPaths = vPaths;
  }

  /**
   * Aggiunge all'elenco delle path per la ricerca della classe del plugin.
   * @param basePath path da aggiungere
   */
  public void addBasePath(String basePath)
  {
    vPaths = (String[]) ArrayUtils.add(vPaths, basePath);
  }
}
