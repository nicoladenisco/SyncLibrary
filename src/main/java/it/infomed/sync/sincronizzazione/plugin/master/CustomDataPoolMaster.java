/*
 *  CustomDataPoolMaster.java
 *  Creato il Mar 12, 2020, 12:37:35 PM
 *
 *  Copyright (C) 2020 Informatica Medica s.r.l.
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

import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import it.infomed.sync.common.FieldLinkInfoBean;
import it.infomed.sync.common.SyncSetupErrorException;
import it.infomed.sync.common.plugin.AbstractDataPool;
import it.infomed.sync.common.plugin.SyncPoolPlugin;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.ClassOper;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * Pool dati custom.
 * Questo pool dati gira la richiesta ad un oggetto generico specificato nel parametro classname.
 *
 * @author Nicola De Nisco
 */
public class CustomDataPoolMaster extends AbstractDataPool
{
  protected String className;
  protected SyncPoolPlugin worker;
  private String basePath = null;
  private String[] vPaths = null;
  private Configuration cfg = null;

  @Override
  public String getRole()
  {
    return ROLE_MASTER;
  }

  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
    super.configure(cfg);

    this.cfg = cfg;
    basePath = ClassOper.getClassPackage(this.getClass());
    vPaths = cfg.getStringArray("classpath");
  }

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    if((className = okStrNull(data.getChildText("classname"))) == null)
      throw new SyncSetupErrorException(0, "pool:classname");

    Class cls = ClassOper.loadClass(className, basePath, vPaths);
    worker = (SyncPoolPlugin) cls.newInstance();
    worker.setParentRule(parentRule);
    worker.configure(cfg);
    worker.setXML(location, data);
  }

  @Override
  public Pair<List<Record>, Schema> getDatiVerifica(String dataBlockName, String poolData, Date oldTimestamp,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception
  {
    return worker.getDatiVerifica(dataBlockName, poolData, oldTimestamp, arFields, extraFilter);
  }

  @Override
  public Pair<List<Record>, Schema> getDatiAggiorna(String dataBlockName, String poolData,
     ArrayMap<String, String> arKeys, List<String> parametri,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception
  {
    return worker.getDatiAggiorna(dataBlockName, poolData, arKeys, parametri, arFields, extraFilter);
  }

  @Override
  public boolean haveNativeFilter()
  {
    return worker.haveNativeFilter();
  }

  @Override
  public void clearPool()
  {
    worker.clearPool();
  }
}
