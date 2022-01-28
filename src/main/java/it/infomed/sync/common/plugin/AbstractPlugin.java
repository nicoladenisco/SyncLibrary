/*
 *  AbstractPlugin.java
 *  Creato il Nov 27, 2017, 12:00:19 PM
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
package it.infomed.sync.common.plugin;

import it.infomed.sync.common.SyncErrorException;
import java.io.File;
import java.util.Date;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commonlib5.utils.CommonFileUtils;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.StringOper;
import org.jdom2.Element;

/**
 * Classe base di tutti i plugin di aggiornamento.
 *
 * @author Nicola De Nisco
 */
public class AbstractPlugin extends StringOper
   implements SyncPlugin
{
  protected Log log = LogFactory.getLog(getClass());
  protected Date FAR_DATE = DateTime.mergeDataOra(1, 1, 1900, 0, 0, 0);

  @Override
  public void configure(Configuration cfg)
     throws Exception
  {
  }

  @Override
  public String getRole()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void ASSERT(boolean test, String cause)
     throws SyncErrorException
  {
    if(!test)
    {
      String mess = "ASSERT failed: " + cause;
      log.debug(mess);
      throw new SyncErrorException(mess);
    }
  }

  /**
   * Verifica che il file esista.
   *
   * @param toTest
   * @throws java.lang.Exception
   */
  public void ASSERT_FILE(File toTest)
     throws SyncErrorException
  {
    if(!(toTest.exists() && toTest.isFile()))
    {
      String mess = String.format(
         "ASSERT_FILE failed: il file %s non esiste.",
         toTest.getAbsolutePath());
      log.debug(mess);
      throw new SyncErrorException(mess);
    }
  }

  /**
   * Verifica che la directory esista.
   *
   * @param toTest
   * @throws java.lang.Exception
   */
  public void ASSERT_DIR(File toTest)
     throws SyncErrorException
  {
    if(!(toTest.exists() && toTest.isDirectory()))
    {
      String mess = String.format(
         "ASSERT_DIR failed: la directory %s non esiste.",
         toTest.getAbsolutePath());
      log.debug(mess);
      throw new SyncErrorException(mess);
    }
  }

  /**
   * Verifica che la directory esista e sia
   * possibile creare files al suo interno.
   *
   * @param toTest
   * @throws java.lang.Exception
   */
  public void ASSERT_DIR_WRITE(File toTest)
     throws SyncErrorException
  {
    toTest.mkdirs();
    if(!(toTest.exists() && toTest.isDirectory() && CommonFileUtils.checkDirectoryWritable(toTest)))
    {
      String mess = String.format(
         "ASSERT_DIR_WRITE failed: la directory %s non esiste o non e' scrivibile.",
         toTest.getAbsolutePath());
      log.debug(mess);
      throw new SyncErrorException(mess);
    }
  }

  public void die(String causa)
     throws SyncErrorException
  {
    throw new SyncErrorException(causa);
  }

  @Override
  public Object clone()
     throws CloneNotSupportedException
  {
    return super.clone();
  }
}
