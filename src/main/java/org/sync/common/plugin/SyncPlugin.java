/*
 *  SyncPlugin.java
 *  Creato il Nov 26, 2017, 12:35:04 PM
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
package org.sync.common.plugin;

import org.apache.commons.configuration2.Configuration;
import org.jdom2.Element;

/**
 * Interfaccia comune a tutti i plugin.
 *
 * @author Nicola De Nisco
 */
public interface SyncPlugin extends Cloneable
{
  public static final String ROLE_MASTER = "master";
  public static final String ROLE_SLAVE = "slave";
  public static final String FETCH_ALL_DATA_FLAG = "fetch-all-data";

  /**
   * Configura il plugin con i dati di setup.
   * @param cfg sezione di setup con eventuali stringhe per il plugin
   * @throws Exception
   */
  public void configure(Configuration cfg)
     throws Exception;

  /**
   * Ritorna il ruolo di caleido.
   * In genere una delle due costanti 'master' o 'slave'
   * @return ruolo nella sincronizzazione
   */
  public String getRole();

  /**
   * Imposta l'elemento XML che rappresenta questo plugin.
   * Il plugin estrarrà tutti i dati di setup di cui ha bisogno.
   * @param data elemento xml
   * @throws Exception
   */
  public void setXML(String location, Element data)
     throws Exception;

  /**
   * Restituisce un clone dell'oggetto.
   * @return clone
   * @throws CloneNotSupportedException
   */
  public Object clone()
     throws CloneNotSupportedException;
}
