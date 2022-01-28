/*
 *  SyncContext.java
 *  Creato il Nov 19, 2017, 4:50:04 PM
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
package it.infomed.sync.common;

import org.commonlib5.xmlrpc.HashtableRpc;

/**
 * Contesto per le informazioni di sincronizzazione.
 *
 * @author Nicola De Nisco
 */
public class SyncContext extends HashtableRpc
{
  public Object getNotNull(String key)
     throws Exception
  {
    Object rv = get(key);
    if(rv == null)
      throw new MissingSyncParameterException(String.format("Il parametro '%s' non è presente nel context.", key));

    return rv;
  }
}
