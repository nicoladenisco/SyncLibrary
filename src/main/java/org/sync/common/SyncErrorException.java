/*
 *  SyncErrorException.java
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
package org.sync.common;

/**
 * Errore generico di sincronizzazione.
 * 
 * @author Nicola De Nisco
 */
public class SyncErrorException extends Exception
{
  public SyncErrorException(String message)
  {
    super(message);
  }

  public SyncErrorException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
