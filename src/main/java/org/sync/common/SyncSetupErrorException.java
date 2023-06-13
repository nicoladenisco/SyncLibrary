/*
 *  SyncSetupErrorException.java
 *  Creato il Nov 24, 2017, 7:54:47 PM
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
 * Errore nel setup di sincronizzazione.
 *
 * @author Nicola De Nisco
 */
public class SyncSetupErrorException extends Exception
{
  public SyncSetupErrorException(int msg, String detail)
  {
    super(formatMessage(msg, detail));
  }

  public SyncSetupErrorException(String message)
  {
    super(message);
  }

  public SyncSetupErrorException(String message, Throwable cause)
  {
    super(message, cause);
  }

  private static String formatMessage(int msg, String detail)
  {
    switch(msg)
    {
      case 0:
        return String.format("Elemento %s non trovato nel setup XML.", detail);
      case 1:
        return String.format("Elemento %s duplicato nel setup XML.", detail);
    }

    return "";
  }
}
