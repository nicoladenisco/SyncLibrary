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
package it.infomed.sync;

/**
 * Errore generico di sincronizzazione.
 * 
 * @author Nicola De Nisco
 */
public class SyncIgnoreRecordException extends Exception
{
  public SyncIgnoreRecordException(String message)
  {
    super(message);
  }

  public SyncIgnoreRecordException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
