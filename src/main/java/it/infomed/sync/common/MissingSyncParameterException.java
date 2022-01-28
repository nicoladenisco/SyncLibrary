/*
 *  MissingSyncParameterException.java
 *  Creato il Nov 26, 2017, 10:57:51 AM
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

/**
 * Eccezione per la notifica di parametro non presente nel context.
 *
 * @author Nicola De Nisco
 */
public class MissingSyncParameterException extends Exception
{
  public MissingSyncParameterException(String message)
  {
    super(message);
  }
}
