/*
 *  Copyright (C) 2011 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 *
 *  Creato il 24-ott-2011, 10.43.58
 */
package it.infomed.sync;

import java.util.function.Function;

/**
 * Gestione dei messaggi per internazionalizzazione.
 *
 * @author Nicola De Nisco
 */
public class I
{
  private static Function<String, String> lmi = null;

  public static Function<String, String> getInterface()
  {
    return lmi;
  }

  public static void setInterface(Function<String, String> lmi)
  {
    I.lmi = lmi;
  }

  public static String I(String msg)
  {
    return lmi != null ? lmi.apply(msg) : msg;
  }

  public static String I(String formatString, Object... objects)
  {
    return String.format(lmi != null ? lmi.apply(formatString) : formatString, objects);
  }
}
