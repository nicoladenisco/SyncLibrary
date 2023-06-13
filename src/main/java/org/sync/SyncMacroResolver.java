/*
 *  SyncMacroResolver.java
 *
 *  Creato il 9 Luglio 2017
 *
 *  Copyright (C) 2017 RAD-IMAGE s.r.l.
 *
 *  RAD-IMAGE s.r.l.
 *  Via San Giovanni, 1 - Contrada Belvedere
 *  San Nicola Manfredi (BN)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.sync;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.MacroResolver;

/**
 * Risolutore generico di macro.
 * In una stringa possono apparire costrutti del tipo ${macro}
 * dove 'macro' viene risolto attraverso una map di parametri.<br>
 * Una serie di macro predefinite vengono risolte indipendentemente
 * dalla map:
 * <ul>
 * <li>YEAR_FIRST - primo giorno dell'anno corrente</li>
 * <li>YEAR_LAST - ultimo giorno dell'anno corrente</li>
 * <li>PREV_YEAR_FIRST - primo giorno dell'anno precedente</li>
 * <li>PREV_YEAR_LAST - ultimo giorno dell'anno precedente</li>
 * <li>MONTH_FIRST - primo giorno del mese corrente</li>
 * <li>MONTH_LAST - ultimo giorno del mese corrente</li>
 * <li>GFIRST - data del primo giorno dell'anno</li>
 * <li>GLAST - data dell'ultimo giorno dell'anno</li>
 * <li>TODAY - data odierna (solo data)</li>
 * <li>TODAYM30 - data odierna meno 30 giorni (solo data)</li>
 * <li>TODAYM60 - data odierna meno 60 giorni (solo data)</li>
 * <li>TOTIME - data odierna (data e ora)</li>
 * <li>TOTIMEM30 - data odierna meno 30 giorni (data e ora)</li>
 * <li>TOTIMEM60 - data odierna meno 60 giorni (data e ora)</li>
 * <li>IYEAR - intervallo primo e ultimo giorno dell'anno</li>
 * <li>IMOUNTH - intervallo primo e ultimo giorno del mese</li>
 *
 * <li>ISO_YEAR_FIRST - primo giorno dell'anno corrente in formato ISO</li>
 * <li>ISO_YEAR_LAST - ultimo giorno dell'anno corrente in formato ISO</li>
 * <li>ISO_PREV_YEAR_FIRST - primo giorno dell'anno precedente in formato ISO</li>
 * <li>ISO_PREV_YEAR_LAST - ultimo giorno dell'anno precedente in formato ISO</li>
 * <li>ISO_MONTH_FIRST - primo giorno del mese corrente in formato ISO</li>
 * <li>ISO_MONTH_LAST - ultimo giorno del mese corrente in formato ISO</li>
 * <li>ISO_GFIRST - data del primo giorno dell'anno in formato ISO</li>
 * <li>ISO_GLAST - data dell'ultimo giorno dell'anno in formato ISO</li>
 * <li>ISO_TODAY - data odierna (solo data) in formato ISO</li>
 * <li>ISO_TODAYM30 - data odierna meno 30 giorni (solo data) in formato ISO</li>
 * <li>ISO_TODAYM60 - data odierna meno 60 giorni (solo data) in formato ISO</li>
 * <li>ISO_TOTIME - data odierna (data e ora) in formato ISO</li>
 * <li>ISO_TOTIMEM30 - data odierna meno 30 giorni (data e ora) in formato ISO</li>
 * <li>ISO_TOTIMEM60 - data odierna meno 60 giorni (data e ora) in formato ISO</li>
 * <li>ISO_IYEAR - intervallo primo e ultimo giorno dell'anno in formato ISO</li>
 * <li>ISO_IMOUNTH - intervallo primo e ultimo giorno del mese in formato ISO</li>
 *
 * <li>ESERCIZIO - anno corrente in quattro cifre (2017)</li>
 * </ul>
 * Le date sono in formato ISO.
 * @author Nicola De Nisco
 */
public class SyncMacroResolver extends MacroResolver
{
  public SyncMacroResolver()
  {
    initFunctions();
  }

  protected void initFunctions()
  {
    mapFunction.put("YEAR_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_YEAR, 1);
      return formatData(cal.getTime());
    });

    mapFunction.put("YEAR_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_YEAR, 365);
      return formatData(cal.getTime());
    });

    mapFunction.put("PREV_YEAR_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.add(Calendar.YEAR, -1);
      cal.set(Calendar.DAY_OF_YEAR, 1);
      return formatData(cal.getTime());
    });

    mapFunction.put("PREV_YEAR_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.add(Calendar.YEAR, -1);
      cal.set(Calendar.DAY_OF_YEAR, 365);
      return formatData(cal.getTime());
    });

    mapFunction.put("MONTH_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_MONTH, 1);
      return formatData(cal.getTime());
    });

    mapFunction.put("MONTH_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
      cal.set(Calendar.DAY_OF_MONTH, lastDay);
      return formatData(cal.getTime());
    });

    mapFunction.put("GFIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 0, 1, 0, 0, 0);
      return formatData(cal.getTime());
    });

    mapFunction.put("GLAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
      return formatData(cal.getTime());
    });

    mapFunction.put("IYEAR", (seg) ->
    {
      String rv = "";
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 0, 1, 0, 0, 0);
      rv += formatData(cal.getTime());
      cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
      rv += "|" + formatData(cal.getTime());
      return rv;
    });

    mapFunction.put("IMONTH", (seg) ->
    {
      String rv = "";
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0, 0);
      rv += formatData(cal.getTime());
      cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMaximum(Calendar.DAY_OF_MONTH), 23, 59, 59);
      rv += "|" + formatData(cal.getTime());
      return rv;
    });

    mapFunction.put("TODAY", (seg) -> formatData(today));
    mapFunction.put("TOTIME", (seg) -> formatDataFull(today));

    mapFunction.put("TODAYM30", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -30);
      return formatData(cal.getTime());
    });

    mapFunction.put("TODAYM60", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -60);
      return formatData(cal.getTime());
    });

    mapFunction.put("TOTIMEM30", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -30);
      return formatDataFull(cal.getTime());
    });

    mapFunction.put("TOTIMEM60", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -60);
      return formatDataFull(cal.getTime());
    });

    mapFunction.put("ISO_YEAR_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_YEAR, 1);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_YEAR_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_YEAR, 365);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_PREV_YEAR_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.add(Calendar.YEAR, -1);
      cal.set(Calendar.DAY_OF_YEAR, 1);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_PREV_YEAR_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.add(Calendar.YEAR, -1);
      cal.set(Calendar.DAY_OF_YEAR, 365);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_MONTH_FIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      cal.set(Calendar.DAY_OF_MONTH, 1);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_MONTH_LAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.setTime(today);
      int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
      cal.set(Calendar.DAY_OF_MONTH, lastDay);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_GFIRST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 0, 1, 0, 0, 0);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_GLAST", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_IYEAR", (seg) ->
    {
      String rv = "";
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), 0, 1, 0, 0, 0);
      rv += formatIso(cal.getTime());
      cal.set(cal.get(Calendar.YEAR), 11, 31, 23, 59, 59);
      rv += "|" + formatIso(cal.getTime());
      return rv;
    });

    mapFunction.put("ISO_IMONTH", (seg) ->
    {
      String rv = "";
      GregorianCalendar cal = new GregorianCalendar();
      cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0, 0);
      rv += formatIso(cal.getTime());
      cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMaximum(Calendar.DAY_OF_MONTH), 23, 59, 59);
      rv += "|" + formatIso(cal.getTime());
      return rv;
    });

    mapFunction.put("ISO_TODAY", (seg) -> formatIso(today));
    mapFunction.put("ISO_TOTIME", (seg) -> formatIsoFull(today));

    mapFunction.put("ISO_TODAYM30", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -30);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_TODAYM60", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -60);
      return formatIso(cal.getTime());
    });

    mapFunction.put("ISO_TOTIMEM30", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -30);
      return formatIsoFull(cal.getTime());
    });

    mapFunction.put("ISO_TOTIMEM60", (seg) ->
    {
      GregorianCalendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -60);
      return formatIsoFull(cal.getTime());
    });

    mapFunction.put("ESERCIZIO", (seg) -> Integer.toString(1900 + today.getYear()));
  }

  public String formatData(Date d)
  {
    return DateTime.Format(d, "dd/MM/yyyy");
  }

  public String formatDataFull(Date d)
  {
    return DateTime.Format(d, "dd/MM/yyyy HH:mm:ss");
  }

  public String formatIso(Date d)
     throws Exception
  {
    return DateTime.formatIso(d);
  }

  public String formatIsoFull(Date d)
     throws Exception
  {
    return DateTime.formatIsoFull(d);
  }
}
