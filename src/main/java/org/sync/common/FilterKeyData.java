/*
 *  FilterKeyData.java
 *  Creato il Feb 2, 2020, 11:44:35 AM
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
package org.sync.common;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.StringOper;
import org.sync.db.QueryParamsBean;

/**
 * Filtro per le chiavi.
 * Definisce un filtro da applicare al prelievo delle chiavi (slave) e/o
 * al confronto delle chiavi (master).
 * <br>
 * L'attributo <b>time-limit-days</b> è valido solo per i datablocks che
 * hanno timestamp e stabilisce quando indietro nel tempo deve avvenire
 * l'estrazione delle chiavi, sotto forma di numero di giorni.
 * <br>
 * I tag <b>sql</b> possono essere ripetuti e utilizzati nella query
 * di selezione delle chiavi dal database.
 * <br>
 * La semantica per i due blocchi è identica.
 * <br>
 * <pre>
 * [sql-filter]
 *   [fetch-key time-limit-days="60"]
 *     [sql][![CDATA[((STATO_REC IS NULL) OR (STATO_REC [ 10))]]][/sql]
 *   [/fetch-key]
 *   [compare-key][/compare-key]
 * [/sql-filter]
 * </pre>
 *
 * @author Nicola De Nisco
 */
public class FilterKeyData
{
  private int timeLimitFetch = 0;
  private final List<String> sqlFilterFetch = new ArrayList<>();

  private int timeLimitCompare = 0;
  private final List<String> sqlFilterCompare = new ArrayList<>();

  public void clear()
  {
    timeLimitCompare = timeLimitFetch = 0;
    sqlFilterCompare.clear();
    sqlFilterFetch.clear();
  }

  public int getTimeLimitFetch()
  {
    return timeLimitFetch;
  }

  public void setTimeLimitFetch(int timeLimitFetch)
  {
    this.timeLimitFetch = timeLimitFetch;
  }

  public int getTimeLimitCompare()
  {
    return timeLimitCompare;
  }

  public void setTimeLimitCompare(int timeLimitCompare)
  {
    this.timeLimitCompare = timeLimitCompare;
  }

  public void addSqlFilterFetch(String sqlFilterFetch)
  {
    this.sqlFilterFetch.add(sqlFilterFetch);
  }

  public void addSqlFilterCompare(String sqlFilterCompare)
  {
    this.sqlFilterCompare.add(sqlFilterCompare);
  }

  public void addWhereFetch(QueryParamsBean qb)
  {
    // aggiunge clausole del filtro
    for(String sql : sqlFilterFetch)
      qb.addWhere(sql);
  }

  public void addWhereFetch(QueryParamsBean qb, String campoTestLimit)
     throws Exception
  {
    // aggiunge clausole del filtro
    for(String sql : sqlFilterFetch)
      qb.addWhere(sql);

    // se richiesto aggiunge limitazione temporale
    if(timeLimitFetch != 0 && StringOper.isOkStr(campoTestLimit))

    {
      Calendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -timeLimitFetch);
      Date firstDate = cal.getTime();
      qb.addWhere(campoTestLimit + " >= ${campoTestLimit:ts}");
      qb.addValues("campoTestLimit", firstDate);
    }
  }

  public String createWhereFetch()
     throws Exception
  {
    String sSQL = "";

    // aggiunge clausole del filtro
    for(String sql : sqlFilterFetch)
      sSQL += " AND (" + sql + ")";

    return sSQL;
  }

  public String createWhereFetch(String campoTestLimit)
     throws Exception
  {
    String sSQL = "";

    // aggiunge clausole del filtro
    for(String sql : sqlFilterFetch)
      sSQL += " AND (" + sql + ")";

    // se richiesto aggiunge limitazione temporale
    if(timeLimitFetch != 0 && StringOper.isOkStr(campoTestLimit))
    {
      Calendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -timeLimitFetch);
      Date firstDate = cal.getTime();
      sSQL += " AND (" + campoTestLimit + " >= '" + DateTime.formatIsoFull(firstDate) + "')";
    }

    return sSQL;
  }

  public void addWhereCompare(QueryParamsBean qb)
  {
    // aggiunge clausole del filtro
    for(String sql : sqlFilterCompare)
      qb.addWhere(sql);
  }

  public void addWhereCompare(QueryParamsBean qb, String campoTestLimit)
     throws Exception
  {
    // aggiunge clausole del filtro
    for(String sql : sqlFilterCompare)
      qb.addWhere(sql);

    // se richiesto aggiunge limitazione temporale
    if(timeLimitCompare != 0 && StringOper.isOkStr(campoTestLimit))
    {
      Calendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -timeLimitCompare);
      Date firstDate = cal.getTime();
      qb.addWhere(campoTestLimit + " >= ${campoTestLimit:ts}");
      qb.addValues("campoTestLimit", firstDate);
    }
  }

  public String createWhereCompare()
     throws Exception
  {
    String sSQL = "";

    // aggiunge clausole del filtro
    for(String sql : sqlFilterCompare)
      sSQL += " AND (" + sql + ")";

    return sSQL;
  }

  public String createWhereCompare(String campoTestLimit)
     throws Exception
  {
    String sSQL = "";

    // aggiunge clausole del filtro
    for(String sql : sqlFilterCompare)
      sSQL += " AND (" + sql + ")";

    // se richiesto aggiunge limitazione temporale
    if(timeLimitCompare != 0 && StringOper.isOkStr(campoTestLimit))
    {
      Calendar cal = new GregorianCalendar();
      cal.add(Calendar.DAY_OF_YEAR, -timeLimitCompare);
      Date firstDate = cal.getTime();
      sSQL += " AND (" + campoTestLimit + " >= '" + DateTime.formatIsoFull(firstDate) + "')";
    }

    return sSQL;
  }
}
