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
package it.infomed.sync;

import java.util.ArrayList;
import java.util.List;

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
  public int timeLimitFetch = 0;
  public List<String> sqlFilterFetch = new ArrayList<>();

  public int timeLimitCompare = 0;
  public List<String> sqlFilterCompare = new ArrayList<>();
}
