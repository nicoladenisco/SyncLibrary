/*
 *  SyncPoolPlugin.java
 *  Creato il Mar 12, 2020, 11:40:22 AM
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
package org.sync.common.plugin;

import com.workingdogs.village.Record;
import com.workingdogs.village.Schema;
import org.sync.common.FieldLinkInfoBean;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;

/**
 * Intefaccia di un generico pool di dati.
 *
 * @author Nicola De Nisco
 */
public interface SyncPoolPlugin extends SyncPlugin
{
  /**
   * Ritorna la regola padre di questo agent.
   * @return
   */
  public SyncRulePlugin getParentRule();

  /**
   * Imposta la regola padre di questo agent.
   * @param parentRule
   */
  public void setParentRule(SyncRulePlugin parentRule);

  /**
   * Ritorna il blocco dati richiesto per la verifica.
   * @param dataBlockName nome del datablock che esegue la richiesta
   * @param poolData identificatore univoco del blocco dati
   * @param oldTimestamp eventuale time stamp per limitare la query (può essere null)
   * @param arFields lista dei campi di interesse nel blocco dati
   * @param extraFilter filtro passato dal remote per limitare il risultato
   * @throws Exception
   * @return elenco di record con i dati richiesti
   */
  public Pair<List<Record>, Schema> getDatiVerifica(String dataBlockName, String poolData, Date oldTimestamp,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception;

  /**
   * Ritorna il blocco dati richiesto per l'aggiornamento.
   * @param dataBlockName nome del datablock che esegue la richiesta
   * @param poolData identificatore univoco del blocco dati
   * @param arKeys mappa nomecampo/tipo per interpretare i parametri
   * @param parametri eventuali chiavi per restringere la query (può essere null)
   * @param arFields lista dei campi di interesse nel blocco dati
   * @param extraFilter filtro passato dal remote per limitare il risultato
   * @throws Exception
   * @return elenco di record con i dati richiesti
   */
  public Pair<List<Record>, Schema> getDatiAggiorna(String dataBlockName, String poolData,
     ArrayMap<String, String> arKeys, List<String> parametri,
     List<FieldLinkInfoBean> arFields, Map<String, String> extraFilter)
     throws Exception;

  /**
   * Disponibilita filtro nativo per aggiorna.
   * Se il pool può nativamente filtrare i dati ritornati da getDatiAggiorna
   * questa funzione ritorna vero. Diversamente un filtro a posteriori
   * viene utilizzato per filtrare i records.
   * @return verso per filtro nativo
   */
  public boolean haveNativeFilter();

  /**
   * Pulizia dei dati del pool.
   * Il pool scarica eventuali dati mantenuti in memoria.
   */
  public void clearPool();
}
