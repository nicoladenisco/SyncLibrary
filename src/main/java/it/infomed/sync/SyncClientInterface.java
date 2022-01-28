/*
 *  SyncClientInterface.java
 *  Creato il Mar 14, 2020, 10:32:29 AM
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

import it.infomed.sync.common.SyncContext;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.commonlib5.utils.Pair;

/**
 * Interfaccia per le funzioni di comunicazione con Applicazione.
 *
 * @author Nicola De Nisco
 */
public interface SyncClientInterface
{
  /**
   * Scarica la strategia di aggiornamento.
   * Ovvero l'elenco dei blocchi che verranno aggiornati.
   * Viene effettuata una ricerca di strategia per applicazione e target.
   * @param application l'applicazione che richiede l'operazione
   * @param target il target specifico
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  void acquisiciStrategia(String application, String target, SyncContext context)
     throws Exception;

  /**
   * Scarica la strategia di aggiornamento.
   * Ovvero l'elenco dei blocchi che verranno aggiornati.
   * Non viene effettuata ricerca: il nomeRegola determina la strategia.
   * @param nomeRegola nome della regola richiesta
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  void acquisiciStrategia(String nomeRegola, SyncContext context)
     throws Exception;

  /**
   * Acquisice i dati dei blocchi da aggiornare.
   * Per il blocco indicato e le chiavi primarie specificate nella richiesta,
   * estrae i record da aggiornare.
   * @param nomeRegola nome della regola di riferimento
   * @param nome nome del blocco
   * @param parametri parametri della richiesta: chiave dei records
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  void aggiornaBlocco(String nomeRegola, String nome, List<String> parametri, SyncContext context)
     throws Exception;

  /**
   * Pianifica aggiornamento richiesto.
   * @param nomeRegola nome della regola di riferimento
   * @param nome nome del blocco
   * @param aggiorna parametri della richiesta: chiave dei records
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  void pianificaAggiornamento(String nomeRegola, String nome, List<String> aggiorna, SyncContext context)
     throws Exception;

  /**
   * Verifica per aggiornamento records.
   * Per ogni blocco riporta la chiave primaria e il timestamp di aggiornamento.
   * Il server risponde con un elenco di chiavi primarie da aggiornare.
   * Se oldTimestamp è valorizzato risponderà solo per le modifiche avvenute successivamente.
   * @param nomeRegola nome della regola di riferimento
   * @param nome nome del blocco
   * @param parametri parametri della richiesta: chiave/timestamp ultimo aggiornamento
   * @param oldTimestamp ultimo timestamp di una richiesta simile (può essere null)
   * @param context contesto da popolare con i risultati
   * @throws Exception
   */
  void verificaBlocco(String nomeRegola, String nome, List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception;

  /**
   * Inizio di esecuzione della regola.
   * Consente operazioni di inizializzazione della regola.
   * @param nomeRegola
   * @param context
   * @throws Exception
   */
  void beginRumbleRule(String nomeRegola, SyncContext context)
     throws Exception;

  /**
   * Fine esecuzione della regola.
   * Consente operazioni di pulizia dopo l'esecuzione.
   * @param nomeRegola
   * @param context
   * @throws Exception
   */
  void endRumbleRule(String nomeRegola, SyncContext context)
     throws Exception;

  /**
   * Recupera configurazione.
   * @return
   */
  Configuration getConfiguration();
}
