/*
 *  DeleteStrategy.java
 *  Creato il Jan 31, 2020, 6:40:20 PM
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
import org.commonlib5.utils.Pair;

/**
 * Informazioni sulla logica di cancellazione.
 * La logica di cancellazione viene specificata a livello di regola
 * oppure in override a livello di datablock.
 *
 * <pre>
 * [delete-strategy type="logical"]
 *   [unknow sameasdelete="true" /]
 *   [delete type="logical"]
 *     [sql-update]STATO_REC=10[/sql-update]
 *     [sql][![CDATA[....]]]
 *     [/sql]
 *   [/delete]
 * [/delete-strategy]
 * </pre>
 *
 * La cancellazione viene implementata solo dagli slave.
 * I parametri con 'Delete' vengono utilizzati quando il master segnala 'key/DELETE'
 * ovvero quando il record è stato cancellato logicamente nel master.
 * I parametri con 'Unknow' vengono utilizzati quando il master segnala 'key/UNKNOW'
 * a indicare un record che non è presente nel master.
 * L'attributo del tag unknow sameasdelete="true" significa che il comportamento di
 * unknow deve essere lo stesso di delete.
 * <br>
 * tipo... può essere nothing, logical, phisical
 * sqlUpdate...Statement sono la componente set di una istruzione UPDATE
 * costruita internamente dal sincronizzatore.
 * sqlGeneric...Statement sono delle istruzioni SQL complete eseguite
 * per cancellare; in queste istruzioni la macro ${key} si espande nella
 * chiave da cancellare (nomecampo=valore and nomecampo=valore).
 * <br>
 * entrambi gli attibuti sql-update e sql possono apparire più volte.
 *
 * @author Nicola De Nisco
 */
public class DeleteStrategy
{
  public static class DeleteField
  {
    public Pair<String, String> field;
    public String valueNormal, valueDelete;
  }

  public String tipoUnknow = "nothing";
  public List<DeleteField> sqlUpdateUnknowStatement = new ArrayList<>();
  public List<String> sqlGenericUnknowStatement = new ArrayList<>();

  public String tipoDelete = "nothing";
  public List<DeleteField> sqlUpdateDeleteStatement = new ArrayList<>();
  public List<String> sqlGenericDeleteStatement = new ArrayList<>();
}
