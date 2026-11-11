/*
 *  AdapterGetFieldMax.java
 *  Creato il Nov 28, 2017, 5:04:37 PM
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
package org.sync.sincronizzazione.plugin;

import com.workingdogs.village.Column;
import com.workingdogs.village.Schema;
import com.workingdogs.village.VillageUtils;
import java.sql.Connection;
import java.util.List;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Element;
import org.sync.common.FieldLinkInfoBean;
import org.sync.common.RecordDataMap;
import org.sync.common.RecordsDataBlock;
import org.sync.common.SyncContext;
import org.sync.common.Utils;
import org.sync.common.plugin.AbstractAdapter;
import org.sync.common.plugin.AbstractAgent;
import org.sync.sincronizzazione.LocationType;
import org.sync.sincronizzazione.RoleType;
import org.sync.sincronizzazione.SqlSaveType;
import org.sync.sincronizzazione.plugin.slave.AgentGenericSlave;
import org.sync.sincronizzazione.plugin.slave.AgentSharedGenericSlave;

/**
 * Adapter per generazione valore estraendolo da una sequenza.
 * Questo adapter può essere utilizzato solo come slave.
 *
 * @author Nicola De Nisco
 */
public class AdapterGetFieldSequence extends AbstractAdapter
{
  protected String tableName, dbName, sequenceName;
  private HashtableRpc setup;

  @Override
  public void setXML(LocationType location, Element data)
     throws Exception
  {
    setup = Utils.parseParams(data);
    sequenceName = setup.getAsString("sequenceName");
  }

  @Override
  public RoleType getRole()
  {
    return RoleType.SLAVE;
  }

  @Override
  public void slavePreparaValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
    this.tableName = ((AgentSharedGenericSlave) parentAgent).correctTableName;
    this.dbName = ((AgentGenericSlave) parentAgent).databaseName;

    if(sequenceName == null)
    {
      // utilizza la regola di default di postgres per i campi serial
      // ad esempio stp.branche_branche_id_seq
      Schema st = ((AbstractAgent) parentAgent).getSchema();
      List<Column> primaryKeys = st.getPrimaryKeys();

      if(primaryKeys.size() == 1)
      {
        String primaryName = primaryKeys.get(0).name();
        sequenceName = tableName + "_" + primaryName + "_seq";
      }
    }
  }

  @Override
  public void slaveFineValidazione(RecordsDataBlock lsRecs, List<FieldLinkInfoBean> arFields, FieldLinkInfoBean field, SyncContext context)
     throws Exception
  {
  }

  @Override
  public Object slaveValidaValore(String key, SqlSaveType st, RecordDataMap record, Object v, FieldLinkInfoBean f, Connection con)
     throws Exception
  {
    if(sequenceName != null && st == SqlSaveType.INSERT)
    {
      // recupera valore dalla sequenza
      return VillageUtils.getValueFromSequence(sequenceName, con);
    }

    return null;
  }
}
