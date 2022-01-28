/*
 *  AgentCopyUpdateMaster.java
 *  Creato il 9-lug-2021, 19.08.44
 *
 *  Copyright (C) 2021 Informatica Medica s.r.l.
 *
 *  Questo software è proprietà di Informatica Medica s.r.l.
 *  Tutti gli usi non esplicitimante autorizzati sono da
 *  considerarsi tutelati ai sensi di legge.
 *
 *  Informatica Medica s.r.l.
 *  Viale dei Tigli, 19
 *  Casalnuovo di Napoli (NA)
 */
package it.infomed.sync.sincronizzazione.plugin.master;

import java.util.List;
import java.util.Map;
import org.commonlib5.xmlrpc.VectorRpc;
import org.jdom2.Element;

/**
 * Adapter per la copia di tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentCopyUpdateMaster extends AgentTableUpdateLocalMaster
{
  protected Element sqlStatements;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    sqlStatements = data.getChild("sql-statements");
  }

  @Override
  public void populateConfigForeign(Map context)
     throws Exception
  {
    super.populateConfigForeign(context);

    VectorRpc v = new VectorRpc();
    List<Element> lsStatements = sqlStatements.getChildren("statement");
    for(Element estat : lsStatements)
    {
      String sql = okStrNull(estat.getText());
      if(sql != null)
        v.add(sql);
    }

    if(!v.isEmpty())
      context.put("sql-statements", v);
  }
}
