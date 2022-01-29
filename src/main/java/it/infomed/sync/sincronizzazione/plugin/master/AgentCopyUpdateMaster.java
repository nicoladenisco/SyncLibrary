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

import org.jdom2.Element;

/**
 * Adapter per la copia di tabelle.
 *
 * @author Nicola De Nisco
 */
public class AgentCopyUpdateMaster extends AgentTableUpdateMaster
{
  protected Element sqlStatements;

  @Override
  public void setXML(String location, Element data)
     throws Exception
  {
    super.setXML(location, data);

    sqlStatements = data.getChild("sql-statements");
  }
}
