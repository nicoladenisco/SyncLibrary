/*
 *  FieldLinkInfoBean.java
 *  Creato il Nov 24, 2017, 8:52:23 PM
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
package org.sync.common;

import org.sync.common.plugin.SyncAdapterPlugin;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;

/**
 * informazioni sul collegamento fra i campi.
 *
 * @author Nicola De Nisco
 */
public class FieldLinkInfoBean
{
  public boolean shared, primary, identityOff, truncZeroes;
  //
  public Pair<String, String> field;
  public String shareFieldName;
  //
  public String adapterName;
  public SyncAdapterPlugin adapter;
  public Element adapterElement;

  @Override
  public String toString()
  {
    return "FieldLinkInfoBean{" + "field=" + field + '}';
  }
}
