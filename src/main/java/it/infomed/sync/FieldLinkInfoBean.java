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
package it.infomed.sync;

import it.infomed.sync.plugins.SyncAdapterPlugin;
import it.infomed.sync.plugins.SyncValidatorPlugin;
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
  public Pair<String, String> localField, foreignField;
  //
  public String foreignAdapterName, adapterName;
  public SyncAdapterPlugin foreignAdapter, localAdapter, adapter;
  public Element foreignAdapterElement, localAdapterElement, adapterElement;
  //
  public String foreignFieldValidatorName;
  public SyncValidatorPlugin foreignFieldValidator;
  public Element foreignFieldValidatorElement;

  @Override
  public String toString()
  {
    if(localField == null)
      return "foreignField:" + foreignField;

    if(foreignField == null)
      return "localField:" + localField;

    return "FieldLinkInfoBean{" + "localField:" + localField + ", foreignField:" + foreignField + '}';
  }
}
