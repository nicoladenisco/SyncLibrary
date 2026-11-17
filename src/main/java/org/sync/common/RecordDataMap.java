/*
 * Copyright (C) 2026 Nicola De Nisco
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.sync.common;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableObject;
import org.commonlib5.utils.DateTime;
import org.commonlib5.utils.StringOper;

/**
 * Map speciale per contenere i valori dei recods.
 *
 * @author Nicola De Nisco
 */
public class RecordDataMap extends CaseInsensitiveMap<String, String>
{
  public RecordDataMap()
  {
  }

  public RecordDataMap(Map<String, String> m)
  {
    super(m);
  }

  public void put(FieldLinkInfoBean f, String value)
  {
    super.put(f.shareFieldName, value);
  }

  public String get(FieldLinkInfoBean f)
  {
    return super.get(f.shareFieldName);
  }

  public String getAsString(String key)
  {
    return StringOper.okStr(get(key));
  }

  public MutableObject getAsMutableString(String key)
  {
    return new MutableObject(StringOper.okStr(get(key)));
  }

  public String getAsStringNull(String key)
  {
    return StringOper.okStrNull(get(key));
  }

  public String getAsStringThrow(String key)
  {
    String s = StringOper.okStrNull(get(key));
    if(s == null)
      throw new IllegalArgumentException("Valore obbligatorio mancante per " + key);
    return s;
  }

  public MutableObject getAsMutableStringNull(String key)
  {
    String s = StringOper.okStrNull(get(key));
    return s == null ? null : new MutableObject(s);
  }

  public void getAsMutableString(String key, MutableObject obj)
  {
    obj.setValue(StringOper.okStr(get(key)));
  }

  public boolean getAsMutableStringNull(String key, MutableObject obj)
  {
    String s = StringOper.okStrNull(get(key));
    if(s != null)
    {
      obj.setValue(s);
      return true;
    }
    return false;
  }

  public int getAsInt(String key)
  {
    return getAsInt(key, 0);
  }

  public double getAsDouble(String key)
  {
    return getAsDouble(key, 0.0);
  }

  public boolean getAsBoolean(String key)
  {
    return getAsBoolean(key, false);
  }

  public Date getAsDate(String key)
  {
    return getAsDate(key, null);
  }

  public String getAsString(String key, String defVal)
  {
    return StringOper.okStr(get(key), defVal);
  }

  public int getAsInt(String key, int defVal)
  {
    Object rv = super.get(key);

    if(rv == null)
      return defVal;

    if(rv instanceof Number)
      return ((Number) rv).intValue();

    return StringOper.parse(rv, defVal);
  }

  public double getAsDouble(String key, double defVal)
  {
    Object rv = super.get(key);

    if(rv == null)
      return defVal;

    if(rv instanceof Number)
      return ((Number) rv).doubleValue();

    return StringOper.parse(rv, defVal);
  }

  public boolean getAsBoolean(String key, boolean defVal)
  {
    Object rv = super.get(key);

    if(rv == null)
      return defVal;

    if(rv instanceof Boolean)
      return (Boolean) rv;

    return StringOper.checkTrueFalse(rv, defVal);
  }

  public Date getAsDate(String key, Date defVal)
  {
    Object rv = super.get(key);

    if(rv == null)
      return defVal;

    if(rv instanceof Date)
      return (Date) rv;

    return DateTime.parseIsoFull(rv.toString(), defVal);
  }

  public Number getAsNumber(String key)
  {
    return getAsNumber(key, 0.0);
  }

  public Number getAsNumber(String key, Number defVal)
  {
    Object rv = super.get(key);

    if(rv == null)
      return defVal;

    if(rv instanceof Number)
      return (Number) rv;

    return StringOper.parse(rv.toString(), defVal.doubleValue());
  }

  public Object put(String key, Object value)
  {
    if(value == null)
      return remove(key);

    if(value instanceof Collection || value.getClass().isArray())
      throw new IllegalArgumentException("Collections or Arrays are proibited.");

    // valori tipi base più pratici in forma di stringa
    if(value instanceof String)
      value = StringOper.okStrNull(value);
    else if(value instanceof java.sql.Date)
      value = DateTime.formatIso((java.sql.Date) value, "");
    else if(value instanceof java.sql.Timestamp)
      value = DateTime.formatIsoFull((java.sql.Timestamp) value, "");
    else if(value instanceof java.util.Date)
      value = DateTime.formatIsoFull((java.util.Date) value, "");

    if(value == null)
      return remove(key);

    return super.put(key, value.toString());
  }

  @Override
  public void putAll(Map t)
  {
    for(Object key : t.keySet())
    {
      Object val = t.get(key);
      this.put(key.toString(), val);
    }
  }

  public RecordDataMap append(String key, Object value)
  {
    put(key, value);
    return this;
  }

  @Override
  public String put(String key, String value)
  {
    if((value = StringOper.okStrNull(value)) == null)
      return remove(key);

    return super.put(key, value);
  }

  public String put(String key, int value)
  {
    return put(key, Integer.toString(value));
  }

  public String put(String key, double value)
  {
    return put(key, Double.toString(value));
  }

  public String put(String key, boolean value)
  {
    return put(key, value ? "1" : "0");
  }

  public String put(String key, Date value)
     throws Exception
  {
    return put(key, DateTime.formatIsoFull(value));
  }

  public String put(String key, Timestamp value)
     throws Exception
  {
    return put(key, DateTime.formatIsoFull(value));
  }

  public String putDateOnly(String key, Date value)
     throws Exception
  {
    return put(key, DateTime.formatIso(value));
  }

  public String putTimeOnly(String key, Date value)
     throws Exception
  {
    return put(key, StringOper.right(DateTime.formatIsoFull(value), 8));
  }
}
