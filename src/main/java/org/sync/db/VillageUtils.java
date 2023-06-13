/*
 *  VillageUtils.java
 *  Creato il Nov 27, 2017, 2:57:47 PM
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
package org.sync.db;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import com.workingdogs.village.DataSetException;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.workingdogs.village.QueryDataSet;
import com.workingdogs.village.Record;
import com.workingdogs.village.TableDataSet;
import com.workingdogs.village.Value;

/**
 * Some Village related code factored out of the BasePeer.
 *
 * @author <a href="mailto:hps@intermeta.de">Henning P. Schmiedehausen</a>
 * @version $Id: VillageUtils.java 476550 2006-11-18 16:08:37Z tfischer $
 */
public final class VillageUtils
{
  /** The log. */
  private static Log log = LogFactory.getLog(VillageUtils.class);

  /**
   * Private constructor to prevent instantiation.
   *
   * Class contains only static method ans should therefore not be
   * instantiated.
   */
  private VillageUtils()
  {
  }

  /**
   * Convenience Method to close a Table Data Set without
   * Exception check.
   *
   * @param tds A TableDataSet
   */
  public static final void close(final TableDataSet tds)
  {
    if(tds != null)
    {
      try
      {
        tds.close();
      }
      catch(Exception ignored)
      {
        log.debug("Caught exception when closing a TableDataSet",
           ignored);
      }
    }
  }

  /**
   * Convenience Method to close a Table Data Set without
   * Exception check.
   *
   * @param qds A TableDataSet
   */
  public static final void close(final QueryDataSet qds)
  {
    if(qds != null)
    {
      try
      {
        qds.close();
      }
      catch(Exception ignored)
      {
        log.debug("Caught exception when closing a QueryDataSet",
           ignored);
      }
    }
  }

  /**
   * Convenience Method to close an Output Stream without
   * Exception check.
   *
   * @param os An OutputStream
   */
  public static final void close(final OutputStream os)
  {
    try
    {
      if(os != null)
      {
        os.close();
      }
    }
    catch(Exception ignored)
    {
      log.debug("Caught exception when closing an OutputStream",
         ignored);
    }
  }

  /**
   * Converts a hashtable to a byte array for storage/serialization.
   *
   * @param hash The Hashtable to convert.
   * @return A byte[] with the converted Hashtable.
   * @throws Exception If an error occurs.
   */
  public static final byte[] hashtableToByteArray(final Hashtable hash)
     throws Exception
  {
    Hashtable saveData = new Hashtable(hash.size());
    byte[] byteArray = null;

    Iterator keys = hash.entrySet().iterator();
    while(keys.hasNext())
    {
      Map.Entry entry = (Map.Entry) keys.next();
      if(entry.getValue() instanceof Serializable)
      {
        saveData.put(entry.getKey(), entry.getValue());
      }
    }

    ByteArrayOutputStream baos = null;
    BufferedOutputStream bos = null;
    ObjectOutputStream out = null;
    try
    {
      // These objects are closed in the finally.
      baos = new ByteArrayOutputStream();
      bos = new BufferedOutputStream(baos);
      out = new ObjectOutputStream(bos);

      out.writeObject(saveData);

      out.flush();
      bos.flush();
      baos.flush();
      byteArray = baos.toByteArray();
    }
    finally
    {
      close(out);
      close(bos);
      close(baos);
    }
    return byteArray;
  }

  /**
   * Factored out setting of a Village Record column from a generic value.
   *
   * @param value the value to set
   * @param rec The Village Record
   * @param colName The name of the column in the record
   * @throws java.lang.Exception
   */
  public static final void setVillageValue(Object value,
     final Record rec,
     final String colName)
     throws Exception
  {
    if(value == null)
    {
      rec.setValueNull(colName);
    }
    else if(value instanceof String)
    {
      rec.setValue(colName, (String) value);
    }
    else if(value instanceof Integer)
    {
      rec.setValue(colName, (int) value);
    }
    else if(value instanceof BigDecimal)
    {
      rec.setValue(colName, (BigDecimal) value);
    }
    else if(value instanceof Boolean)
    {
      rec.setValue(colName, ((Boolean) value).booleanValue());
    }
    else if(value instanceof java.util.Date)
    {
      rec.setValue(colName, (java.util.Date) value);
    }
    else if(value instanceof Float)
    {
      rec.setValue(colName, (float) value);
    }
    else if(value instanceof Double)
    {
      rec.setValue(colName, (double) value);
    }
    else if(value instanceof Byte)
    {
      rec.setValue(colName, ((Byte) value).byteValue());
    }
    else if(value instanceof Long)
    {
      rec.setValue(colName, (long) value);
    }
    else if(value instanceof Short)
    {
      rec.setValue(colName, ((Short) value).shortValue());
    }
    else if(value instanceof Hashtable)
    {
      rec.setValue(colName, hashtableToByteArray((Hashtable) value));
    }
    else if(value instanceof byte[])
    {
      rec.setValue(colName, (byte[]) value);
    }
  }

  public static Object mapVillageToObject(Value v)
     throws Exception
  {
    if(v.isBigDecimal())
    {
      return v.asBigDecimal();
    }
    else if(v.isByte())
    {
      return v.asInt();
    }
    else if(v.isBytes())
    {
      return null;
    }
    else if(v.isDate())
    {
      return v.asUtilDate();
    }
    else if(v.isShort())
    {
      return v.asInt();
    }
    else if(v.isInt())
    {
      return v.asInt();
    }
    else if(v.isLong())
    {
      return v.asInt();
    }
    else if(v.isDouble())
    {
      return v.asDouble();
    }
    else if(v.isFloat())
    {
      return v.asDouble();
    }
    else if(v.isBoolean())
    {
      return v.asBoolean();
    }
    else if(v.isString())
    {
      return v.asString();
    }
    else if(v.isTime())
    {
      return v.asUtilDate();
    }
    else if(v.isTimestamp())
    {
      return v.asUtilDate();
    }
    else if(v.isUtilDate())
    {
      return v.asUtilDate();
    }

    return v.toString();
  }

  public static int getFieldIndex(String fieldName, Record r)
  {
    try
    {
      return r.schema().index(fieldName);
    }
    catch(DataSetException ex)
    {
      return -1;
    }
  }
}
