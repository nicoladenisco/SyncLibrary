/*
 *  Utils.java
 *  Creato il Nov 24, 2017, 8:14:37 PM
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
package it.infomed.sync.common;

import com.workingdogs.village.DataSetException;
import com.workingdogs.village.Record;
import com.workingdogs.village.Value;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;
import org.commonlib5.utils.StringOper;
import org.commonlib5.xmlrpc.HashtableRpc;
import org.jdom2.Attribute;
import org.jdom2.Element;

/**
 * Utilita.
 *
 * @author Nicola De Nisco
 */
public class Utils extends StringOper
{
  public static Pair<String, String> parseNameTypeThrow(Map ef, String detail)
     throws SyncSetupErrorException
  {
    if(ef == null)
      throw new SyncSetupErrorException(0, detail);

    String nome = okStr(ef.get("name"));
    String tipo = okStr(ef.get("type"));

    if(nome.isEmpty())
      throw new SyncSetupErrorException(0, detail);

    return new Pair<>(nome, tipo);
  }

  public static Pair<String, String> parseNameTypeIgnore(Map ef)
     throws SyncSetupErrorException
  {
    if(ef == null)
      return null;

    String nome = okStr(ef.get("name"));
    String tipo = okStr(ef.get("type"));

    if(nome.isEmpty())
      return null;

    return new Pair<>(nome, tipo);
  }

  public static void parseNameTypeVectorThrow(List data, ArrayMap<String, String> lsField, String detail)
     throws SyncSetupErrorException
  {
    if(data == null)
      throw new SyncSetupErrorException(0, detail);

    for(int i = 0; i < data.size(); i++)
    {
      Map m = (Map) data.get(i);
      lsField.add(parseNameTypeThrow(m, detail));
    }
  }

  public static void parseNameTypeVectorIgnore(List data, ArrayMap<String, String> lsField)
     throws SyncSetupErrorException
  {
    if(data == null)
      return;

    for(int i = 0; i < data.size(); i++)
    {
      Map m = (Map) data.get(i);
      lsField.add(parseNameTypeIgnore(m));
    }
  }

  public static Vector createNameTypeVector(ArrayMap<String, String> lsField, String name, String value)
  {
    Vector v = new Vector();
    for(Pair<String, String> p : lsField.getAsList())
      v.add(createNameTypeMap(p, name, value));
    return v;
  }

  public static HashtableRpc createNameTypeMap(Pair<String, String> p, String name, String value)
  {
    HashtableRpc hr = new HashtableRpc();
    hr.put(name, p.first);
    if(p != null && p.second != null && !p.second.isEmpty())
      hr.put(value, p.second);
    return hr;
  }

  public static String getChildName(Map ef, String child)
  {
    Map e;
    if(ef == null || (e = (Map) ef.get(child)) == null)
      return null;

    return okStrNull(e.get("name"));
  }

  public static Map getChildTestName(Map ef, String child)
  {
    Map e;
    if(ef == null || (e = (Map) ef.get(child)) == null)
      return null;

    return isOkStr(e.get("name")) ? e : null;
  }

  public static <A, B> Map<A, B> cvtPair2Map(List<Pair<A, B>> ls, Map<A, B> map)
  {
    for(Pair<A, B> l : ls)
      map.put(l.first, l.second);
    return map;
  }

  public static Pair<String, String> parseNameTypeThrow(Element ef, String detail)
     throws SyncSetupErrorException
  {
    if(ef == null)
      throw new SyncSetupErrorException(0, detail);

    String nome = okStr(ef.getAttributeValue("name"));
    String tipo = okStr(ef.getAttributeValue("type"));

    if(nome.isEmpty())
      throw new SyncSetupErrorException(0, detail);

    return new Pair<>(nome, tipo);
  }

  public static Pair<String, String> parseNameTypeIgnore(Element ef)
     throws SyncSetupErrorException
  {
    if(ef == null)
      return null;

    String nome = okStr(ef.getAttributeValue("name"));
    String tipo = okStr(ef.getAttributeValue("type"));

    if(nome.isEmpty())
      return null;

    return new Pair<>(nome, tipo);
  }

  public static String getChildName(Element ef, String child)
  {
    Element e;
    if(ef == null || (e = ef.getChild(child)) == null)
      return null;

    return okStrNull(e.getAttributeValue("name"));
  }

  public static Element getChildTestName(Element ef, String child)
  {
    Element e;
    if(ef == null || (e = ef.getChild(child)) == null)
      return null;

    return isOkStr(e.getAttributeValue("name")) ? e : null;
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

  public static HashtableRpc parseParams(Element padre)
  {
    HashtableRpc hr = new HashtableRpc();

    List<Element> lsParams = padre.getChildren("param");
    for(Element ep : lsParams)
    {
      String chiave = okStrNull(ep.getAttributeValue("name"));
      String valore = okStrNull(ep.getTextTrim());
      if(chiave != null)
        hr.put(chiave, valore);
    }

    List<Attribute> lsAttrs = padre.getAttributes();
    for(Attribute at : lsAttrs)
    {
      String chiave = okStrNull(at.getName());
      String valore = okStrNull(at.getValue());
      if(chiave != null)
        hr.put(chiave, valore);
    }

    return hr;
  }

  public static int getSqlType(String descrizioneTipo)
  {
    Integer rv = sqlStringTypeMap.get(descrizioneTipo.toUpperCase());
    return rv == null ? 0 : rv;
  }

  public static Map<String, Integer> sqlStringTypeMap = new HashMap<>();
  public static Map<Integer, String> sqlTypeStringMap = new HashMap<>();

  static
  {
    sqlStringTypeMap.put("BIT", Types.BIT);
    sqlStringTypeMap.put("TINYINT", Types.TINYINT);
    sqlStringTypeMap.put("SMALLINT", Types.SMALLINT);
    sqlStringTypeMap.put("INTEGER", Types.INTEGER);
    sqlStringTypeMap.put("BIGINT", Types.BIGINT);
    sqlStringTypeMap.put("FLOAT", Types.FLOAT);
    sqlStringTypeMap.put("REAL", Types.REAL);
    sqlStringTypeMap.put("DOUBLE", Types.DOUBLE);
    sqlStringTypeMap.put("NUMERIC", Types.NUMERIC);
    sqlStringTypeMap.put("DECIMAL", Types.DECIMAL);
    sqlStringTypeMap.put("CHAR", Types.CHAR);
    sqlStringTypeMap.put("VARCHAR", Types.VARCHAR);
    sqlStringTypeMap.put("LONGVARCHAR", Types.LONGVARCHAR);
    sqlStringTypeMap.put("DATE", Types.DATE);
    sqlStringTypeMap.put("TIME", Types.TIME);
    sqlStringTypeMap.put("TIMESTAMP", Types.TIMESTAMP);
    sqlStringTypeMap.put("BINARY", Types.BINARY);
    sqlStringTypeMap.put("VARBINARY", Types.VARBINARY);
    sqlStringTypeMap.put("LONGVARBINARY", Types.LONGVARBINARY);
    sqlStringTypeMap.put("NULL", Types.NULL);

    sqlStringTypeMap.put("OTHER", Types.OTHER);
    sqlStringTypeMap.put("JAVA_OBJECT", Types.JAVA_OBJECT);
    sqlStringTypeMap.put("DISTINCT", Types.DISTINCT);
    sqlStringTypeMap.put("STRUCT", Types.STRUCT);
    sqlStringTypeMap.put("ARRAY", Types.ARRAY);
    sqlStringTypeMap.put("BLOB", Types.BLOB);
    sqlStringTypeMap.put("CLOB", Types.CLOB);
    sqlStringTypeMap.put("REF", Types.REF);

    sqlStringTypeMap.put("DATALINK", Types.DATALINK);
    sqlStringTypeMap.put("BOOLEAN", Types.BOOLEAN);
    sqlStringTypeMap.put("ROWID", Types.ROWID);
    sqlStringTypeMap.put("NCHAR", Types.NCHAR);
    sqlStringTypeMap.put("NVARCHAR", Types.NVARCHAR);
    sqlStringTypeMap.put("LONGNVARCHAR", Types.LONGNVARCHAR);
    sqlStringTypeMap.put("NCLOB", Types.NCLOB);
    sqlStringTypeMap.put("SQLXML", Types.SQLXML);
    sqlStringTypeMap.put("REF_CURSOR", Types.REF_CURSOR);
    sqlStringTypeMap.put("TIME_WITH_TIMEZONE", Types.TIME_WITH_TIMEZONE);
    sqlStringTypeMap.put("TIMESTAMP_WITH_TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE);

    sqlTypeStringMap.put(Types.BIT, "BIT");
    sqlTypeStringMap.put(Types.TINYINT, "TINYINT");
    sqlTypeStringMap.put(Types.SMALLINT, "SMALLINT");
    sqlTypeStringMap.put(Types.INTEGER, "INTEGER");
    sqlTypeStringMap.put(Types.BIGINT, "BIGINT");
    sqlTypeStringMap.put(Types.FLOAT, "FLOAT");
    sqlTypeStringMap.put(Types.REAL, "REAL");
    sqlTypeStringMap.put(Types.DOUBLE, "DOUBLE");
    sqlTypeStringMap.put(Types.NUMERIC, "NUMERIC");
    sqlTypeStringMap.put(Types.DECIMAL, "DECIMAL");
    sqlTypeStringMap.put(Types.CHAR, "CHAR");
    sqlTypeStringMap.put(Types.VARCHAR, "VARCHAR");
    sqlTypeStringMap.put(Types.LONGVARCHAR, "LONGVARCHAR");
    sqlTypeStringMap.put(Types.DATE, "DATE");
    sqlTypeStringMap.put(Types.TIME, "TIME");
    sqlTypeStringMap.put(Types.TIMESTAMP, "TIMESTAMP");
    sqlTypeStringMap.put(Types.BINARY, "BINARY");
    sqlTypeStringMap.put(Types.VARBINARY, "VARBINARY");
    sqlTypeStringMap.put(Types.LONGVARBINARY, "LONGVARBINARY");
    sqlTypeStringMap.put(Types.NULL, "NULL");

    sqlTypeStringMap.put(Types.OTHER, "OTHER");
    sqlTypeStringMap.put(Types.JAVA_OBJECT, "JAVA_OBJECT");
    sqlTypeStringMap.put(Types.DISTINCT, "DISTINCT");
    sqlTypeStringMap.put(Types.STRUCT, "STRUCT");
    sqlTypeStringMap.put(Types.ARRAY, "ARRAY");
    sqlTypeStringMap.put(Types.BLOB, "BLOB");
    sqlTypeStringMap.put(Types.CLOB, "CLOB");
    sqlTypeStringMap.put(Types.REF, "REF");

    sqlTypeStringMap.put(Types.DATALINK, "DATALINK");
    sqlTypeStringMap.put(Types.BOOLEAN, "BOOLEAN");
    sqlTypeStringMap.put(Types.ROWID, "ROWID");
    sqlTypeStringMap.put(Types.NCHAR, "NCHAR");
    sqlTypeStringMap.put(Types.NVARCHAR, "NVARCHAR");
    sqlTypeStringMap.put(Types.LONGNVARCHAR, "LONGNVARCHAR");
    sqlTypeStringMap.put(Types.NCLOB, "NCLOB");
    sqlTypeStringMap.put(Types.SQLXML, "SQLXML");
    sqlTypeStringMap.put(Types.REF_CURSOR, "REF_CURSOR");
    sqlTypeStringMap.put(Types.TIME_WITH_TIMEZONE, "TIME_WITH_TIMEZONE");
    sqlTypeStringMap.put(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_WITH_TIMEZONE");
  }

  public static boolean isNumeric(String type)
  {
    return isEquAny(type, "BOOLEAN", "BYTE", "SHORT", "INTEGER", "LONG", "FLOAT", "DOUBLE", "BIGDECIMAL");
  }

  public static boolean isString(String type)
  {
    return isEquAny(type, "STRING");
  }

  public static boolean isDate(String type)
  {
    return isEquAny(type, "DATE", "TIME", "TIMESTAMP");
  }

  public static boolean isBinary(String type)
  {
    return isEquAny(type, "BINARY", "VARBINARY", "LONGVARBINARY");
  }

  public static FilterKeyData parseFilterKeyData(Element e)
     throws SyncSetupErrorException
  {
    Element ef = e.getChild("sql-filter");
    if(ef == null)
      return null;

    FilterKeyData fkd = new FilterKeyData();

    {
      Element fetch = ef.getChild("fetch-keys");
      if(fetch != null)
      {
        fkd.timeLimitFetch = parse(fetch.getAttributeValue("time-limit-days"), fkd.timeLimitFetch);
        List<Element> lsSQL = fetch.getChildren("sql");
        for(Element esq : lsSQL)
        {
          String tmp = okStr(esq.getText());
          if(!tmp.isEmpty())
            fkd.sqlFilterFetch.add(tmp);
        }
      }
    }

    {
      Element compare = ef.getChild("compare-keys");
      if(compare != null)
      {
        fkd.timeLimitCompare = parse(compare.getAttributeValue("time-limit-days"), fkd.timeLimitCompare);
        List<Element> lsSQL = compare.getChildren("sql");
        for(Element esq : lsSQL)
        {
          String tmp = okStr(esq.getText());
          if(!tmp.isEmpty())
            fkd.sqlFilterCompare.add(tmp);
        }
      }
    }

    return fkd;
  }

  public static FilterKeyData parseFilterKeyData(Map m)
     throws SyncSetupErrorException
  {
    Map ef = (Map) m.get("sql-filter");
    if(ef == null)
      return null;

    FilterKeyData fkd = new FilterKeyData();

    {
      Map fetch = (Map) ef.get("fetch-keys");
      if(fetch != null)
      {
        fkd.timeLimitFetch = parse(fetch.get("time-limit-days"), fkd.timeLimitFetch);
        List lsSQL = (List) fetch.get("sql");
        for(Object esq : lsSQL)
        {
          String tmp = okStr(esq);
          if(!tmp.isEmpty())
            fkd.sqlFilterFetch.add(tmp);
        }
      }
    }

    {
      Map compare = (Map) ef.get("compare-keys");
      if(compare != null)
      {
        fkd.timeLimitCompare = parse(compare.get("time-limit-days"), fkd.timeLimitCompare);
        List lsSQL = (List) compare.get("sql");
        for(Object esq : lsSQL)
        {
          String tmp = okStr(esq);
          if(!tmp.isEmpty())
            fkd.sqlFilterCompare.add(tmp);
        }
      }
    }

    return fkd;
  }

  public static Map formatFilterKeyData(FilterKeyData fk, Map m)
     throws SyncSetupErrorException
  {
    if(fk == null)
      return m;

    HashtableRpc mps = new HashtableRpc();
    m.put("sql-filter", mps);

    HashtableRpc fetch = new HashtableRpc();
    fetch.put("time-limit-days", fk.timeLimitFetch);
    fetch.put("sql", fk.sqlFilterFetch);
    mps.put("fetch-keys", fetch);

    HashtableRpc compare = new HashtableRpc();
    compare.put("time-limit-days", fk.timeLimitCompare);
    compare.put("sql", fk.sqlFilterCompare);
    mps.put("compare-keys", compare);

    return m;
  }

  public static String convertValue(Object valore, String tipo)
  {
    if(Utils.isNumeric(tipo))
      return valore.toString();
    else
      return "'" + CvtSQLstring(valore.toString()) + "'";
  }

  public static String convertValue(Object valore, Pair<String, String> field)
  {
    if(Utils.isNumeric(field.second))
      return valore.toString();
    else
      return "'" + valore.toString() + "'";
  }

  /**
   * Converte chiave trasferita.
   * Da una chiave nella forma 'valore1^valore2^valore3' estre la relativa
   * mappa campo1=valore1, campo2=valore2, campo3=valore3.
   * @param key chiave trasferita
   * @param arKeys definizione delle chiavi primarie
   * @return mappa nome campo/valore campo corrispondente alla chiave
   * @throws Exception
   */
  public static Map<String, String> reverseKey(String key, Map<String, String> arKeys)
     throws Exception
  {
    Map<String, String> rv = new ArrayMap<>();
    if(arKeys == null || arKeys.isEmpty())
      throw new SyncErrorException("MISSING PARAMETER: 'arKeys == null || arKeys.isEmpty()'");

    if(arKeys.size() == 1)
    {
      // ottimizzazione per campo singolo
      Map.Entry<String, String> campo = arKeys.entrySet().iterator().next();
      rv.put(campo.getKey(), key);
      return rv;
    }

    String[] ss = split(key, '^');
    if(ss.length != arKeys.size())
      return null;

    int i = 0;
    for(Map.Entry<String, String> entry : arKeys.entrySet())
    {
      String keyVal = entry.getKey();
      String tipVal = entry.getValue();
      rv.put(keyVal, ss[i++].replace('|', '^'));
    }

    return rv;
  }
}
