/*
 *  FieldFilterBean.java
 *  Creato il 29-ott-2021, 10.33.29
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
package it.infomed.sync;

import com.workingdogs.village.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;
import org.commonlib5.utils.StringOper;

/**
 * Informazioni di filtro su un campo.
 *
 * @author Nicola De Nisco
 */
public class FieldFilterBean
{
  public String fieldName, cmp;
  public List<String> values = new ArrayList<>();

  @Override
  public String toString()
  {
    return "FieldFilterBean{" + "fieldName=" + fieldName + ", cmp=" + cmp + ", values=" + values + '}';
  }

  /**
   * Verifica il record per questo filtro.
   * @param r record da verificare
   * @param type tipo del campo
   * @param defVal valore di default se il filtro non è applicabile
   * @return vero se il filtro è valido (record valido)
   * @throws Exception
   */
  public boolean testRecord(Record r, String type, boolean defVal)
     throws Exception
  {
    if(values.isEmpty())
      return defVal;

    switch(StringOper.okStr(type).toUpperCase())
    {
      default:
        return defVal;

      case "INTEGER":
        return testInteger(r, defVal);

      case "STRING":
        return testString(r, defVal);

      case "FLOAT":
      case "DOUBLE":
        return testDouble(r, defVal);
    }
  }

  /**
   * Verifica il record per questo filtro.
   * @param r record da verificare
   * @param type tipo del campo
   * @param defVal valore di default se il filtro non è applicabile
   * @return vero se il filtro è valido (record valido)
   * @throws Exception
   */
  public boolean testRecord(Map r, String type, boolean defVal)
     throws Exception
  {
    if(values.isEmpty())
      return defVal;

    switch(StringOper.okStr(type).toUpperCase())
    {
      default:
        return defVal;

      case "INTEGER":
        return testInteger(r, defVal);

      case "STRING":
        return testString(r, defVal);

      case "FLOAT":
      case "DOUBLE":
        return testDouble(r, defVal);
    }
  }

  protected boolean testInteger(Record r, boolean defVal)
     throws Exception
  {
    ArrayList<Integer> arInts = new ArrayList<>();
    MutableInt mi = new MutableInt();
    for(String val : values)
    {
      if(parse(val, mi))
        arInts.add(mi.intValue());
    }

    if(arInts.isEmpty())
      return defVal;

    int test = r.getValue(fieldName).asInt();
    return testInteger(defVal, arInts, test);
  }

  protected boolean testInteger(Map r, boolean defVal)
     throws Exception
  {
    ArrayList<Integer> arInts = new ArrayList<>();
    MutableInt mi = new MutableInt();
    for(String val : values)
    {
      if(parse(val, mi))
        arInts.add(mi.intValue());
    }

    if(arInts.isEmpty())
      return defVal;

    if(!parse(r.get(fieldName), mi))
      return defVal;

    int test = mi.intValue();
    return testInteger(defVal, arInts, test);
  }

  protected boolean testInteger(boolean defVal, ArrayList<Integer> arInts, int test)
  {
    switch(StringOper.okStr(cmp).toUpperCase())
    {
      default:
        return defVal;

      case "EQ":
      case "IN":
        for(int val : arInts)
        {
          if(test == val)
            return true;
        }
        return false;

      case "NE":
        for(int val : arInts)
        {
          if(!(test != val))
            return false;
        }
        break;

      case "GT":
        for(int val : arInts)
        {
          if(!(test > val))
            return false;
        }
        break;
      case "LT":
        for(int val : arInts)
        {
          if(!(test < val))
            return false;
        }
        break;
      case "GE":
        for(int val : arInts)
        {
          if(!(test >= val))
            return false;
        }
        break;
      case "LE":
        for(int val : arInts)
        {
          if(!(test <= val))
            return false;
        }
        break;

      case "RANGE":
        int n = arInts.size();
        if((n & 1) != 0)
          n--;

        if(n == 0)
          return defVal;

        for(int i = 0; i < n; i += 2)
        {
          int min = arInts.get(i);
          int max = arInts.get(i + 1);

          if(!(test >= min && test <= max))
            return false;
        }
        break;
    }

    return true;
  }

  protected boolean testString(Record r, boolean defVal)
     throws Exception
  {
    ArrayList<String> arStrings = new ArrayList<>();
    String tmp;
    for(String val : values)
    {
      if((tmp = StringOper.okStrNull(val)) != null)
        arStrings.add(tmp);
    }

    String test = r.getValue(fieldName).asString();
    return testString(defVal, arStrings, test);
  }

  protected boolean testString(Map r, boolean defVal)
     throws Exception
  {
    ArrayList<String> arStrings = new ArrayList<>();
    String tmp;
    for(String val : values)
    {
      if((tmp = StringOper.okStrNull(val)) != null)
        arStrings.add(tmp);
    }

    String test = StringOper.okStr(r.get(fieldName));
    return testString(defVal, arStrings, test);
  }

  protected boolean testString(boolean defVal, ArrayList<String> arStrings, String test)
  {
    switch(StringOper.okStr(cmp).toUpperCase())
    {
      default:
        return defVal;

      case "IN":
      case "EQ":
        for(String s : arStrings)
        {
          if(StringOper.isEqu(s, test))
            return true;
        }
        return false;
      case "NE":
        for(String s : arStrings)
        {
          if(StringOper.isEqu(s, test))
            return false;
        }
        break;

      case "GT":
        for(String s : arStrings)
        {
          int tc = StringOper.compare(s, test);
          if(!(tc > 0))
            return false;
        }
        break;
      case "LT":
        for(String s : arStrings)
        {
          int tc = StringOper.compare(s, test);
          if(!(tc < 0))
            return false;
        }
        break;
      case "GE":
        for(String s : arStrings)
        {
          int tc = StringOper.compare(s, test);
          if(!(tc >= 0))
            return false;
        }
        break;
      case "LE":
        for(String s : arStrings)
        {
          int tc = StringOper.compare(s, test);
          if(!(tc <= 0))
            return false;
        }
        break;

      case "RANGE":
        return defVal;
    }

    return true;
  }

  protected boolean testDouble(Record r, boolean defVal)
     throws Exception
  {
    ArrayList<Double> arDoubles = new ArrayList<>();
    MutableDouble mi = new MutableDouble();
    for(String val : values)
    {
      if(parse(val, mi))
        arDoubles.add(mi.doubleValue());
    }

    if(arDoubles.isEmpty())
      return defVal;

    double test = r.getValue(fieldName).asDouble();
    return testDouble(defVal, arDoubles, test);
  }

  protected boolean testDouble(Map r, boolean defVal)
     throws Exception
  {
    ArrayList<Double> arDoubles = new ArrayList<>();
    MutableDouble mi = new MutableDouble();
    for(String val : values)
    {
      if(parse(val, mi))
        arDoubles.add(mi.doubleValue());
    }

    if(arDoubles.isEmpty())
      return defVal;

    if(!parse(r.get(fieldName), mi))
      return defVal;

    double test = mi.doubleValue();
    return testDouble(defVal, arDoubles, test);
  }

  protected boolean testDouble(boolean defVal, ArrayList<Double> arDoubles, double test)
  {
    switch(StringOper.okStr(cmp).toUpperCase())
    {
      default:
        return defVal;

      case "EQ":
      case "IN":
        for(double val : arDoubles)
        {
          if(test == val)
            return true;
        }
        return false;

      case "NE":
        for(double val : arDoubles)
        {
          if(!(test != val))
            return false;
        }
        break;

      case "GT":
        for(double val : arDoubles)
        {
          if(!(test > val))
            return false;
        }
        break;
      case "LT":
        for(double val : arDoubles)
        {
          if(!(test < val))
            return false;
        }
        break;
      case "GE":
        for(double val : arDoubles)
        {
          if(!(test >= val))
            return false;
        }
        break;
      case "LE":
        for(double val : arDoubles)
        {
          if(!(test <= val))
            return false;
        }
        break;

      case "RANGE":
        int n = arDoubles.size();
        if((n & 1) != 0)
          n--;

        if(n == 0)
          return defVal;

        for(int i = 0; i < n; i += 2)
        {
          double min = arDoubles.get(i);
          double max = arDoubles.get(i + 1);

          if(!(test >= min && test <= max))
            return false;
        }
        break;
    }

    return true;
  }

  /**
   * Converte stringa in intero.
   * Ritorna il valore intero della stringa in base 10 senza
   * sollevare alcuna eccezione.
   * @param val un qualsiasi oggetto java
   * @param valout oggetto per memorizzare il risultato della conversione
   * @return vero se val è convertibile in un numero
   */
  public boolean parse(Object val, MutableInt valout)
  {
    try
    {
      valout.setValue(Integer.parseInt(StringOper.okStrNull(val)));
      return true;
    }
    catch(Exception e)
    {
      return false;
    }
  }

  /**
   * Converte stringa in doppia precisione.
   * Ritorna il valore doppia precisione della stringa in base 10 senza
   * sollevare alcuna eccezione.
   * @param val un qualsiasi oggetto java
   * @param valout oggetto per memorizzare il risultato della conversione
   * @return vero se val è convertibile in un numero
   */
  public boolean parse(Object val, MutableDouble valout)
  {
    try
    {
      valout.setValue(Double.parseDouble(StringOper.okStrNull(val)));
      return true;
    }
    catch(Exception e)
    {
      return false;
    }
  }
}
