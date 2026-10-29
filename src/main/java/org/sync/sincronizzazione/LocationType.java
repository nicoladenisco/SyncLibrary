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
package org.sync.sincronizzazione;

/**
 * Tipo di funzione.
 * Puo essere solo LOCAL, FOREIGN.
 *
 * @author Nicola De Nisco
 */
public enum LocationType
{
  LOCAL("local"),
  FOREIGN("foreign");

  private final String function;

  LocationType(String value)
  {
    this.function = value;
  }

  public String getFunction()
  {
    return function;
  }

  public static LocationType fromString(String value)
  {
    for(LocationType type : values())
    {
      if(type.function.equalsIgnoreCase(value))
        return type;
    }

    throw new IllegalArgumentException("Tipo di funzione non valido: " + value);
  }

  @Override
  public String toString()
  {
    return function;
  }

  public LocationType getOpposite()
  {
    switch(this)
    {
      case LOCAL:
        return FOREIGN;

      case FOREIGN:
        return LOCAL;

      default:
        throw new IllegalArgumentException("Valore non definito: " + this);
    }
  }
}
