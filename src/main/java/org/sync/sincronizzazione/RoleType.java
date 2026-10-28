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
 * Tipi di ruolo.
 * Puo essere solo MASTER, SLAVE.
 *
 * @author Nicola De Nisco
 */
public enum RoleType
{
  MASTER("master"),
  SLAVE("slave");

  private final String role;

  RoleType(String value)
  {
    this.role = value;
  }

  public String getRole()
  {
    return role;
  }

  public static RoleType fromString(String value)
  {
    for(RoleType type : values())
    {
      if(type.role.equalsIgnoreCase(value))
        return type;
    }

    throw new IllegalArgumentException("Tipo di ruolo non valido: " + value);
  }

  @Override
  public String toString()
  {
    return role;
  }
}
