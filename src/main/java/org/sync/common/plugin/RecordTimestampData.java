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
package org.sync.common.plugin;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import org.commonlib5.utils.ArrayMap;
import org.commonlib5.utils.Pair;

/**
 * Dati chiave/timestamp per aggiornamento.
 *
 * @author Nicola De Nisco
 */
public class RecordTimestampData extends ArrayMap<String, Timestamp>
{
  public RecordTimestampData()
  {
  }

  public RecordTimestampData(int size)
  {
    super(size);
  }

  public RecordTimestampData(Map<String, Timestamp> othermap)
  {
    super(othermap);
  }

  public RecordTimestampData(Collection<Pair<String, Timestamp>> pcoll)
  {
    super(pcoll);
  }

  public RecordTimestampData(Objects[] pairObjects)
  {
    super(pairObjects);
  }

  public void put(String key, Date ts)
  {
    super.put(key, new Timestamp(ts.getTime()));
  }
}
