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

import java.util.Date;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.commonlib5.utils.Pair;
import org.jdom2.Element;
import org.sync.SyncClientInterface;
import org.sync.common.SyncContext;

/**
 * Trasporto informazioni fra client e server.
 * In una implementazione reale questa classe usa qualche protocollo RPC (XML-RPC, REST, SOAP, ecc.).
 * Nel pacchetto di test chiama semplicemente le funzioni del server.
 *
 * @author Nicola De Nisco
 */
public class SyncClientTransport implements SyncClientInterface
{
  private final SyncServerTest server;
  private final SyncClientTest client;

  public SyncClientTransport(SyncServerTest server, SyncClientTest client)
  {
    this.server = server;
    this.client = client;
  }

  @Override
  public void acquisiciStrategia(String application, String target, SyncContext context)
     throws Exception
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Element acquisiciStrategia(String nomeRegola, SyncContext context)
     throws Exception
  {
    return client.acquisiciStrategia(nomeRegola, context);
  }

  @Override
  public void aggiornaBlocco(String nomeRegola, String nome, List<String> parametri, SyncContext context)
     throws Exception
  {
    server.aggiornaBlocco(nomeRegola, nome, parametri, context);
  }

  @Override
  public void pianificaAggiornamento(String nomeRegola, String nome, List<String> aggiorna, SyncContext context)
     throws Exception
  {
    server.pianificaAggiornamento(nomeRegola, nome, aggiorna, aggiorna, context);
  }

  @Override
  public void verificaBlocco(String nomeRegola, String nome, List<Pair<String, Date>> parametri, Date oldTimestamp, SyncContext context)
     throws Exception
  {
    server.verificaBlocco(nomeRegola, nome, parametri, oldTimestamp, context);
  }

  @Override
  public void beginRumbleRule(String nomeRegola, SyncContext context)
     throws Exception
  {
    server.inizioEsecuzioneRegola(nomeRegola, context);
  }

  @Override
  public void endRumbleRule(String nomeRegola, SyncContext context)
     throws Exception
  {
    server.fineEsecuzioneRegola(nomeRegola, context);
  }

  @Override
  public Configuration getConfiguration()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
