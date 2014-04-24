/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging.device.chat;

import org.zeromq.messaging.BaseFixture;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.support.thread.ZmqProcess;

public class ChatFixture extends BaseFixture {

  private final ZmqContext ctx;

  public ChatFixture(ZmqContext ctx) {
    this.ctx = ctx;
  }

  void chat(String frontendPub,
            String clusterPub,
            String frontendSub,
            String clusterPubConnAddr) {
    with(
        ZmqProcess.builder()
                  .withActor(
                      Chat.builder()
                          .withCtx(ctx)
                          .withPollTimeout(100)
                          .withFrontendPubProps(Props.builder().withBindAddr(frontendPub).build())
                          .withClusterPubProps(Props.builder().withBindAddr(clusterPub).build())
                          .withFrontendSubProps(Props.builder().withBindAddr(frontendSub).build())
                          .withClusterSubProps(Props.builder().withConnectAddr(clusterPubConnAddr).build())
                          .build()
                  )
                  .build()
    );
  }
}
