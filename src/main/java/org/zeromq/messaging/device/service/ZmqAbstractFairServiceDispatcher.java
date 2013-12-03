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

package org.zeromq.messaging.device.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqMessage;

public abstract class ZmqAbstractFairServiceDispatcher extends ZmqAbstractServiceDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqAbstractFairServiceDispatcher.class);

  @Override
  public final void exec() {
    // ==== handle backend traffic first ====
    if (_backend.canRecv()) {
      ZmqMessage message = _backend.recv();
      if (message == null) {
        LOG.error(".recv() failed on backend!");
        return;
      }

      boolean sent = _frontend.send(message);
      if (!sent) {
        LOG.warn(".send() failed on frontend!");
      }
    }

    // ==== handle frontend traffic second ====
    if (_frontend.canRecv()) {
      ZmqMessage message = _frontend.recv();
      if (message == null) {
        LOG.error(".recv() failed on frontend!");
        return;
      }

      boolean sent = _backend.send(message);
      if (!sent) {
        LOG.info("Can't send message on backend. Asking to TRY_AGAIN ...");
        boolean sentTryAgain = _frontend.send(ZmqMessage.builder(message)
                                                        .withHeaders(new ServiceHeaders()
                                                                         .copy(message.headers())
                                                                         .setMsgTypeTryAgain())
                                                        .build());
        if (sentTryAgain) {
          LOG.info("Asked to TRY_AGAIN.");
        }
        else {
          LOG.warn("Didn't send TRY_AGAIN!");
        }
      }
    }
  }
}
