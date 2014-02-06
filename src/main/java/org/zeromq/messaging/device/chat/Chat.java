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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.ZmqAbstractRunnableContext;

public final class Chat extends ZmqAbstractRunnableContext {

  private static final Logger LOG = LoggerFactory.getLogger(Chat.class);

  public static final class Builder extends ZmqAbstractRunnableContext.Builder<Builder, Chat> {

    public Builder() {
      super(new Chat());
    }

    public Builder withFrontendPubProps(Props frontendPubProps) {
      _target.setFrontendPubProps(frontendPubProps);
      return this;
    }

    public Builder withClusterPubProps(Props clusterPubProps) {
      _target.setClusterPubProps(clusterPubProps);
      return this;
    }

    public Builder withFrontendSubProps(Props frontendSubProps) {
      _target.setFrontendSubProps(frontendSubProps);
      return this;
    }

    public Builder withClusterSubProps(Props clusterSubProps) {
      _target.setClusterSubProps(clusterSubProps);
      return this;
    }
  }

  private Props frontendPubProps;
  private Props clusterPubProps;
  private Props frontendSubProps;
  private Props clusterSubProps;

  /**
   * XSUB -- for serving connecting publishers:
   * <pre>
   *       byte[] <--      <--conn-- PUB
   *                  XSUB <--conn-- PUB
   *    sub/unsub -->      <--conn-- PUB
   * </pre>
   */
  private ZmqChannel _frontendPub;
  /**
   * XPUB -- for serving cluster wide subscribers:
   * <pre>
   *      byte[] -->      <--conn-- cluster XSUB
   *                 XPUB <--conn-- cluster XSUB
   *   sub/unsub <--      <--conn-- cluster XSUB
   * </pre>
   */
  private ZmqChannel _clusterPub;
  /**
   * XPUB -- for serving connecting subscribers:
   * <pre>
   *    sub/unsub <--      <--conn-- SUB
   *                  XPUB <--conn-- SUB
   *       byte[] -->      <--conn-- SUB
   * </pre>
   */
  private ZmqChannel _frontendSub;
  /**
   * XSUB -- for serving cluster wide publishers:
   * <pre>
   *      byte[] <--      --conn--> cluster XPUB
   *                 XSUB --conn--> cluster XPUB
   *   sub/unsub -->      --conn--> cluster XPUB
   * </pre>
   */
  private ZmqChannel _clusterSub;

  //// METHDOS

  public static Builder builder() {
    return new Builder();
  }

  public void setFrontendPubProps(Props frontendPubProps) {
    this.frontendPubProps = frontendPubProps;
  }

  public void setClusterPubProps(Props clusterPubProps) {
    this.clusterPubProps = clusterPubProps;
  }

  public void setFrontendSubProps(Props frontendSubProps) {
    this.frontendSubProps = frontendSubProps;
  }

  public void setClusterSubProps(Props clusterSubProps) {
    this.clusterSubProps = clusterSubProps;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (frontendPubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (clusterPubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (frontendSubProps.bindAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
    if (clusterSubProps.connectAddr().isEmpty()) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    checkInvariant();

    reg(_frontendPub = ZmqChannel.XSUB(ctx).withProps(frontendPubProps).build());
    reg(_clusterPub = ZmqChannel.XPUB(ctx).withProps(clusterPubProps).build());
    reg(_frontendSub = ZmqChannel.XPUB(ctx).withProps(frontendSubProps).build());
    reg(_clusterSub = ZmqChannel.XSUB(ctx).withProps(clusterSubProps).build());

    // By default, unconditionally, Chat is set to handle duplicate subscriptions/unsubscriptions.
    _clusterPub.setExtendedPubSubVerbose();
    _frontendSub.setExtendedPubSubVerbose();

    _frontendPub.watchRecv(_poller);
    _clusterPub.watchRecv(_poller);
    _frontendSub.watchRecv(_poller);
    _clusterSub.watchRecv(_poller);
  }

  @Override
  public void execute() {
    super.execute();

    if (_frontendPub.canRecv()) {
      _clusterPub.send(_frontendPub.recv());
      LOG.debug("Message: local --> cluster.");
    }
    if (_clusterPub.canRecv()) {
      ZmqMessage message = _clusterPub.recv();
      _frontendPub.send(message);
      if (message.isSubscribe()) {
        LOG.debug("Subscribe: local <-- cluster.");
      }
      else if (message.isUnsubscribe()) {
        LOG.debug("Unsubsribe: local <-- cluster.");
      }
    }
    if (_clusterSub.canRecv()) {
      _frontendSub.send(_clusterSub.recv());
      LOG.debug("Message: local <-- cluster.");
    }
    if (_frontendSub.canRecv()) {
      ZmqMessage message = _frontendSub.recv();
      _clusterSub.send(message);
      if (message.isSubscribe()) {
        LOG.debug("Subscribe: local --> cluster.");
      }
      else if (message.isUnsubscribe()) {
        LOG.debug("Unsubsribe: local --> cluster.");
      }
    }
  }
}
