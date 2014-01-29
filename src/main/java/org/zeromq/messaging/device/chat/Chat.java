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
  private Props clusterSubProps;
  private Props frontendSubProps;
  private Props clusterPubProps;

  /**
   * XSUB -- for serving local multithreaded publishers:
   * <pre>
   *   thread PUB --->
   *   thread PUB ---> L -->
   *   thread PUB --->
   * </pre>
   */
  private ZmqChannel _localPublisher;
  /**
   * XPUB -- for serving cluster wide subscribers:
   * <pre>
   *         <--- cluster SUB
   *   --> C <--- cluster SUB
   *         <--- cluster SUB
   * </pre>
   */
  private ZmqChannel _clusterPublisher;
  /**
   * XPUB -- for serving local multithreaded subscribers:
   * <pre>
   *  thread SUB <---
   *  thread SUB <--- L -->
   *  thread SUB <---
   * </pre>
   */
  private ZmqChannel _localSubscriber;
  /**
   * XSUB -- for serving cluster wide publishers:
   * <pre>
   *        ---> cluster PUB
   *  <-- C ---> cluster PUB
   *        ---> cluster PUB
   * </pre>
   */
  private ZmqChannel _clusterSubscriber;

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
  public void init() {
    reg(_localPublisher = ZmqChannel.builder()
                                    .withCtx(ctx)
                                    .ofXSUBType()
                                    .withProps(frontendPubProps)
                                    .build());

    reg(_clusterPublisher = ZmqChannel.builder()
                                      .withCtx(ctx)
                                      .ofXPUBType()
                                      .withProps(clusterPubProps)
                                      .build());

    reg(_localSubscriber = ZmqChannel.builder()
                                     .withCtx(ctx)
                                     .ofXPUBType()
                                     .withProps(frontendSubProps)
                                     .build());

    reg(_clusterSubscriber = ZmqChannel.builder()
                                       .withCtx(ctx)
                                       .ofXSUBType()
                                       .withProps(clusterSubProps)
                                       .build());

    _localPublisher.watchRecv(_poller);
    _clusterPublisher.watchRecv(_poller);
    _localSubscriber.watchRecv(_poller);
    _clusterSubscriber.watchRecv(_poller);
  }

  @Override
  public void execute() {
    super.execute();

    if (_localPublisher.canRecv()) {
      _clusterPublisher.send(_localPublisher.recv());
      LOG.trace("Handle --> traffic: local publisher(s) send message to the cluster.");
    }
    if (_clusterSubscriber.canRecv()) {
      _localSubscriber.send(_clusterSubscriber.recv());
      LOG.trace("Handle <-- traffic: cluster wide publisher(s) send message to this socket.");
    }

    if (_localSubscriber.canRecv()) {
      _clusterSubscriber.send(_localSubscriber.recv());
      LOG.trace("Handle --> subscriptions: server local subscribers, forward their subscriptions.");
    }
    if (_clusterPublisher.canRecv()) {
      _localPublisher.send(_clusterPublisher.recv());
      LOG.trace("Handle <-- subscriptions: serve cluster subscribers, forward their subscriptions.");
    }
  }
}
