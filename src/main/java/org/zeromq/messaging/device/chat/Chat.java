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

    public Builder withLocalPublisherProps(Props localPublisherProps) {
      _target.localPublisherProps = localPublisherProps;
      return this;
    }

    public Builder withClusterPublisherProps(Props clusterPublisherProps) {
      _target.clusterPublisherProps = clusterPublisherProps;
      return this;
    }

    public Builder withLocalSubscriberProps(Props localSubscriberProps) {
      _target.localSubscriberProps = localSubscriberProps;
      return this;
    }

    public Builder withClusterSubscriberProps(Props clusterSubscriberProps) {
      _target.clusterSubscriberProps = clusterSubscriberProps;
      return this;
    }
  }

  private Props localPublisherProps;
  private Props clusterSubscriberProps;
  private Props localSubscriberProps;
  private Props clusterPublisherProps;

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

  public void setLocalPublisherProps(Props localPublisherProps) {
    this.localPublisherProps = localPublisherProps;
  }

  public void setClusterPublisherProps(Props clusterPublisherProps) {
    this.clusterPublisherProps = clusterPublisherProps;
  }

  public void setLocalSubscriberProps(Props localSubscriberProps) {
    this.localSubscriberProps = localSubscriberProps;
  }

  public void setClusterSubscriberProps(Props clusterSubscriberProps) {
    this.clusterSubscriberProps = clusterSubscriberProps;
  }

  @Override
  public void init() {
    if (localPublisherProps == null) {
      throw ZmqException.fatal();
    }
    if (clusterPublisherProps == null) {
      throw ZmqException.fatal();
    }
    if (localSubscriberProps == null) {
      throw ZmqException.fatal();
    }
    if (clusterSubscriberProps == null) {
      throw ZmqException.fatal();
    }

    register(_localPublisher = ZmqChannel.builder()
                                         .withZmqContext(zmqContext)
                                         .ofXSUBType()
                                         .withProps(localPublisherProps)
                                         .build());

    register(_clusterPublisher = ZmqChannel.builder()
                                            .withZmqContext(zmqContext)
                                            .ofXPUBType()
                                            .withProps(clusterPublisherProps)
                                            .build());

    register(_localSubscriber = ZmqChannel.builder()
                                          .withZmqContext(zmqContext)
                                          .ofXPUBType()
                                          .withProps(localSubscriberProps)
                                          .build());

    register(_clusterSubscriber = ZmqChannel.builder()
                                           .withZmqContext(zmqContext)
                                           .ofXSUBType()
                                           .withProps(clusterSubscriberProps)
                                           .build());

    _poller = zmqContext.newPoller(4);

    _localPublisher.watchRecv(_poller);
    _clusterPublisher.watchRecv(_poller);
    _localSubscriber.watchRecv(_poller);
    _clusterSubscriber.watchRecv(_poller);
  }

  @Override
  public void exec() {
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
