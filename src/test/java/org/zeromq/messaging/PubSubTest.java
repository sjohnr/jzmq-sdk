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

package org.zeromq.messaging;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubTest extends ZmqAbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(PubSubTest.class);

  static class Fixture {

    final ZmqContext zmqContext;

    Fixture(ZmqContext zmqContext) {
      this.zmqContext = zmqContext;
    }

    ZmqChannel newPublisher() {
      return ZmqChannel.builder()
                       .withCtx(zmqContext)
                       .ofPUBType()
                       .withProps(Props.builder()
                                       .withBindAddress(inprocAddr("p"))
                                       .build())
                       .build();
    }

    ZmqChannel newSubscriber() {
      return ZmqChannel.builder()
                       .withCtx(zmqContext)
                       .ofSUBType()
                       .withProps(Props.builder()
                                       .withConnectAddress(inprocAddr("p"))
                                       .build())
                       .build();
    }
  }

  @Test
  public void t0() throws InterruptedException {
    LOG.info("Testing .send()/.recv() .");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      byte[] topicA = "A".getBytes();
      byte[] topicB = "B".getBytes();
      byte[] topicC = "C".getBytes();

      subscriber.subscribe(topicA);
      subscriber.subscribe(topicB);
      subscriber.subscribe(topicC);

      // send message with topic A.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicA).build());
      // send message with topic B.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicB).build());
      // send message with topic C.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicC).build());

      // send other messages.
      for (int i = 0; i < 3; i++) {
        publisher.send(HELLO());
      }

      // receive three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // assert that other messages can't be received because of topic subscription.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }

  @Test
  public void t1() throws InterruptedException {
    LOG.info("Testing subscribtions and .send()/.recv() .");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      byte[] topic = "topic".getBytes();
      // subscribe just once.
      subscriber.subscribe(topic);

      // send messages with topic.
      for (int i = 0; i < 4; i++) {
        publisher.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      }

      // receive only three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // assert that messages can't be received.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }

  @Test
  public void t2() throws InterruptedException {
    LOG.info("Testing subscribtions.");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      // subscribe many times.
      int numOfSubscr = 17;
      // topic.
      byte[] topic = "topic".getBytes();
      for (int i = 0; i < numOfSubscr; i++) {
        subscriber.subscribe(topic);
      }

      // send messages with topic.
      for (int i = 0; i < 6; i++) {
        publisher.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      }

      // receive only three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // check that you can get messages because subscriptions are there yet.
      assert subscriber.recv() != null;
      // unsubscribe again.
      subscriber.unsubscribe(topic);
      // check again.
      assert subscriber.recv() != null;
      // unsubscribe fully.
      for (int i = 0; i < numOfSubscr; i++) {
        subscriber.unsubscribe(topic);
      }
      // assert that messages can't be received.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }
}
