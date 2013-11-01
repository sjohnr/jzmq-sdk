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

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.zeromq.TestRecorder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.HELLO;

public class PubSubTest extends ZmqAbstractTest {

  static class Fixture {

    final ZmqContext zmqContext;

    Fixture(ZmqContext zmqContext) {
      this.zmqContext = zmqContext;
    }

    ZmqChannel newPublisher() {
      return ZmqChannelFactory.builder()
                              .withZmqContext(zmqContext)
                              .ofPUBType()
                              .withBindAddress("inproc://p")
                              .build()
                              .newChannel();
    }

    ZmqChannel newSubscriber() {
      return ZmqChannelFactory.builder()
                              .withZmqContext(zmqContext)
                              .ofSUBType()
                              .withConnectAddress("inproc://p")
                              .build()
                              .newChannel();
    }
  }

  @Test
  public void t0() throws InterruptedException {
    Fixture f = new Fixture(zmqContext());
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
    Fixture f = new Fixture(zmqContext());
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
    Fixture f = new Fixture(zmqContext());
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

  @Test
  public void t3_perf() {
    TestRecorder r = new TestRecorder();
    r.log("Perf test for PUB/SUB. Sending messages via PUB w/o subscriber.");

    Fixture f = new Fixture(zmqContext());
    ZmqChannel publisher = f.newPublisher();

    try {
      int ITER = 500;
      int MESSAGE_NUM = 1000;
      ZmqMessage message = ZmqMessage.builder(HELLO())
                                     .withTopic("xyz".getBytes())
                                     .build();
      Stopwatch timer = new Stopwatch().start();
      for (int j = 0; j < ITER; j++) {
        for (int i = 0; i < MESSAGE_NUM; i++) {
          assert publisher.send(message);
        }
      }
      r.logQoS((ITER * MESSAGE_NUM) / timer.stop().elapsedTime(MILLISECONDS), "messages/ms.");
    }
    finally {
      publisher.destroy();
    }
  }
}
