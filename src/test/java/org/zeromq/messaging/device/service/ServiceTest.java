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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.Checker;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;

import java.util.ArrayList;
import java.util.Collection;

import static junit.framework.Assert.assertEquals;
import static org.zeromq.messaging.device.service.ServiceFixture.Answering;

public class ServiceTest extends ZmqAbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(ServiceTest.class);

  @Test
  public void t0() {
    LOG.info(
        "\n" +
        "********************************************************** \n" +
        "                                                           \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> NOT_AVAIL.             \n" +
        "                                                           \n" +
        "                          LRU                              \n" +
        "----------         -----------------         ------------- \n" +
        "|        | ------> |               | ---X--> |           | \n" +
        "| DEALER |         | R(333)-R(444) |         | NOT_AVAIL | \n" +
        "|        | <------ |               | <--X--- |           | \n" +
        "----------         -----------------         ------------- \n" +
        "                                                           \n" +
        "********************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());
      f.workerAcceptor(zmqContext(), new Answering(SHIRT()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    int MESSAGE_NUM = 10;
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t1() {
    LOG.info(
        "\n" +
        "******************************************************** \n" +
        "                                                         \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> DEALER.              \n" +
        "                                                         \n" +
        "                          LRU                            \n" +
        "----------         -----------------         ----------  \n" +
        "|        | ------> |               | ------> |        |  \n" +
        "| DEALER |         | R(333)-R(444) |         | DEALER |  \n" +
        "|        | <------ |               | <------ |        |  \n" +
        "----------         -----------------         ----------  \n" +
        "                                                         \n" +
        "******************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t2() {
    LOG.info(
        "\n" +
        "******************************************************** \n" +
        "                                                         \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> DEALER.              \n" +
        "                                                         \n" +
        "                          LRU                            \n" +
        "----------         -----------------         ----------  \n" +
        "|        | ------> |               | ------> |        |  \n" +
        "| DEALER |         | R(333)-R(444) |         | DEALER |  \n" +
        "|        | <------ |               | <------ |        |  \n" +
        "----------         -----------------         ----------  \n" +
        "                     TTL is small                        \n" +
        "                                                         \n" +
        "******************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.volatileLruCache());
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t3() {
    LOG.info(
        "\n" +
        "*************************************************************** \n" +
        "                                                                \n" +
        "Test DEALER(retry=true) <--> ROUTER-ROUTER <--> DEALER.         \n" +
        "                                                                \n" +
        "                          LRU                                   \n" +
        "----------         -----------------         ----------         \n" +
        "|        | ------> |               | ---X--> |        |         \n" +
        "| DEALER |         | R(333)-R(444) |         | DEALER |         \n" +
        "|        | <------ |               | <------ |        |         \n" +
        "----------         -----------------         ----------         \n" +
        "                  not matched socket_id                         \n" +
        "                                                                \n" +
        "*************************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.notMatchingLruCache());
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t4() {
    LOG.info(
        "\n" +
        "******************************************************************************************* \n" +
        "                                                                                            \n" +
        "Test DEALER(retry=true) <--> ROUTER-DEALER <--> ROUTER-ROUTER <--> DEALER.                  \n" +
        "                                                                                            \n" +
        "                           gateway                            LRU                           \n" +
        "----------         --------------------------          -----------------         ---------- \n" +
        "|        | ------> |                        | -------> |               | ---X--> |        | \n" +
        "| DEALER |         | R(inproc)-D(conn->333) |          | R(333)-R(444) |         | DEALER | \n" +
        "|        | <------ |                        | <------- |               | <------ |        | \n" +
        "----------         --------------------------          -----------------         ---------- \n" +
        "                                                      not matched socket_id                 \n" +
        "                                                                                            \n" +
        "******************************************************************************************* \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairEmitter(zmqContext(), "inproc://gateway", "tcp://localhost:" + 333);
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.notMatchingLruCache());
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "inproc://gateway");
    client.lease();
    int MESSAGE_NUM = 10;
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t5() throws InterruptedException {
    LOG.info(
        "\n" +
        "*********************************************************** \n" +
        "                                                            \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> [DEALER].               \n" +
        "                                                            \n" +
        "                          LRU                               \n" +
        "----------         -----------------         ----------     \n" +
        "|        | ------> |               | ------> |        |     \n" +
        "| DEALER |         | R(333)-R(444) |         | DEALER |--   \n" +
        "|        | <------ |               | <------ |        | |   \n" +
        "----------         -----------------         ---------- |-- \n" +
        "                                               |        | | \n" +
        "                                               ---------- | \n" +
        "                                                 |        | \n" +
        "                                                 ---------- \n" +
        "                                                            \n" +
        "*********************************************************** \n");

    final ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    Runnable client = new Runnable() {
      public void run() {
        SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
        client.lease();
        Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
        for (int i = 0; i < MESSAGE_NUM; i++) {
          assert client.send(HELLO());
          ZmqMessage reply = client.recv();
          assertPayload("world", reply);
          replies.add(reply);
        }
        client.release();
        assertEquals(MESSAGE_NUM, replies.size());
      }
    };

    Checker checker = new Checker();

    Thread t0 = new Thread(client);
    t0.setUncaughtExceptionHandler(checker);
    t0.setDaemon(true);
    t0.start();

    Thread t1 = new Thread(client);
    t1.setUncaughtExceptionHandler(checker);
    t1.setDaemon(true);
    t1.start();

    Thread t2 = new Thread(client);
    t2.setUncaughtExceptionHandler(checker);
    t2.setDaemon(true);
    t2.start();

    try {
      t0.join();
      t1.join();
      t2.join();
      assert checker.passed();
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t6() {
    LOG.info(
        "\n" +
        "********************************************************************************************** \n" +
        "                                                                                               \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> DEALER-ROUTER <--> [...] <--> [DEALER].                    \n" +
        "                                                                                               \n" +
        "                      LRU               Dispatcher i-th        Dispatcher i-th                 \n" +
        "----------      -----------------      ----------------       ----------------      ---------- \n" +
        "|        | ---> |               | ---> |               | ---> |              | ---> |        | \n" +
        "| DEALER |      | R(333)-R(444) |      | D(...)-R(...) |      |     ...      |      | DEALER | \n" +
        "|        | <--- |               | <--- |               | <--- |              | <--- |        | \n" +
        "----------      -----------------      -----------------      ----------------      ---------- \n" +
        "                                                                                               \n" +
        "********************************************************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());
      f.fairActiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
      f.fairActiveAcceptor(zmqContext(), "tcp://localhost:" + 555, "tcp://*:" + 666);
      f.workerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 666);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t7() throws InterruptedException {
    LOG.info(
        "\n" +
        "******************************************************** \n" +
        "                                                         \n" +
        "Test DEALER <--> ROUTER-ROUTER <--> DEALER.              \n" +
        "                                                         \n" +
        "                          LRU                            \n" +
        "----------         -----------------         ----------  \n" +
        "|        | ------> |               | ------> |        |  \n" +
        "| DEALER |         | R(333)-R(444) |         | DEALER |  \n" +
        "|        | <------ |               | <------ |        |  \n" +
        "----------         -----------------         ----------  \n" +
        "                  with custom routing                    \n" +
        "                                                         \n" +
        "******************************************************** \n");

    final ServiceFixture f = new ServiceFixture();
    {
      f.lruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.matchingLRUCache());
      f.workerEmitterWithIdentity(zmqContext(),
                                  "X",
                                  new Answering(SHIRT()),
                                  "tcp://localhost:" + 444);
      f.workerEmitterWithIdentity(zmqContext(),
                                  "Y",
                                  new Answering(CARP()),
                                  "tcp://localhost:" + 444);
    }
    f.init();

    Checker checker = new Checker();

    Thread t0 = new Thread(
        new Runnable() {
          @Override
          public void run() {
            SyncClient client = f.newConnClientWithIdentity(zmqContext(), "X", "tcp://localhost:" + 333);
            client.lease();
            Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
            for (int i = 0; i < MESSAGE_NUM; i++) {
              assert client.send(HELLO());
              ZmqMessage reply = client.recv();
              assertPayload("shirt", reply);
              replies.add(reply);
            }
            client.release();
            assertEquals(MESSAGE_NUM, replies.size());
          }
        }
    );
    t0.setUncaughtExceptionHandler(checker);
    t0.setDaemon(true);
    t0.start();

    Thread t1 = new Thread(
        new Runnable() {
          @Override
          public void run() {
            SyncClient client = f.newConnClientWithIdentity(zmqContext(), "Y", "tcp://localhost:" + 333);
            client.lease();
            Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
            for (int i = 0; i < MESSAGE_NUM; i++) {
              assert client.send(HELLO());
              ZmqMessage reply = client.recv();
              assertPayload("carp", reply);
              replies.add(reply);
            }
            client.release();
            assertEquals(MESSAGE_NUM, replies.size());
          }
        }
    );
    t1.setUncaughtExceptionHandler(checker);
    t1.setDaemon(true);
    t1.start();

    try {
      t0.join();
      t1.join();
      assert checker.passed();
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t8() {
    LOG.info(
        "\n" +
        "************************************************************ \n" +
        "                                                             \n" +
        "Test DEALER <--> ROUTER-DEALER <--> [ROUTER].                \n" +
        "                                                             \n" +
        "                                                 ----------  \n" +
        "                                                 |        |  \n" +
        "                         FAIR          __>>__<<__| ROUTER |  \n" +
        "----------         -----------------  /          |        |  \n" +
        "|        | ------> |               | /           ----------  \n" +
        "| DEALER |         | R(333)-D(444) |/                        \n" +
        "|        | <------ |               |\\            ---------- \n" +
        "----------         ----------------- \\           |        | \n" +
        "                                      \\__>>__<<__| ROUTER | \n" +
        "                                                 |        |  \n" +
        "                                                 ---------   \n" +
        "                                                             \n" +
        "************************************************************ \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);
      f.workerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
      f.workerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t9() {
    LOG.info(
        "\n" +
        "********************************************************************************************** \n" +
        "                                                                                               \n" +
        "Test DEALER <--> ROUTER-DEALER <--> ROUTER-DEALER <--> [ROUTER].                               \n" +
        "                                                                                               \n" +
        "                                                                                   ----------  \n" +
        "                                                                                   |        |  \n" +
        "                         FAIR                    FAIR_PASSIVE_MAMA       __>>__<<__| ROUTER |  \n" +
        "----------         -----------------          -----------------------   /          |        |  \n" +
        "|        | ------> |               | -------> |                     |  /           ----------  \n" +
        "| DEALER |         | R(333)-D(444) |          | R(conn->444)-D(555) | /                        \n" +
        "|        | <------ |               | <------- |                     | \\            ---------- \n" +
        "----------         -----------------          -----------------------  \\           |        | \n" +
        "                                                                        \\__>>__<<__| ROUTER | \n" +
        "                                                                                   |        |  \n" +
        "                                                                                   ----------  \n" +
        "                                                                                               \n" +
        "********************************************************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);
      f.fairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
      f.workerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 555);
      f.workerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 555);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t10() {
    LOG.info(
        "\n" +
        "****************************************************** \n" +
        "                                                       \n" +
        "Test DEALER <--> [ROUTER-DEALER] <--> ROUTER.          \n" +
        "                                                       \n" +
        "                       FAIR                            \n" +
        "                 -----------------                     \n" +
        "             ___>| R(555)-D(666) | <__                 \n" +
        "----------  /    -----------------     \\  ----------  \n" +
        "|        | /                            \\ |        |  \n" +
        "| DEALER |/                              \\| ROUTER |  \n" +
        "|        | \\           FAIR             / |        |  \n" +
        "----------  \\    -----------------     /  ----------  \n" +
        "             \\__>| R(556)-D(667) |<___/               \n" +
        "                 -----------------                     \n" +
        "                                                       \n" +
        "                                                       \n" +
        "****************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairRouter(zmqContext(), "tcp://*:" + 555, "tcp://*:" + 666);
      f.fairRouter(zmqContext(), "tcp://*:" + 556, "tcp://*:" + 667);
      f.workerAcceptor(zmqContext(),
                       new Answering(WORLD()),
                       "tcp://localhost:" + 666,
                       "tcp://localhost:" + 667);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 555,
                                        "tcp://localhost:" + 556);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t11() {
    LOG.info(
        "\n" +
        "****************************************************** \n" +
        "                                                       \n" +
        "                 [  NOT_AVAIL  ]                       \n" +
        "Test DEALER <--> [ROUTER-DEALER] <--> ROUTER.          \n" +
        "                 [  NOT_AVAIL  ]                       \n" +
        "                                                       \n" +
        "                 -----------------                     \n" +
        "             ___>|   NOT_AVAIL   | <__                 \n" +
        "----------  /    -----------------     \\  ----------  \n" +
        "|        | /     -----------------      \\ |        |  \n" +
        "| DEALER |/----->| R(555)-D(666) |<------\\| ROUTER |  \n" +
        "|        | \\     ----------------       / |        |  \n" +
        "----------  \\    -----------------     /  ----------  \n" +
        "             \\__>|  NOT_AVAIL    |<___/               \n" +
        "                 -----------------                     \n" +
        "                                                       \n" +
        "                                                       \n" +
        "****************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairRouter(zmqContext(), "tcp://*:" + 555, "tcp://*:" + 666);
      // Create worker connected at all HUBs' backends.
      // NOT: there will be only one LIVE HUB.
      f.workerAcceptor(zmqContext(),
                       new Answering(WORLD()),
                       NOT_AVAIL_0,
                       "tcp://localhost:" + 666,
                       NOT_AVAIL_1);
    }
    f.init();

    int HWM = 1;
    int NUM_OF_NOTAVAIL = 2;
    SyncClient client = SyncClient.builder()
                                  .withChannelBuilder(
                                      ZmqChannel.builder()
                                                .withZmqContext(zmqContext())
                                                .ofDEALERType()
                                                .withHwmForSend(HWM)
                                                .withConnectAddress(NOT_AVAIL_0)
                                                .withConnectAddress("tcp://localhost:" + 555)
                                                .withConnectAddress(NOT_AVAIL_1))
                                  .build();
    client.lease();
    int MESSAGE_NUM = 10 * HWM; // number of messages -- several times bigger than HWM.
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        if (reply != null) {
          assertPayload("world", reply);
          replies.add(reply);
        }
      }
      assertEquals(MESSAGE_NUM - NUM_OF_NOTAVAIL * HWM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t12() {
    LOG.info(
        "\n" +
        "****************************************************************************** \n" +
        "                                                                               \n" +
        "Test DEALER <--> ROUTER-DEALER <--> [...] <--> ROUTER.                         \n" +
        "                                                                               \n" +
        "                      FAIR             FAIR_PASSIVE_MAMA                       \n" +
        "----------      -----------------      -----------------            ---------- \n" +
        "|        | ---> |               | ---> |               |       ---> |        | \n" +
        "| DEALER |      | R(333)-D(444) |      | R(...)-D(...) |---         | ROUTER | \n" +
        "|        | <--- |               | <--- |               |  |    <--- |        | \n" +
        "----------      -----------------      -----------------  |---      ---------- \n" +
        "                                          |               |  |                 \n" +
        "                                          -----------------  |                 \n" +
        "                                             |               |                 \n" +
        "                                             -----------------                 \n" +
        "                                                                               \n" +
        "****************************************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.fairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);
      f.fairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
      f.fairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 555, "tcp://*:" + 666);
      f.fairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 666, "tcp://*:" + 777);
      f.workerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 777);
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t13() {
    LOG.info(
        "\n" +
        "**************************************************** \n" +
        "                                                     \n" +
        "Test DEALER <--> [ROUTER].                           \n" +
        "                                                     \n" +
        "                                          ---------- \n" +
        "                                          |        | \n" +
        "-----------------   ____hello>>__<<carp___| ROUTER | \n" +
        "|               |  /                      |        | \n" +
        "|               | /                       ---------- \n" +
        "|  DEALER(222)  |/                                   \n" +
        "|               |\\                       ---------- \n" +
        "|               | \\                      |        | \n" +
        "-----------------  \\___hello>>__<<shirt__| ROUTER | \n" +
        "                                         |        |  \n" +
        "                                         ----------  \n" +
        "                                                     \n" +
        "**************************************************** \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.workerAcceptor(zmqContext(), new Answering(CARP()), "tcp://localhost:" + 222);
      f.workerAcceptor(zmqContext(), new Answering(SHIRT()), "tcp://localhost:" + 222);
    }
    f.init();

    SyncClient client = f.newBindingClient(zmqContext(), "tcp://*:" + 222);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        replies.add(client.recv());
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t14() {
    LOG.info(
        "\n" +
        "******************************************************* \n" +
        "                                                        \n" +
        "Test DEALER <--> [ROUTER].                              \n" +
        "                                                        \n" +
        "                                       ---------------  \n" +
        "                                       |             |  \n" +
        "------------   ____hello>>__<<world____| ROUTER(333) |  \n" +
        "|          |  /                        |             |  \n" +
        "|          | /                         ---------------  \n" +
        "|  DEALER  |/                                           \n" +
        "|          |\\                          --------------- \n" +
        "|          | \\                         |             | \n" +
        "------------  \\____hello>>__<<world____| ROUTER(334) | \n" +
        "                                       |             |  \n" +
        "                                       ---------------  \n" +
        "                                                        \n" +
        "******************************************************* \n");

    ServiceFixture f = new ServiceFixture();
    {
      f.workerWellknown(zmqContext(), "tcp://*:" + 333, new Answering(WORLD()));
      f.workerWellknown(zmqContext(), "tcp://*:" + 334, new Answering(WORLD()));
    }
    f.init();

    SyncClient client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 333,
                                        "tcp://localhost:" + 334);
    client.lease();
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t15() {
    LOG.info(
        "\n" +
        "**************************************************** \n" +
        "                                                     \n" +
        "Test non-blocking behaviour DEALER <--> [NOT_AVAIL]. \n" +
        "                                                     \n" +
        "                                     -------------   \n" +
        "                                     |           |   \n" +
        "------------   __hello>>__<<no-reply-| NOT_AVAIL |   \n" +
        "|          |  /                      |           |   \n" +
        "|          | /                       -------------   \n" +
        "|  DEALER  |/                                        \n" +
        "|          |\\                       -------------   \n" +
        "|          | \\                      |           |   \n" +
        "------------  \\_hello>>__<<no-reply-| NOT_AVAIL |   \n" +
        "                                    |           |    \n" +
        "                                    -------------    \n" +
        "                                                     \n" +
        "**************************************************** \n");

    ServiceFixture f = new ServiceFixture();

    // NOTE: this test case relies on HWM defaults settings which come along with every socket.
    // test will send 8 message, hopefully, 8 - is not greater or equal to default HWM settings.
    SyncClient client = f.newConnClient(zmqContext(), NOT_AVAIL_0, NOT_AVAIL_1);
    client.lease();
    try {
      int MESSAGE_NUM = 10; // message num being sent is significantly less than default HWM.
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO()); // this line SHOULDN'T block or raise error.
      }
    }
    finally {
      client.release();
      f.destroy();
    }
  }

  @Test
  public void t16() {
    LOG.info(
        "\n" +
        "******************************************************* \n" +
        "                                                        \n" +
        "                 [NOT_AVAIL].                           \n" +
        "Test DEALER <--> [ROUTER].                              \n" +
        "                 [NOT_AVAIL].                           \n" +
        "                                                        \n" +
        "                                       ---------------  \n" +
        "                                       |             |  \n" +
        "------------   ________________________|   NOT_AVAIL |  \n" +
        "|          |  /                        |             |  \n" +
        "|          | /                         ---------------  \n" +
        "|  DEALER  |/                                           \n" +
        "|          |\\                          --------------- \n" +
        "|          | \\                         |             | \n" +
        "------------  \\____hello>>__<<world____| ROUTER(333) | \n" +
        "                                       |             |  \n" +
        "                                       ---------------  \n" +
        "                                                        \n" +
        "******************************************************* \n");

    int livePort = 333;
    ServiceFixture f = new ServiceFixture();
    {
      f.workerWellknown(zmqContext(), "tcp://*:" + livePort, new Answering(WORLD()));
    }
    f.init();

    int HWM = 1;
    int NUM_OF_NOTAVAIL = 2;
    SyncClient client = SyncClient.builder()
                                  .withChannelBuilder(
                                      ZmqChannel.builder()
                                                .withZmqContext(zmqContext())
                                                .ofDEALERType()
                                                .withHwmForSend(HWM)
                                                .withConnectAddress(NOT_AVAIL_0)
                                                .withConnectAddress("tcp://localhost:" + livePort)
                                                .withConnectAddress(NOT_AVAIL_1))
                                  .build();
    client.lease();
    int MESSAGE_NUM = 10 * HWM; // number of messages -- several times bigger than HWM.
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        ZmqMessage reply = client.recv();
        if (reply != null) {
          assertPayload("world", reply);
          replies.add(reply);
        }
      }
      assertEquals(MESSAGE_NUM - NUM_OF_NOTAVAIL * HWM, replies.size());
    }
    finally {
      client.release();
      f.destroy();
    }
  }
}
