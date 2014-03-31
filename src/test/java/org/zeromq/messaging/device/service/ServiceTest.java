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
import org.zeromq.messaging.ZmqMessage;

import java.util.ArrayList;
import java.util.Collection;

import static junit.framework.Assert.assertEquals;
import static org.zeromq.messaging.device.service.ServiceFixture.Answering.answering;

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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.defaultLruCache());
      f.workerAcceptor(ctx(), answering(SHIRT()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.defaultLruCache());
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.volatileLruCache());
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.notMatchingLruCache());
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.fairEmitter(ctx(), inprocAddr("gateway"), connAddr(333));
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.notMatchingLruCache());
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), inprocAddr("gateway"));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.defaultLruCache());
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    Runnable client = new Runnable() {
      public void run() {
        BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.defaultLruCache());
      f.fairActiveAcceptor(ctx(), connAddr(444), bindAddr(555));
      f.fairActiveAcceptor(ctx(), connAddr(555), bindAddr(666));
      f.workerEmitter(ctx(), answering(WORLD()), connAddr(666));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.lruRouter(ctx(), bindAddr(333), bindAddr(444), f.matchingLRUCache());
      f.workerEmitterWithIdentity(ctx(), "X", answering(SHIRT()), connAddr(444));
      f.workerEmitterWithIdentity(ctx(), "Y", answering(CARP()), connAddr(444));
      f.workerEmitterWithIdentity(ctx(), "X", answering(SHIRT()), connAddr(444));
      f.workerEmitterWithIdentity(ctx(), "Y", answering(CARP()), connAddr(444));
    }
    f.init();

    Checker checker = new Checker();

    Thread t0 = new Thread(
        new Runnable() {
          @Override
          public void run() {
            BlockingClient client = f.newConnBlockingClientWithIdentity(ctx(), "X", connAddr(333));
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
            BlockingClient client = f.newConnBlockingClientWithIdentity(ctx(), "Y", connAddr(333));
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
      f.fairRouter(ctx(), bindAddr(333), bindAddr(444));
      f.workerAcceptor(ctx(), answering(WORLD()), connAddr(444));
      f.workerAcceptor(ctx(), answering(WORLD()), connAddr(444));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.fairRouter(ctx(), bindAddr(333), bindAddr(444));
      f.fairPassiveAcceptor(ctx(), connAddr(444), bindAddr(555));
      f.workerAcceptor(ctx(), answering(WORLD()), connAddr(555));
      f.workerAcceptor(ctx(), answering(WORLD()), connAddr(555));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.fairRouter(ctx(), bindAddr(333), bindAddr(444));
      f.fairPassiveAcceptor(ctx(), connAddr(444), bindAddr(555));
      f.fairPassiveAcceptor(ctx(), connAddr(555), bindAddr(666));
      f.fairPassiveAcceptor(ctx(), connAddr(666), bindAddr(777));
      f.workerAcceptor(ctx(), answering(WORLD()), connAddr(777));
    }
    f.init();

    BlockingClient client = f.newConnBlockingClient(ctx(), connAddr(333));
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
      f.workerAcceptor(ctx(), answering(CARP()), connAddr(222));
      f.workerAcceptor(ctx(), answering(SHIRT()), connAddr(222));
    }
    f.init();

    BlockingClient client = f.newBindBlockingClient(ctx(), bindAddr(222));
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
}
