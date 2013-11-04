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

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.zeromq.Checker;
import org.zeromq.TestRecorder;
import org.zeromq.messaging.ZmqAbstractArchitectureTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqChannelFactory;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.thread.ZmqRunnable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static junit.framework.Assert.assertEquals;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.CARP;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.HELLO;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.SHIRT;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.WORLD;

public class ServiceTest extends ZmqAbstractArchitectureTest {

  static class Answering implements ZmqMessageProcessor {

    private final ZmqMessage ANSWER;

    public Answering(ZmqMessage ANSWER) {
      this.ANSWER = ANSWER;
    }

    @Override
    public ZmqMessage process(ZmqMessage MSG) {
      return ZmqMessage.builder(MSG)
                       .withPayload(ANSWER.payload())
                       .build();
    }
  }

  static class Fixture extends ZmqAbstractArchitectureTest.Fixture {

    LruCache defaultLruCache() {
      return new LruCache(100);
    }

    LruCache volatileLruCache() {
      return new LruCache(1);
    }

    LruCache notMatchingLruCache() {
      return new LruCache(100,
                          new Comparator<byte[]>() {
                            @Override
                            public int compare(byte[] a, byte[] b) {
                              return -1;
                            }
                          });
    }

    LruCache matchingLRUCache() {
      return new LruCache(100,
                          new Comparator<byte[]>() {
                            @Override
                            public int compare(byte[] front, byte[] back) {
                              return front[0] == back[0] ? 0 : -1;
                            }
                          });
    }

    void deployWorkerEmitter(ZmqContext zmqContext,
                             ZmqMessageProcessor messageProcessor,
                             String... connectAddresses) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         WorkerAnonymEmitter.builder()
                                            .withConnectAddresses(connectAddresses)
                                            .withZmqContext(zmqContext)
                                            .withMessageProcessor(messageProcessor)
                                            .withPollTimeout(10)
                                            .build()
                     )
                     .build()
      );
    }

    void deployWorkerEmitterWithIdentity(ZmqContext zmqContext,
                                         String identity,
                                         ZmqMessageProcessor messageProcessor,
                                         String... connectAddresses) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         WorkerAnonymEmitter.builder()
                                            .withConnectAddresses(connectAddresses)
                                            .withZmqContext(zmqContext)
                                            .withMessageProcessor(messageProcessor)
                                            .withWorkerIdentity(identity)
                                            .withPollTimeout(10)
                                            .build()
                     )
                     .build()
      );
    }

    void deployWorkerAcceptor(ZmqContext zmqContext,
                              ZmqMessageProcessor messageProcessor,
                              String... connectAddresses) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         WorkerAnonymAcceptor.builder()
                                             .withConnectAddresses(connectAddresses)
                                             .withZmqContext(zmqContext)
                                             .withMessageProcessor(messageProcessor)
                                             .build()
                     )
                     .build()
      );
    }

    void deployWorkerWellknown(ZmqContext zmqContext,
                               String bindAddress,
                               ZmqMessageProcessor messageProcessor) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         WorkerWellknown.builder()
                                        .withZmqContext(zmqContext)
                                        .withBindAddress(bindAddress)
                                        .withMessageProcessor(messageProcessor)
                                        .build()
                     )
                     .build()
      );
    }

    void deployLruRouter(ZmqContext zmqContext,
                         String frontendAddress,
                         String backendAddress,
                         LruCache lruCache) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         LruRouter.builder()
                                  .withZmqContext(zmqContext)
                                  .withSocketIdentityStorage(lruCache)
                                  .withFrontendAddress(frontendAddress)
                                  .withBackendAddress(backendAddress)
                                  .build()
                     )
                     .build()
      );
    }

    void deployFairRouter(ZmqContext zmqContext,
                          String frontendAddress,
                          String backendAddress) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         FairRouter.builder()
                                   .withZmqContext(zmqContext)
                                   .withFrontendAddress(frontendAddress)
                                   .withBackendAddress(backendAddress)
                                   .build()
                     )
                     .build()
      );
    }

    void deployFairActiveAcceptor(ZmqContext zmqContext,
                                  String frontendAddress,
                                  String backendAddress) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         FairActiveAcceptor.builder()
                                           .withZmqContext(zmqContext)
                                           .withFrontendAddress(frontendAddress)
                                           .withBackendAddress(backendAddress)
                                           .build()
                     )
                     .build()
      );
    }

    void deployFairPassiveAcceptor(ZmqContext zmqContext,
                                   String frontendAddress,
                                   String backendAddress) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         FairPassiveAcceptor.builder()
                                            .withZmqContext(zmqContext)
                                            .withFrontendAddress(frontendAddress)
                                            .withBackendAddress(backendAddress)
                                            .build()
                     )
                     .build()
      );
    }

    void deployFairEmitter(ZmqContext zmqContext,
                           String frontendAddress,
                           String backendAddress) {
      _threadPool.withRunnable(
          ZmqRunnable.builder()
                     .withRunnableContext(
                         FairEmitter.builder()
                                    .withZmqContext(zmqContext)
                                    .withFrontendAddress(frontendAddress)
                                    .withBackendAddress(backendAddress)
                                    .build()
                     )
                     .build()
      );
    }
  }

  @Test
  public void t0() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployWorkerAcceptor(zmqContext(), new Answering(SHIRT()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    int MESSAGE_NUM = 10;
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t1() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
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
      f.destroy();
    }
  }

  @Test
  public void t1_perf() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    int ITER = 100;
    int MESSAGE_NUM = 100;
    try {
      Stopwatch timer = new Stopwatch().start();
      for (int j = 0; j < ITER; j++) {
        for (int i = 0; i < MESSAGE_NUM; i++) {
          assert client.send(HELLO());
          assert client.recv() != null;
        }
      }
      r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/req-rep.");
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t2() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.volatileLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
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
      f.destroy();
    }
  }

  @Test
  public void t3() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "*************************************************************** \n" +
        "                                                                \n" +
        "Test DEALER(try_again=true) <--> ROUTER-ROUTER <--> DEALER.     \n" +
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.notMatchingLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t4() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "******************************************************************************************* \n" +
        "                                                                                            \n" +
        "Test DEALER(try_again=true) <--> ROUTER-DEALER <--> ROUTER-ROUTER <--> DEALER.              \n" +
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

    Fixture f = new Fixture();

    f.deployFairEmitter(zmqContext(), "inproc://gateway", "tcp://localhost:" + 333);

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.notMatchingLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "inproc://gateway");
    int MESSAGE_NUM = 10;
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      assert client.recv() == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t5() throws InterruptedException {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    final Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    Runnable client = new Runnable() {
      public void run() {
        ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
        Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
        for (int i = 0; i < MESSAGE_NUM; i++) {
          assert client.send(HELLO());
          ZmqMessage reply = client.recv();
          assertPayload("world", reply);
          replies.add(reply);
        }
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
  public void t5_perf() throws InterruptedException {
    final TestRecorder r = new TestRecorder().start();
    r.log(
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

    final Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    Runnable client = new Runnable() {
      public void run() {
        ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
        int ITER = 100;
        int MESSAGE_NUM = 100;
        Stopwatch timer = new Stopwatch().start();
        for (int j = 0; j < ITER; j++) {
          for (int i = 0; i < MESSAGE_NUM; i++) {
            assert client.send(HELLO());
            assert client.recv() != null;
          }
        }
        r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/req-rep.");
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
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployFairActiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
    f.deployFairActiveAcceptor(zmqContext(), "tcp://localhost:" + 555, "tcp://*:" + 666);

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 666);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    try {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
      }
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        ZmqMessage reply = client.recv();
        assertPayload("world", reply);
        replies.add(reply);
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t6_perf() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.defaultLruCache());

    f.deployFairActiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
    f.deployFairActiveAcceptor(zmqContext(), "tcp://localhost:" + 555, "tcp://*:" + 666);

    f.deployWorkerEmitter(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 666);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
    int ITER = 100;
    int MESSAGE_NUM = 100;
    try {
      Stopwatch timer = new Stopwatch().start();
      for (int j = 0; j < ITER; j++) {
        for (int i = 0; i < MESSAGE_NUM; i++) {
          assert client.send(HELLO());
          assert client.recv() != null;
        }
      }
      r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/req-rep.");
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t7() throws InterruptedException {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    final Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.matchingLRUCache());

    f.deployWorkerEmitterWithIdentity(zmqContext(), "X", new Answering(SHIRT()), "tcp://localhost:" + 444);
    f.deployWorkerEmitterWithIdentity(zmqContext(), "Y", new Answering(CARP()), "tcp://localhost:" + 444);

    f.init();

    Checker checker = new Checker();

    Thread t0 = new Thread(
        new Runnable() {
          @Override
          public void run() {
            ZmqChannel c = f.newConnClientWithIdentity(zmqContext(), "X", "tcp://localhost:" + 333);
            Collection<ZmqMessage> rr = new ArrayList<ZmqMessage>();
            for (int i = 0; i < MESSAGE_NUM; i++) {
              assert c.send(HELLO());
              ZmqMessage reply = c.recv();
              assertPayload("shirt", reply);
              rr.add(reply);
            }
            assertEquals(MESSAGE_NUM, rr.size());
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
            ZmqChannel c = f.newConnClientWithIdentity(zmqContext(), "Y", "tcp://localhost:" + 333);
            Collection<ZmqMessage> rr = new ArrayList<ZmqMessage>();
            for (int i = 0; i < MESSAGE_NUM; i++) {
              assert c.send(HELLO());
              ZmqMessage reply = c.recv();
              assertPayload("carp", reply);
              rr.add(reply);
            }
            assertEquals(MESSAGE_NUM, rr.size());
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
  public void t7_perf() throws InterruptedException {
    final TestRecorder r = new TestRecorder().start();
    r.log(
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

    final Fixture f = new Fixture();

    f.deployLruRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444, f.matchingLRUCache());

    f.deployWorkerEmitterWithIdentity(zmqContext(), "X", new Answering(SHIRT()), "tcp://localhost:" + 444);
    f.deployWorkerEmitterWithIdentity(zmqContext(), "Y", new Answering(SHIRT()), "tcp://localhost:" + 444);

    f.init();

    Checker checker = new Checker();

    Thread t0 = new Thread(
        new Runnable() {
          @Override
          public void run() {
            ZmqChannel c = f.newConnClientWithIdentity(zmqContext(), "X", "tcp://localhost:" + 333);
            int ITER = 100;
            int MESSAGE_NUM = 100;
            Stopwatch timer = new Stopwatch().start();
            for (int j = 0; j < ITER; j++) {
              for (int i = 0; i < MESSAGE_NUM; i++) {
                assert c.send(HELLO());
                assert c.recv() != null;
              }
            }
            r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/req-rep.");
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
            ZmqChannel c = f.newConnClientWithIdentity(zmqContext(), "Y", "tcp://localhost:" + 333);
            int ITER = 100;
            int MESSAGE_NUM = 100;
            Stopwatch timer = new Stopwatch().start();
            for (int j = 0; j < ITER; j++) {
              for (int i = 0; i < MESSAGE_NUM; i++) {
                assert c.send(HELLO());
                assert c.recv() != null;
              }
            }
            r.logQoS(timer.stop().elapsedTime(MICROSECONDS) / (ITER * MESSAGE_NUM), "microsec/req-rep.");
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
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployFairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);

    f.deployWorkerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);
    f.deployWorkerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 444);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
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
      f.destroy();
    }
  }

  @Test
  public void t9() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployFairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);

    f.deployFairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);

    f.deployWorkerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 555);
    f.deployWorkerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 555);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
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
      f.destroy();
    }
  }

  @Test
  public void t10() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployFairRouter(zmqContext(), "tcp://*:" + 555, "tcp://*:" + 666);
    f.deployFairRouter(zmqContext(), "tcp://*:" + 556, "tcp://*:" + 667);

    f.deployWorkerAcceptor(zmqContext(),
                           new Answering(WORLD()),
                           "tcp://localhost:" + 666,
                           "tcp://localhost:" + 667);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 555,
                                        "tcp://localhost:" + 556);
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
      f.destroy();
    }
  }

  @Test
  public void t11() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployFairRouter(zmqContext(), "tcp://*:" + 555, "tcp://*:" + 666);

    // Create worker connected at all HUBs' backends.
    // NOT: there will be only one LIVE HUB.

    f.deployWorkerAcceptor(zmqContext(),
                           new Answering(WORLD()),
                           "tcp://localhost:" + 667,  // NOT_AVAIL
                           "tcp://localhost:" + 666,  // !!!!! LIVE HUB !!!!!
                           "tcp://localhost:" + 670); // NOT_AVAIL

    f.init();

    int HWM_FOR_SEND = 10;
    int NUM_OF_NOTAVAIL = 2;
    ZmqChannel client = ZmqChannelFactory.builder()
                                         .withZmqContext(zmqContext())
                                         .ofDEALERType()
                                         .withWaitOnSend(100)
                                         .withWaitOnRecv(100)
                                         .withHwmForSend(HWM_FOR_SEND)
                                         .withConnectAddress("tcp://localhost:" + 556)
                                         .withConnectAddress("tcp://localhost:" + 555)
                                         .withConnectAddress("tcp://localhost:" + 559)
                                         .build()
                                         .newChannel();

    int MESSAGE_NUM = 10 * HWM_FOR_SEND; // number of messages -- several times bigger than HWM.
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
      assertEquals(MESSAGE_NUM - NUM_OF_NOTAVAIL * HWM_FOR_SEND, replies.size());
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t12() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployFairRouter(zmqContext(), "tcp://*:" + 333, "tcp://*:" + 444);

    f.deployFairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 444, "tcp://*:" + 555);
    f.deployFairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 555, "tcp://*:" + 666);
    f.deployFairPassiveAcceptor(zmqContext(), "tcp://localhost:" + 666, "tcp://*:" + 777);

    f.deployWorkerAcceptor(zmqContext(), new Answering(WORLD()), "tcp://localhost:" + 777);

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(), "tcp://localhost:" + 333);
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
      f.destroy();
    }
  }

  @Test
  public void t13() {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "\n" +
        "**************************************************** \n" +
        "                                                     \n" +
        "Test DEALER <--> [ROUTER].                           \n" +
        "                                                     \n" +
        "                                     --------------- \n" +
        "                                     |             | \n" +
        "------------   ____hello>>__<<carp___| ROUTER(333) | \n" +
        "|          |  /                      |             | \n" +
        "|          | /                       --------------- \n" +
        "|  DEALER  |/                                        \n" +
        "|          |\\                       --------------- \n" +
        "|          | \\                      |             | \n" +
        "------------  \\___hello>>__<<shirt__| ROUTER(334) | \n" +
        "                                    |             |  \n" +
        "                                    ---------------  \n" +
        "                                                     \n" +
        "**************************************************** \n");

    Fixture f = new Fixture();

    f.deployWorkerWellknown(zmqContext(), "tcp://*:" + 333, new Answering(CARP()));
    f.deployWorkerWellknown(zmqContext(), "tcp://*:" + 334, new Answering(SHIRT()));

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 333,
                                        "tcp://localhost:" + 334);
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        replies.add(client.recv());
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t14() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployWorkerAcceptor(zmqContext(), new Answering(CARP()), "tcp://localhost:" + 222);
    f.deployWorkerAcceptor(zmqContext(), new Answering(SHIRT()), "tcp://localhost:" + 222);

    f.init();

    ZmqChannel client = f.newBindingClient(zmqContext(), "tcp://*:" + 222);
    try {
      Collection<ZmqMessage> replies = new ArrayList<ZmqMessage>();
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO());
        replies.add(client.recv());
      }
      assertEquals(MESSAGE_NUM, replies.size());
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t15() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    f.deployWorkerWellknown(zmqContext(), "tcp://*:" + 333, new Answering(WORLD()));
    f.deployWorkerWellknown(zmqContext(), "tcp://*:" + 334, new Answering(WORLD()));

    f.init();

    ZmqChannel client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 333,
                                        "tcp://localhost:" + 334);
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
      f.destroy();
    }
  }

  @Test
  public void t16() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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

    Fixture f = new Fixture();

    // NOTE: this test case relies on HWM defaults settings which come along with every socket.
    // test will send 8 message, hopefully, 8 - is not greater or equal to default HWM settings.

    ZmqChannel client = f.newConnClient(zmqContext(),
                                        "tcp://localhost:" + 333,  // NOT_AVAIL
                                        "tcp://localhost:" + 334,  // NOT_AVAIL
                                        "tcp://localhost:" + 335,  // NOT_AVAIL
                                        "tcp://localhost:" + 336); // NOT_AVAIL
    try {
      int MESSAGE_NUM = 10; // message num being sent is significantly less than default HWM.
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert client.send(HELLO()); // this line SHOULDN'T block or raise error.
      }
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t17() {
    TestRecorder r = new TestRecorder().start();
    r.log(
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
    Fixture f = new Fixture();

    f.deployWorkerWellknown(zmqContext(), "tcp://*:" + livePort, new Answering(WORLD()));

    f.init();

    int HWM_FOR_SEND = 10;
    int NUM_OF_NOTAVAIL = 2;
    ZmqChannel client = ZmqChannelFactory.builder()
                                         .withZmqContext(zmqContext())
                                         .ofDEALERType()
                                         .withWaitOnSend(100)
                                         .withWaitOnRecv(100)
                                         .withHwmForSend(HWM_FOR_SEND)
                                         .withConnectAddress("tcp://localhost:" + 777)
                                         .withConnectAddress("tcp://localhost:" + livePort)
                                         .withConnectAddress("tcp://localhost:" + 780)
                                         .build()
                                         .newChannel();

    int MESSAGE_NUM = 10 * HWM_FOR_SEND; // number of messages -- several times bigger than HWM.
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
      assertEquals(MESSAGE_NUM - NUM_OF_NOTAVAIL * HWM_FOR_SEND, replies.size());
    }
    finally {
      f.destroy();
    }
  }
}
