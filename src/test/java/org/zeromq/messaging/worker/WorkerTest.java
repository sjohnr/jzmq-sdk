package org.zeromq.messaging.worker;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.service.AbstractProcessor;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WorkerTest extends ZmqAbstractTest {

  static final Logger LOGGER = LoggerFactory.getLogger(WorkerTest.class);

  @Test
  public void t0() throws InterruptedException {
    LOGGER.info("master <-*-> slave: master sends 'hello' and expects 'world' back.");

    WorkerFixture f = new WorkerFixture(ctx());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             masterAddr(inproc("master")).build(),
             new AbstractProcessor() {
               @Override
               public void onRoot() {
                 assertEquals(1, route.size());
                 assertEquals("hello", new String(payload));
                 set(nextSlaveRoute()).route();
               }

               @Override
               public void onSlave() {
                 assertEquals(1, route.size());
                 assertEquals("world", new String(payload));
                 route();
               }
             });

    f.slave(Props.builder().withConnectAddr(inproc("master")).build(),
            slaveAddr(inproc("master@router")).build(),
            new AbstractProcessor() {
              @Override
              public void onMaster() {
                assertEquals(2, route.size());
                assertEquals("hello", new String(payload));
                set(world()).route();
              }
            });

    f.init();
    LOGGER.info("Wait a second ...");
    waitSec(); // wait a second.
    try {
      ZmqChannel channel = client(inproc("master@router"));
      channel.route(emptyIdentities(), hello(), 0);
      ZmqFrames frames = channel.recv(0);
      assertNotNull(frames);
      assertEquals("world", new String(frames.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t1() {
    LOGGER.info("master <-x-> no slave: connection between them isn't really ready, expect 'hello' back.");

    WorkerFixture f = new WorkerFixture(ctx());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             masterAddr(inproc("master")).build(),
             new AbstractProcessor() {
               @Override
               public void onRoot() {
                 assertEquals(1, route.size());
                 assertEquals("hello", new String(payload));
                 assertNull(nextSlaveRoute());
                 route();
               }
             });

    f.init();
    try {
      ZmqChannel channel = client(inproc("master@router"));
      channel.route(emptyIdentities(), hello(), 0);
      ZmqFrames frames = channel.recv(0);
      assertNotNull(frames);
      assertEquals("hello", new String(frames.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t2() {
    LOGGER.info("slave <-x-> no master: connection between them isn't really ready, expect 'hello' back.");

    WorkerFixture f = new WorkerFixture(ctx());

    f.slave(Props.builder()
                 .withBindAddr(inproc("t2@router"))
                 .withConnectAddr(conn(5151))
                 .build(),
            slaveAddr(conn(5252)).build(),
            new AbstractProcessor() {
              @Override
              public void onRoot() {
                assertEquals(1, route.size());
                assertEquals("hello", new String(payload));
                assertNull(nextMasterRoute());
                route();
              }
            });

    f.init();
    try {
      ZmqChannel channel = client(inproc("t2@router"));
      channel.route(emptyIdentities(), hello(), 0);
      ZmqFrames frames = channel.recv(0);
      assertNotNull(frames);
      assertEquals("hello", new String(frames.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t3() throws InterruptedException {
    LOGGER.info("master <-*-> worker <-*-> slave: master sends 'hello' and expects 'world' back.");

    WorkerFixture f = new WorkerFixture(ctx());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             masterAddr(inproc("master")).build(),
             new AbstractProcessor() {
               @Override
               public void onRoot() {
                 assertEquals(1, route.size());
                 assertEquals("hello", new String(payload));
                 set(nextSlaveRoute()).route();
               }

               @Override
               public void onSlave() {
                 assertEquals(1, route.size());
                 assertEquals("world", new String(payload));
                 route();
               }
             });

    f.worker(Props.builder()
                  .withConnectAddr(inproc("master"))
                  .withBindAddr(inproc("worker@router"))
                  .build(),
             masterAddr(inproc("worker@master")).build(),
             slaveAddr(inproc("master@router")).build(),
             new AbstractProcessor() {
               @Override
               public void onMaster() {
                 assertEquals(2, route.size());
                 assertEquals("hello", new String(payload));
                 set(nextSlaveRoute()).route();
               }

               @Override
               public void onSlave() {
                 assertEquals(2, route.size());
                 assertEquals("world", new String(payload));
                 route();
               }
             });

    f.slave(Props.builder().withConnectAddr(inproc("worker@master")).build(),
            slaveAddr(inproc("worker@router")).build(),
            new AbstractProcessor() {
              @Override
              public void onMaster() {
                assertEquals(3, route.size());
                assertEquals("hello", new String(payload));
                set(world()).route();
              }
            });

    f.init();
    LOGGER.info("Wait a second ...");
    waitSec(); // wait a second.
    try {
      ZmqChannel channel = client(inproc("master@router"));
      channel.route(emptyIdentities(), hello(), 0);
      ZmqFrames frames = channel.recv(0);
      assertNotNull(frames);
      assertEquals("world", new String(frames.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t4() throws InterruptedException {
    LOGGER.info("master <-*-> slave: slave sends 'hello', master replying 'world' and so on until 42.");

    WorkerFixture f = new WorkerFixture(ctx());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             masterAddr(inproc("master")).build(),
             new AbstractProcessor() {
               @Override
               public void onSlave() {
                 assertEquals(2, route.size());
                 int i = Integer.parseInt(new String(payload));
                 set(new ZmqFrames(origin(), thisMaster(), root())).set(String.valueOf(++i).getBytes()).route();
               }
             });

    f.slave(Props.builder()
                 .withBindAddr(inproc("slave@router"))
                 .withConnectAddr(inproc("master"))
                 .build(),
            slaveAddr(inproc("master@router")).build(),
            new AbstractProcessor() {
              @Override
              public void onRoot() {
                assertEquals(1, route.size());
                assertEquals("0", new String(payload));
                set(nextMasterRoute()).route();
              }

              @Override
              public void onMaster() {
                assertEquals(2, route.size());
                int i = Integer.parseInt(new String(payload));
                if (i == 42) {
                  set(new ZmqFrames(root())).set(String.valueOf(i).getBytes()).route();
                }
                else {
                  set(new ZmqFrames(origin(), thisSlave(), root())).route();
                }
              }
            });

    f.init();
    LOGGER.info("Wait a second ...");
    waitSec(); // wait a second.
    try {
      ZmqChannel channel = client(inproc("slave@router"));
      channel.route(emptyIdentities(), "0".getBytes(), 0);
      ZmqFrames frames = channel.recv(0);
      assertNotNull(frames);
      assertEquals("42", new String(frames.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  private ZmqChannel client(String connectAddr) {
    return ZmqChannel.DEALER(ctx()).with(Props.builder().withConnectAddr(connectAddr).build()).build();
  }

  private Props.Builder masterAddr(String bindAddr) {
    return Props.builder().withBindAddr(bindAddr);
  }

  private Props.Builder slaveAddr(String connectAddr) {
    return Props.builder().withConnectAddr(connectAddr);
  }

  private Props.Builder slaveAddr(Iterable<String> connectAddr) {
    return Props.builder().withConnectAddr(connectAddr);
  }

  public static byte[] hello() {
    return "hello".getBytes();
  }

  public static byte[] world() {
    return "world".getBytes();
  }
}
