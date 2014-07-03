package org.zeromq.messaging.worker;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;
import org.zeromq.messaging.service.AbstractProcessor;
import org.zeromq.messaging.service.Processor;

import static com.google.common.collect.ImmutableList.of;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WorkerTest extends ZmqAbstractTest {

  static final Logger LOGGER = LoggerFactory.getLogger(WorkerTest.class);

  @Test
  public void t0() throws InterruptedException {
    LOGGER.info("master <-*-> slave: master sends 'hello' and expects 'world' back.");

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             bind(inproc("master")).build(),
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
            conn(inproc("master@router")).build(),
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

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             bind(inproc("master")).build(),
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

    WorkerFixture f = new WorkerFixture(c());

    f.slave(Props.builder()
                 .withBindAddr(inproc("t2@router"))
                 .withConnectAddr(conn(5151))
                 .build(),
            conn(conn(5252)).build(),
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

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             bind(inproc("master")).build(),
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
             bind(inproc("worker@master")).build(),
             conn(inproc("master@router")).build(),
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
            conn(inproc("worker@router")).build(),
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
    LOGGER.info("master <-*-> slave: slave sends int, master incr int and reply and so on until 42.");

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             bind(inproc("master")).build(),
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
            conn(inproc("master@router")).build(),
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

  @Test
  public void t5() throws InterruptedException {
    LOGGER.info("N masters <-*-> 1 slave: masters send 'hello', and slave replies 'world'.");

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master0@router")).build(),
             bind(inproc("master0")).build(),
             t5MasterProcessor());
    f.master(Props.builder().withBindAddr(inproc("master1@router")).build(),
             bind(inproc("master1")).build(),
             t5MasterProcessor());
    f.master(Props.builder().withBindAddr(inproc("master2@router")).build(),
             bind(inproc("master2")).build(),
             t5MasterProcessor());

    f.slave(Props.builder().withConnectAddr(of(inproc("master0"), inproc("master1"), inproc("master2"))).build(),
            conn(of(inproc("master0@router"), inproc("master1@router"), inproc("master2@router"))).build(),
            new AbstractProcessor() {
              @Override
              public void onMaster() {
                assertEquals(2, route.size());
                set(world()).route();
              }
            });

    f.init();
    LOGGER.info("Wait a second ...");
    waitSec(); // wait a second.
    try {
      ZmqChannel channel = client(of(inproc("master0@router"), inproc("master1@router"), inproc("master2@router")));
      channel.route(emptyIdentities(), hello(), 0);
      channel.route(emptyIdentities(), hello(), 0);
      channel.route(emptyIdentities(), hello(), 0);
      assertNotNull(channel.recv(0));
      assertNotNull(channel.recv(0));
      assertNotNull(channel.recv(0));
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t6() throws InterruptedException {
    LOGGER.info("1 master <-*-> N slaves: master send 'hello', and slaves reply 'world'.");

    WorkerFixture f = new WorkerFixture(c());

    f.master(Props.builder().withBindAddr(inproc("master@router")).build(),
             bind(inproc("master")).build(),
             new AbstractProcessor() {
               @Override
               public void onRoot() {
                 assertEquals(1, route.size());
                 set(nextSlaveRoute()).route();
               }

               @Override
               public void onSlave() {
                 assertEquals(1, route.size());
                 route();
               }
             });

    f.slave(Props.builder().withConnectAddr(inproc("master")).build(),
            conn(inproc("master@router")).build(),
            t6SlaveProcessor());
    f.slave(Props.builder().withConnectAddr(inproc("master")).build(),
            conn(inproc("master@router")).build(),
            t6SlaveProcessor());
    f.slave(Props.builder().withConnectAddr(inproc("master")).build(),
            conn(inproc("master@router")).build(),
            t6SlaveProcessor());

    f.init();
    LOGGER.info("Wait a second ...");
    waitSec(); // wait a second.
    try {
      ZmqChannel channel = client(inproc("master@router"));
      channel.route(emptyIdentities(), hello(), 0);
      channel.route(emptyIdentities(), hello(), 0);
      channel.route(emptyIdentities(), hello(), 0);
      ZmqFrames frames0 = channel.recv(0);
      assertNotNull(frames0);
      assertEquals("world", new String(frames0.getPayload()));
      ZmqFrames frames1 = channel.recv(0);
      assertNotNull(frames1);
      assertEquals("world", new String(frames1.getPayload()));
      ZmqFrames frames2 = channel.recv(0);
      assertNotNull(frames2);
      assertEquals("world", new String(frames2.getPayload()));
    }
    finally {
      f.destroy();
    }
  }

  private Processor t6SlaveProcessor() {
    return new AbstractProcessor() {
      @Override
      public void onMaster() {
        assertEquals(2, route.size());
        set(world()).route();
      }
    };
  }

  private Processor t5MasterProcessor() {
    return new AbstractProcessor() {
      @Override
      public void onRoot() {
        assertEquals(1, route.size());
        set(nextSlaveRoute()).route();
      }

      @Override
      public void onSlave() {
        assertEquals(1, route.size());
        route();
      }
    };
  }

  private ZmqChannel client(String connectAddr) {
    return ZmqChannel.DEALER(c()).with(Props.builder().withConnectAddr(connectAddr).build()).build();
  }

  private ZmqChannel client(Iterable<String> connectAddr) {
    return ZmqChannel.DEALER(c()).with(Props.builder().withConnectAddr(connectAddr).build()).build();
  }

  private Props.Builder bind(String bindAddr) {
    return Props.builder().withBindAddr(bindAddr);
  }

  private Props.Builder conn(String connectAddr) {
    return Props.builder().withConnectAddr(connectAddr);
  }

  private Props.Builder conn(Iterable<String> connectAddr) {
    return Props.builder().withConnectAddr(connectAddr);
  }

  public static byte[] hello() {
    return "hello".getBytes();
  }

  public static byte[] world() {
    return "world".getBytes();
  }
}
