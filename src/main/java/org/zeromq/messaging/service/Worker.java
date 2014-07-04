package org.zeromq.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractActor;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

import java.util.Arrays;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.support.ZmqUtils.makeHash;

public final class Worker extends ZmqAbstractActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  public static final byte[] PING = "ping".getBytes();
  public static final byte[] PONG = "pong".getBytes();

  /**
   * Worker's outgoing traffic gateway. It can {@code bind} and/or {@code connect}, in either case
   * this socket is -- {@code ROUTER} with "routing table".
   * <p/>
   * If {@code bind} than it's <b>master mode</b>:
   * <ul>
   * <li>via this socket must be expected outgoing "request" traffic</li>
   * <li>"routing table" must contain socket identities of {@code slaves} (those who had sent PING here)</li>
   * <li>this socket has to serve PING from {@code slaves} and populate "routing table"</li>
   * <li>on PING from {@code slave} this socket shall send PONG back</li>
   * </ul>
   * <pre>
   *   byte[] -->         <--[conn,PING]-- *DEALER
   *     PING <-- ROUTER: <--[conn,PING]-- *DEALER
   *     PONG -->         <--[conn,PING]-- *DEALER
   * </pre>
   * If {@code connect} than it's <b>slave mode</b>:
   * <ul>
   * <li>via this socket must be expected outgoing "reply" traffic</li>
   * <li>"routing table" must contain socket identities of {@code masters} (those who had sent PONG here)</li>
   * </ul>
   * <pre>
   *   byte[] -->         --conn--> :DEALER
   *              ROUTER* --conn--> :DEALER
   *   byte[] -->         --conn--> :DEALER
   * </pre>
   */
  private static final String ROUTER = "router[:*]";
  /**
   * Worker's incoming traffic gateway. Socket is -- bound {@code DEALER} with "identity".
   * <p/>
   * Designates a fact that actor is working in <b>master mode</b>.
   * <ul>
   * <li>via this socket must be expected incoming "reply" traffic from {@code routers}</li>
   * </ul>
   * <pre>
   *   byte[] <--         <--conn-- *ROUTER
   *              DEALER: <--conn-- *ROUTER
   *   byte[] <--         <--conn-- *ROUTER
   * </pre>
   */
  private static final String MASTER = "master[:]";
  /**
   * Worker's incoming traffic gateway. Socket is -- connected {@code DEALER} with "identity".
   * <p/>
   * Designates a fact that actor is working in <b>slave mode</b>.
   * <ul>
   * <li>via this socket must be expected incoming "request" traffic from {@code routers}</li>
   * <li>PING must be sent to all {@code routers} and PONG shall be expected back</li>
   * </ul>
   * <pre>
   *   byte[] <--         --[conn,PING]--> :ROUTER
   *              DEALER* --[conn,PING]--> :ROUTER
   *     PONG <--         --[conn,PING]--> :ROUTER
   * </pre>
   */
  private static final String SLAVE = "slave[*]";

  public static final class Builder extends ZmqAbstractActor.Builder<Builder, Worker> {

    private Builder() {
      super(new Worker());
    }

    public Builder withRouter(Props props) {
      _target.setRouter(props);
      return this;
    }

    public Builder withMaster(Props props) {
      _target.setMaster(props);
      return this;
    }

    public Builder withSlave(Props props) {
      _target.setSlave(props);
      return this;
    }

    public Builder withMasterRouting(Routing routing) {
      _target.setMasterRouting(routing);
      return this;
    }

    public Builder withSlaveRouting(Routing routing) {
      _target.setSlaveRouting(routing);
      return this;
    }

    public Builder with(Processor processor) {
      _target.setProcessor(processor);
      return this;
    }
  }

  private Props master;
  private Props slave;
  private Props router;
  private Processor processor;
  /** 0 - master, 1 - slave */
  private Routing[] routings = new Routing[2];
  /** 0 - master, 1 - slave */
  private Object[] identities = new Object[2];

  //// CONSTRUCTORS

  private Worker() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setRouter(Props router) {
    this.router = router;
  }

  public void setMaster(Props master) {
    this.master = master;
  }

  public void setSlave(Props slave) {
    this.slave = slave;
  }

  public void setMasterRouting(Routing masterRouting) {
    this.routings[0] = masterRouting;
  }

  public void setSlaveRouting(Routing slaveRouting) {
    this.routings[1] = slaveRouting;
  }

  public void setProcessor(Processor processor) {
    this.processor = processor;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    {
      checkArgument(router != null);
      if (master != null) {
        checkArgument(!router.bindAddr().isEmpty(), "Router: bindAddr is required!");
      }
      if (slave != null) {
        checkArgument(!router.connectAddr().isEmpty(), "Router: connectAddr is required!");
      }
    }
    if (master != null) {
      checkArgument(!master.bindAddr().isEmpty(), "Master: bindAddr is required!");
      checkArgument(master.bindAddr().size() == 1);
      checkArgument(master.connectAddr().isEmpty(), "Master: can't have connecAddr!");
      checkArgument(routings[1] != null, "Master: slaveRouting is required!");
    }
    if (slave != null) {
      checkArgument(slave.bindAddr().isEmpty(), "Slave: can't have bindAddr!");
      checkArgument(!slave.connectAddr().isEmpty(), "Slave: connectAddr is required!");
      checkArgument(routings[0] != null, "Slave: masterRouting is required!");
    }
    checkArgument(processor != null);
  }

  @Override
  public void init() {
    if (master != null) {
      if (master.identity() == null) {
        master = Props.builder(master).withIdentity(generateIdentity()).build();
      }
      put(MASTER, ZmqChannel.DEALER(ctx).with(master).build()).watchRecv(_poller);
      identities[0] = master.identity();
    }
    if (slave != null) {
      if (slave.identity() == null) {
        slave = Props.builder(slave).withIdentity(generateIdentity()).build();
      }
      put(SLAVE, ZmqChannel.DEALER(ctx).with(slave).build()).watchRecv(_poller);
      identities[1] = slave.identity();
    }
    router = Props.builder(router).withRouterMandatory().build();
    put(ROUTER, ZmqChannel.ROUTER(ctx).with(router).build()).watchRecv(_poller);
  }

  @Override
  public void exec() throws Exception {
    poll();

    ZmqChannel router = get(ROUTER);
    ZmqChannel master = get(MASTER);
    ZmqChannel slave = get(SLAVE);

    if (router.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = router.recv(DONTWAIT);
        if (frames == null)
          break;

        byte[] payload = frames.getPayload();
        ZmqFrames route = frames.getIdentities();
        if (isPing(payload)) {
          if (route.size() == 1) {
            routings[1].put(route.get(0), payload);
            {
              // Send PONG back, use identities [route|master_identity].
              ZmqFrames masterRoute = new ZmqFrames();
              masterRoute.add(route.get(0));
              masterRoute.add((byte[]) identities[0]);
              router.route(masterRoute, PONG, DONTWAIT);
              LOGGER.info("Got PING (slave.hash={}), send PONG back (master.hash={}).",
                          makeHash(route.get(0)),
                          makeHash((byte[]) identities[0]));
            }
          }
          else {
            LOGGER.error("Wrong PING! Got route.size={}.", route.size());
          }
        }
        else {
          logTraffic("router", payload);
          processor.set(route).set(payload).set(router).set(routings).set(identities).onRoot();
        }
      }
    }

    if (slave != null) {
      if (!slave.canRecv()) {
        // Send PING (dont send blindly, check "timer" before send, e.g. every X seconds).
        for (String connectAddr : this.slave.connectAddr()) {
          slave.route(new ZmqFrames(), PING, DONTWAIT);
          LOGGER.info("Send PING (slave.hash={}) on {}.", makeHash((byte[]) identities[1]), connectAddr);
        }
      }
      else {
        for (; ; ) {
          ZmqFrames frames = slave.recv(DONTWAIT);
          if (frames == null)
            break;

          byte[] payload = frames.getPayload();
          ZmqFrames route = frames.getIdentities();
          if (isPong(payload)) {
            if (route.size() == 1) {
              LOGGER.info("Got PONG (master.hash={}).", makeHash(route.get(0)));
              routings[0].put(route.get(0), payload);
            }
            else {
              LOGGER.error("Wrong PONG! Got route.size={}.", route.size());
            }
          }
          else {
            logTraffic("master", payload);
            processor.set(route).set(payload).set(router).set(routings).set(identities).onMaster();
          }
        }
      }
    }

    if (master != null) {
      if (master.canRecv()) {
        for (; ; ) {
          ZmqFrames frames = master.recv(DONTWAIT);
          if (frames == null)
            break;

          byte[] payload = frames.getPayload();
          ZmqFrames route = frames.getIdentities();
          logTraffic("slave", payload);
          processor.set(route).set(payload).set(router).set(routings).set(identities).onSlave();
        }
      }
    }
  }

  private void logTraffic(String prefix, byte[] payload) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got {} traffic (payload={} bytes).", prefix, payload.length);
    }
  }

  private boolean isPing(byte[] payload) {
    return Arrays.equals(payload, PING);
  }

  private boolean isPong(byte[] payload) {
    return Arrays.equals(payload, PONG);
  }

  private byte[] generateIdentity() {
    return ("" + UUID.randomUUID().getMostSignificantBits()).getBytes();
  }
}
