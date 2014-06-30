package org.zeromq.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractActor;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.support.ZmqUtils.makeHash;

public final class Worker extends ZmqAbstractActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  public static final byte[] PING = "ping".getBytes();
  public static final byte[] PONG = "pong".getBytes();

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

  public static final class Builder extends ZmqAbstractActor.Builder<Builder, Worker> {

    private Builder() {
      super(new Worker());
    }

    public Builder withMaster(Props props) {
      _target.setMaster(props);
      return this;
    }

    public Builder withSlave(Props props) {
      _target.setSlave(props);
      return this;
    }

    public Builder withRouter(Props props) {
      _target.setRouter(props);
      return this;
    }

    public Builder withSlaveRouting(ZmqRouting routing) {
      _target.setSlaveRouting(routing);
      return this;
    }

    public Builder withMasterRouting(ZmqRouting routing) {
      _target.setMasterRouting(routing);
      return this;
    }

    public Builder with(ZmqProcessor processor) {
      _target.setProcessor(processor);
      return this;
    }
  }

  private Props master;
  private Props slave;
  private Props router;
  private ZmqRouting slaveRouting;
  private ZmqRouting masterRouting;
  private ZmqProcessor processor;

  //// CONSTRUCTORS

  private Worker() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setMaster(Props master) {
    this.master = master;
  }

  public void setSlave(Props slave) {
    this.slave = slave;
  }

  public void setRouter(Props router) {
    this.router = router;
  }

  public void setSlaveRouting(ZmqRouting slaveRouting) {
    this.slaveRouting = slaveRouting;
  }

  public void setMasterRouting(ZmqRouting masterRouting) {
    this.masterRouting = masterRouting;
  }

  public void setProcessor(ZmqProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (master != null) {
      checkArgument(!master.bindAddr().isEmpty(), "Master: bindAddr is required!");
      checkArgument(master.bindAddr().size() == 1);
      checkArgument(master.connectAddr().isEmpty(), "Master: can't have connecAddr!");
      checkArgument(master.identity() != null, "Master: identity is required!");
      checkArgument(slaveRouting != null, "Master: slaveRouting is required!");
    }
    if (slave != null) {
      checkArgument(slave.bindAddr().isEmpty(), "Slave: can't have bindAddr!");
      checkArgument(!slave.connectAddr().isEmpty(), "Slave: connectAddr is required!");
      checkArgument(slave.identity() != null, "Slave: identity is required!");
      checkArgument(masterRouting != null, "Slave: masterRouting is required!");
    }
    {
      checkArgument(router != null);
      if (master != null) {
        checkArgument(!router.bindAddr().isEmpty(), "Router: bindAddr is required!");
        checkArgument(router.bindAddr().size() == 1);
      }
      if (slave != null) {
        checkArgument(!router.connectAddr().isEmpty(), "Router: connectAddr is required!");
      }
    }
    checkArgument(processor != null);
  }

  @Override
  public void init() {
    if (master != null) {
      put(MASTER, ZmqChannel.DEALER(ctx).with(master).build()).watchRecv(_poller);
    }
    if (slave != null) {
      put(SLAVE, ZmqChannel.DEALER(ctx).with(slave).build()).watchRecv(_poller);
    }
    put(ROUTER, ZmqChannel.ROUTER(ctx).with(router).build()).watchRecv(_poller);
  }

  @Override
  public void exec() throws Exception {
    poll();

    ZmqChannel master = get(MASTER);
    ZmqChannel slave = get(SLAVE);
    ZmqChannel router = get(ROUTER);

    if (router.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = router.recv(DONTWAIT);
        if (frames == null)
          break;

        byte[] payload = frames.getPayload();
        ZmqFrames route = frames.getIdentities();
        if (isPing(payload)) {
          if (route.size() == 1) {
            LOGGER.info("Got PING, route.hash={}.", makeHash(route.get(0)));
            slaveRouting.putRouting(route);
            {
              // Send PONG back, use identities [route|master_identity].
              ZmqFrames masterRoute = new ZmqFrames();
              masterRoute.add(route.get(0));
              masterRoute.add(getMasterIdentity());
              router.route(masterRoute, PONG, DONTWAIT);
              LOGGER.info("Send PONG back, route.hash={}.", makeHash(this.master.identity()));
            }
          }
          else {
            LOGGER.error("Wrong PING! Got route.size={}.", route.size());
          }
        }
        else {
          logTraffic("router", payload);
          processor.onMessage(route,
                              payload,
                              router,
                              masterRouting,
                              slaveRouting,
                              getMasterIdentity(),
                              getSlaveIdentity());
        }
      }
    }

    if (slave != null) {
      if (!slave.canRecv()) {
        // Send PING (dont send blindly, check "timer" before send, e.g. every X seconds).
        for (String connectAddr : this.slave.connectAddr()) {
          slave.route(new ZmqFrames(), PING, DONTWAIT);
          LOGGER.info("Send PING on {}.", connectAddr);
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
              LOGGER.info("Got PONG, route.hash={}.", makeHash(route.get(0)));
              masterRouting.putRouting(route);
            }
            else {
              LOGGER.error("Wrong PONG! Got route.size={}.", route.size());
            }
          }
          else {
            logTraffic("master", payload);
            processor.onMasterMessage(route,
                                      payload,
                                      router,
                                      masterRouting,
                                      slaveRouting,
                                      getMasterIdentity(),
                                      getSlaveIdentity());
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
          processor.onSlaveMessage(route,
                                   payload,
                                   router,
                                   masterRouting,
                                   slaveRouting,
                                   getMasterIdentity(),
                                   getSlaveIdentity());
        }
      }
    }
  }

  private byte[] getSlaveIdentity() {
    return slave != null ? slave.identity() : null;
  }

  private byte[] getMasterIdentity() {
    return master != null ? master.identity() : null;
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
}
