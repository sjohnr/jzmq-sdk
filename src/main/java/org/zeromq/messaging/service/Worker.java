package org.zeromq.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractActor;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqFrames;

import java.util.ArrayList;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;
import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.support.ZmqUtils.makeHash;

public final class Worker extends ZmqAbstractActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  public static final byte[] PING = "ping".getBytes();
  public static final byte[] PONG = "pong".getBytes();

  /**
   * Worker's {@code incoming} traffic gateway. Socket is -- bound {@code DEALER} with "identity".
   * <ul>
   * <li>via this socket must be expected "comeback" traffic from all connected ROUTERs</li>
   * </ul>
   * <pre>
   *   byte[] <--         <--conn-- *ROUTER
   *              DEALER: <--conn-- *ROUTER
   *   byte[] <--         <--conn-- *ROUTER
   * </pre>
   */
  private static final String COMEBACK = "comeback:";
  /**
   * Worker's {@code incoming} traffic gateway. Socket is -- connected {@code DEALER} with "identity".
   * <ul>
   * <li>via this socket must be expected incoming "caller" traffic from all connected ROUTERs</li>
   * <li>PING must be sent to all connected ROUTERs and PONG shall be expected back</li>
   * </ul>
   * <pre>
   *   byte[] <--         --[conn,PING]--> :ROUTER
   *              DEALER* --[conn,PING]--> :ROUTER
   *     PONG <--         --[conn,PING]--> :ROUTER
   * </pre>
   */
  private static final String ACCEPTOR = "acceptor*";
  /**
   * Worker's {@code outgoing} traffic gateway. It can {@code bind} and/or {@code connect}, in either case
   * this socket is -- {@code ROUTER} with "routing table".
   * <p/>
   * If {@code bind}:
   * <ul>
   * <li>via this socket must be expected outgoing "caller" traffic</li>
   * <li>"routing table" must contain socket identities of DEALERs (those who had sent PING here)</li>
   * <li>this socket has to serve PING from DEALERs and populate "routing table"</li>
   * <li>on PING from DEALER this socket shall send PONG back</li>
   * </ul>
   * <pre>
   *   byte[] -->         <--[conn,PING]-- *DEALER
   *     PING <-- ROUTER: <--[conn,PING]-- *DEALER
   *     PONG -->         <--[conn,PING]-- *DEALER
   * </pre>
   * If {@code connect}:
   * <ul>
   * <li>via this socket must be expected "comeback" traffic</li>
   * <li>"routing table" must contain socket identities of DEALERs (those who had send PONG on inbound socket)</li>
   * </ul>
   * <pre>
   *   byte[] -->         --conn--> :DEALER
   *              ROUTER* --conn--> :DEALER
   *   byte[] -->         --conn--> :DEALER
   * </pre>
   */
  private static final String OUTCOME = "outcome:*";

  public static final class Builder extends ZmqAbstractActor.Builder<Builder, Worker> {

    private Builder() {
      super(new Worker());
    }

    public Builder withIncomeProps(Props incomeProps) {
      _target.setIncomeProps(incomeProps);
      return this;
    }

    public Builder withComebackIdentity(String comebackIdentity) {
      _target.setComebackIdentity(comebackIdentity);
      return this;
    }

    public Builder withAcceptorIdentity(String acceptorIdentity) {
      _target.setAcceptorIdentity(acceptorIdentity);
      return this;
    }

    public Builder withOutcomeProps(Props outcomeProps) {
      _target.setOutcomeProps(outcomeProps);
      return this;
    }

    public Builder withOutcomeRouting(ZmqRouting outcomeRouting) {
      _target.setOutcomeRouting(outcomeRouting);
      return this;
    }

    public Builder withProcessor(ZmqProcessor processor) {
      _target.setProcessor(processor);
      return this;
    }
  }

  private Props incomeProps;
  private byte[] comebackIdentity;
  private byte[] acceptorIdentity;
  private Props outcomeProps;
  private ZmqRouting outcomeRouting;
  private ZmqProcessor processor;

  //// CONSTRUCTORS

  private Worker() {
  }

  //// METHODS

  public void setIncomeProps(Props incomeProps) {
    this.incomeProps = incomeProps;
  }

  public void setComebackIdentity(String comebackIdentity) {
    this.comebackIdentity = comebackIdentity.getBytes();
  }

  public void setAcceptorIdentity(String acceptorIdentity) {
    this.acceptorIdentity = acceptorIdentity.getBytes();
  }

  public void setOutcomeProps(Props outcomeProps) {
    this.outcomeProps = outcomeProps;
  }

  public void setOutcomeRouting(ZmqRouting outcomeRouting) {
    this.outcomeRouting = outcomeRouting;
  }

  public void setProcessor(ZmqProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void checkInvariant() {
    super.checkInvariant();
    if (incomeProps == null) {
      throw ZmqException.fatal();
    }
    if (incomeProps.bindAddr().size() > 1) {
      throw ZmqException.fatal();
    }
    if (comebackIdentity == null) {
      throw ZmqException.fatal();
    }
    if (acceptorIdentity == null) {
      throw ZmqException.fatal();
    }
    if (outcomeProps == null) {
      throw ZmqException.fatal();
    }
    if (outcomeProps.bindAddr().size() > 1) {
      throw ZmqException.fatal();
    }
    if (outcomeRouting == null) {
      throw ZmqException.fatal();
    }
    if (processor == null) {
      throw ZmqException.fatal();
    }
  }

  @Override
  public void init() {
    // Setup <<comeback>> DEALER.
    for (String bindAddr : incomeProps.bindAddr()) {
      ZmqChannel comeback = ZmqChannel.DEALER(ctx).withProps(Props.builder(incomeProps)
                                                                  .withBindAddr(bindAddr)
                                                                  .withConnectAddr(new ArrayList<String>())
                                                                  .withIdentity(comebackIdentity)
                                                                  .build())
                                      .build();
      put(COMEBACK, comeback).watchRecv(_poller);
    }
    // Setup <<acceptor>> DEALER.
    for (String connectAddr : incomeProps.connectAddr()) {
      ZmqChannel acceptor = ZmqChannel.DEALER(ctx).withProps(Props.builder(incomeProps)
                                                                  .withBindAddr(new ArrayList<String>())
                                                                  .withConnectAddr(connectAddr)
                                                                  .withIdentity(acceptorIdentity)
                                                                  .build())
                                      .build();
      put(ACCEPTOR, acceptor).watchRecv(_poller);
    }
    // Setup <<outcome>> ROUTER.
    put(OUTCOME, ZmqChannel.ROUTER(ctx).withProps(outcomeProps).build()).watchRecv(_poller);
  }

  @Override
  public void exec() throws Exception {
    poll();

    ZmqChannel acceptor = channel(ACCEPTOR);
    if (acceptor != null) {
      if (!acceptor.canRecv()) {
        // Send PING (dont send blindly, check "timer" before send).
        for (String connectAddr : incomeProps.connectAddr()) {
          acceptor.route(new ZmqFrames(), PING, DONTWAIT);
          LOGGER.info("Send PING on {}.", connectAddr);
        }
      }
      else {
        for (; ; ) {
          ZmqFrames frames = acceptor.recv(DONTWAIT);
          if (frames == null)
            break;

          byte[] payload = frames.getPayload();
          ZmqFrames route = frames.getIdentities();
          if (isPong(payload)) {
            checkState(route.size() == 1, "Wrong PONG! Got route.size=" + route.size());
            LOGGER.info("Got PONG, route.hash={}.", makeHash(route.get(0)));
            outcomeRouting.putRouting(route);
          }
          else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Got \"acceptor\" traffic (payload={} bytes).", payload.length);
            }
            processor.accept(route, payload, channel(OUTCOME), outcomeRouting);
          }
        }
      }
    }

    ZmqChannel outcome = channel(OUTCOME);
    if (outcome.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = outcome.recv(DONTWAIT);
        if (frames == null)
          break;

        byte[] payload = frames.getPayload();
        ZmqFrames route = frames.getIdentities();
        if (isPing(payload)) {
          checkState(route.size() == 1, "Wrong PING! Got route.size=" + route.size());
          LOGGER.info("Got PING, route.hash={}.", makeHash(route.get(0)));
          outcomeRouting.putRouting(route);
          // Send PONG back, use identities [route|comebackIdentity].
          ZmqFrames pongRoute = new ZmqFrames();
          pongRoute.add(route.get(0));
          pongRoute.add(comebackIdentity);
          outcome.route(pongRoute, PONG, DONTWAIT);
          LOGGER.info("Send PONG back, route.hash={}.", makeHash(comebackIdentity));
        }
      }
    }

    ZmqChannel comeback = channel(COMEBACK);
    if (comeback != null && comeback.canRecv()) {
      for (; ; ) {
        ZmqFrames frames = comeback.recv(DONTWAIT);
        if (frames == null)
          break;

        byte[] payload = frames.getPayload();
        ZmqFrames route = frames.getIdentities();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Got \"comeback\" traffic (payload={} bytes).", payload.length);
        }
        processor.comeback(route, payload, channel(OUTCOME), outcomeRouting);
      }
    }
  }

  private boolean isPing(byte[] payload) {
    return Arrays.equals(payload, PING);
  }

  private boolean isPong(byte[] payload) {
    return Arrays.equals(payload, PONG);
  }
}
