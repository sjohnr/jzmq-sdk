package org.zeromq.messaging.service;

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqFrames;

import static com.google.common.base.Preconditions.checkState;
import static org.zeromq.ZMQ.DONTWAIT;

@SuppressWarnings("unchecked")
public abstract class AbstractProcessor<T extends AbstractProcessor> implements Processor<T> {

  /**
   * Original message route.
   * Use following functions to work with this field: {@link #origin()}, {@link #root()}.
   */
  protected ZmqFrames route;
  /** Message payload. */
  protected byte[] payload;
  /** {@code router} channel. */
  private ZmqChannel router;
  /**
   * Combined "routing tables": {@code masterRouting} at 0 and {@code slaveRouting} at 1.
   * Use following functions to work with this field: {@link #masterRouting()}, {@link #slaveRouting()}.
   */
  private Routing[] routings;
  /**
   * Combined "identities": {@code masterIdentity} at 0 and {@code slaveIdentity} at 1.
   * Use following functions to work with this field: {@link #thisMaster()}, {@link #thisSlave()}.
   */
  private Object[] identities;

  @Override
  public final T set(ZmqFrames route) {
    this.route = route;
    return (T) this;
  }

  @Override
  public final T set(byte[] payload) {
    this.payload = payload;
    return (T) this;
  }

  @Override
  public final T set(ZmqChannel router) {
    this.router = router;
    return (T) this;
  }

  @Override
  public final T set(Routing[] routings) {
    this.routings = routings;
    return (T) this;
  }

  @Override
  public final T set(Object[] identities) {
    this.identities = identities;
    return (T) this;
  }

  /**
   * Indicates that received {@link #route} and {@link #payload} are coming from some outer source,
   * i.e. neither from master and nor from slave.
   */
  @Override
  public void onRoot() {
    throw new UnsupportedOperationException();
  }

  /** Indicates that received {@link #route} and {@link #payload} are coming from master node. */
  @Override
  public void onMaster() {
    throw new UnsupportedOperationException();
  }

  /** Indicates that received {@link #route} and {@link #payload} are coming from slave node. */
  @Override
  public void onSlave() {
    throw new UnsupportedOperationException();
  }

  protected final Routing masterRouting() {
    Routing routing = routings[0];
    checkState(routing != null);
    return routing;
  }

  protected final Routing slaveRouting() {
    Routing routing = routings[1];
    checkState(routing != null);
    return routing;
  }

  protected final byte[] thisMaster() {
    Object identity = identities[0];
    checkState(identity != null);
    return (byte[]) identity;
  }

  protected final byte[] thisSlave() {
    Object identity = identities[1];
    checkState(identity != null);
    return (byte[]) identity;
  }

  protected final byte[] origin() {
    checkState(!route.isEmpty());
    return route.get(0);
  }

  protected final byte[] root() {
    checkState(!route.isEmpty());
    return route.get(route.size() - 1);
  }

  /**
   * Constructs new master route. Resulting frames shall consist of master identity, slave identity and original message route.
   * <pre>
   *                master --> [WHERE-TO-GO]
   *                 slave --> [WHERE-TO-REPLY]
   *    route(all, if any) --> [...]
   * </pre>
   *
   * @return frames or null in case if route can't be built.
   */
  protected final ZmqFrames nextMasterRoute() {
    if (routings[0].available() == 0)
      return null;

    ZmqFrames frames = new ZmqFrames();
    frames.add(routings[0].get(root(), payload)); // where-to-go.
    frames.add(thisSlave()); // where-reply-to (i.e. on this slave).
    frames.addAll(route); // put all rest.
    return frames;
  }

  /**
   * Constructs new slave route. Resulting frames shall consist of slave identity, master identity and original message route.
   * <pre>
   *                  slave --> [WHERE-TO-GO]
   *                 master --> [WHERE-TO-REPLY]
   *     route(all, if any) --> [...]
   * </pre>
   *
   * @return frames or null in case if route can't be built.
   */
  protected final ZmqFrames nextSlaveRoute() {
    if (routings[1].available() == 0)
      return null;

    ZmqFrames frames = new ZmqFrames();
    frames.add(routings[1].get(root(), payload)); // where-to-go.
    frames.add(thisMaster()); // where-reply-to (i.e. on this master).
    frames.addAll(route); // put all rest.
    return frames;
  }

  /** Shortcut method. Takes existing {@link #route} and {@link #payload} and routes them. */
  public final void route() {
    router.route(route, payload, DONTWAIT);
  }
}
