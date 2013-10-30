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

import org.zeromq.messaging.ZmqChannelFactory;
import org.zeromq.support.IsPrototype;
import org.zeromq.support.ObjectAdapter;

/**
 * Emitter-worker device:
 * <pre>
 *   <-w(DEALER) / with-ping
 * </pre>
 */
public final class WorkerAnonymEmitter extends ZmqAbstractWorker implements IsPrototype {

  private static final int DEFAULT_NUM_OF_COPIES = 2;

  public static class Builder extends ZmqAbstractWorker.Builder<Builder, WorkerAnonymEmitter> {

    private Builder() {
      super(new WorkerAnonymEmitter());
    }

    public Builder withWorkerIdentity(String workerIdentity) {
      _target.setWorkerIdentity(workerIdentity);
      return this;
    }

    public Builder withWorkerIdentityConverter(ObjectAdapter<String, byte[]> workerIdentityConverter) {
      _target.setWorkerIdentityConverter(workerIdentityConverter);
      return this;
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      assert !_target.connectAddresses.isEmpty();
      if (_target.workerIdentity != null) {
        assert _target.workerIdentityConverter != null;
      }
    }
  }

  private int numOfCopies = DEFAULT_NUM_OF_COPIES;
  private String workerIdentity;
  private ObjectAdapter<String, byte[]> workerIdentityConverter = new ObjectAdapter<String, byte[]>() {
    @Override
    public byte[] convert(String src) {
      return src.getBytes();
    }
  };

  //// CONSTRUCTORS

  public WorkerAnonymEmitter() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setWorkerIdentity(String workerIdentity) {
    this.workerIdentity = workerIdentity;
  }

  public void setWorkerIdentityConverter(ObjectAdapter<String, byte[]> workerIdentityConverter) {
    this.workerIdentityConverter = workerIdentityConverter;
  }

  public void setNumOfCopies(int numOfCopies) {
    if (numOfCopies > 0) {
      this.numOfCopies = numOfCopies;
    }
  }

  @Override
  public int numOfCopies() {
    return numOfCopies;
  }

  @Override
  public void init() {
    _pingStrategy = new DoPing();

    ZmqChannelFactory.Builder builder = ZmqChannelFactory.builder();
    if (workerIdentity != null) {
      builder.withSocketIdentityPrefix(workerIdentityConverter.convert(workerIdentity));
    }
    _channelFactory = builder
        .withZmqContext(zmqContext)
        .withEventListeners(eventListeners)
        .withConnectAddresses(connectAddresses)
        .ofDEALERType()
        .build();

    super.init();
  }
}
