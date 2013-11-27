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

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
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

    public Builder withIdentity(String identity) {
      _target.setIdentity(identity);
      return this;
    }

    public Builder withIdentityConverter(ObjectAdapter<String, byte[]> identityConverter) {
      _target.setIdentityConverter(identityConverter);
      return this;
    }

    @Override
    public void checkInvariant() {
      super.checkInvariant();
      if (_target.connectAddresses.isEmpty()) {
        throw ZmqException.fatal();
      }
      if (_target.identity != null) {
        if (_target.identityConverter == null) {
          throw ZmqException.fatal();
        }
      }
    }
  }

  private int numOfCopies = DEFAULT_NUM_OF_COPIES;
  private String identity;
  private ObjectAdapter<String, byte[]> identityConverter = new ObjectAdapter<String, byte[]>() {
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

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public void setIdentityConverter(ObjectAdapter<String, byte[]> identityConverter) {
    this.identityConverter = identityConverter;
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

    ZmqChannel.Builder builder = ZmqChannel.builder();
    if (identity != null) {
      builder.withSocketIdentityPrefix(identityConverter.convert(identity));
    }
    _channel = builder.withZmqContext(zmqContext)
                      .withConnectAddresses(connectAddresses)
                      .ofDEALERType()
                      .build();

    super.init();
  }
}
