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

package org.zeromq.messaging;

import com.google.common.base.Objects;
import org.zeromq.ZMQ;

import static com.google.common.base.Preconditions.checkArgument;

public final class ZmqException extends RuntimeException {

  private static final long serialVersionUID = ZmqException.class.getCanonicalName().hashCode();

  public static enum ErrorCode {
    SEE_CAUSE,
    FATAL,
    CONTEXT_NOT_ACCESSIBLE,
    HEADER_IS_NOT_SET,
    WRONG_HEADER,
    WRONG_MESSAGE,
    NATIVE_ERROR
  }

  private final ErrorCode code;
  private final ZMQ.Error nativeError;

  //// CONSTRUCTORS

  private ZmqException(ErrorCode code) {
    this.code = code;
    this.nativeError = null;
  }

  private ZmqException(Throwable e) {
    super(e);
    checkArgument(e != null);
    this.code = ErrorCode.SEE_CAUSE;
    this.nativeError = null;
  }

  private ZmqException(ZMQ.Error nativeError) {
    checkArgument(nativeError != null);
    this.code = ErrorCode.NATIVE_ERROR;
    this.nativeError = nativeError;
  }

  //// METHODS

  public static ZmqException seeCause(Throwable e) {
    return new ZmqException(e);
  }

  public static ZmqException fatal() {
    return new ZmqException(ErrorCode.FATAL);
  }

  public static ZmqException contextNotAccessible() {
    return new ZmqException(ErrorCode.CONTEXT_NOT_ACCESSIBLE);
  }

  public static ZmqException headerIsNotSet() {
    return new ZmqException(ErrorCode.HEADER_IS_NOT_SET);
  }

  public static ZmqException wrongHeader() {
    return new ZmqException(ErrorCode.WRONG_HEADER);
  }

  public static ZmqException wrongMessage() {
    return new ZmqException(ErrorCode.WRONG_MESSAGE);
  }

  public static ZmqException wrappedNative(ZMQ.Error nativeError) {
    return new ZmqException(nativeError);
  }

  public ErrorCode code() {
    return code;
  }

  public ZMQ.Error nativeError() {
    return nativeError;
  }

  @Override
  public String getMessage() {
    return Objects.toStringHelper(this)
                  .add("code", code)
                  .add("nativeError", nativeError)
                  .toString();
  }
}
