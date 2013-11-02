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

import com.google.common.base.Preconditions;

public final class ZmqException extends RuntimeException {

  private static final long serialVersionUID = ZmqException.class.getCanonicalName().hashCode();

  public static enum ErrorCode {
    SEE_CAUSE,
    FATAL,
    CONTEXT_NOT_ACCESSIBLE,
    SOCKET_IDENTITY_STORAGE_IS_EMPTY,
    SOCKET_IDENTITY_NOT_MATCHED,
    HEADER_IS_NOT_SET,
    WRONG_HEADER,
    WRONG_MESSAGE,
    FAILED_AT_SEND,
    FAILED_AT_RECV
  }

  private final ErrorCode errorCode;

  //// CONSTRUCTORS

  private ZmqException(ErrorCode errorCode) {
    this.errorCode = errorCode;
  }

  private ZmqException(Throwable e) {
    super(e);
    Preconditions.checkArgument(e != null);
    this.errorCode = ErrorCode.SEE_CAUSE;
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

  public static ZmqException socketIdentityStorageIsEmpty() {
    return new ZmqException(ErrorCode.SOCKET_IDENTITY_STORAGE_IS_EMPTY);
  }

  public static ZmqException socketIdentityNotMatched() {
    return new ZmqException(ErrorCode.SOCKET_IDENTITY_NOT_MATCHED);
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

  public static ZmqException failedAtSend() {
    return new ZmqException(ErrorCode.FAILED_AT_SEND);
  }

  public static ZmqException failedAtRecv() {
    return new ZmqException(ErrorCode.FAILED_AT_RECV);
  }

  public ErrorCode errorCode() {
    return errorCode;
  }

  @Override
  public String getMessage() {
    return "errorCode=" + errorCode;
  }
}
