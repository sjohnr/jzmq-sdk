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
