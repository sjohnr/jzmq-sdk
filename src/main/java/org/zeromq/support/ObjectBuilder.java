package org.zeromq.support;

/** Abstraction around object-creation. */
public interface ObjectBuilder<T> {

  T build();
}
