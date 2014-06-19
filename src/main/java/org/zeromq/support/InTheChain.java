package org.zeromq.support;

public interface InTheChain<E> {

  Object withNext(E o);

  Object next();
}
