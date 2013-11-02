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

package org.zeromq.support.exception;

import com.google.common.base.Preconditions;
import org.zeromq.support.InTheChain;

public abstract class AbstractExceptionHandlerInTheChain implements ExceptionHandler, InTheChain<ExceptionHandler> {

  /**
   * Next exception handler in the chain.
   * <p/>
   * <b>NOTE: by default initialized to {@link UncaughtExceptionHandler}. This field isn't optional.</b>
   */
  private ExceptionHandler nextHandler = new UncaughtExceptionHandler();

  @Override
  public final AbstractExceptionHandlerInTheChain withNext(ExceptionHandler nextHandler) {
    Preconditions.checkArgument(nextHandler != null);
    this.nextHandler = nextHandler;
    return this;
  }

  @Override
  public final ExceptionHandler next() {
    return nextHandler;
  }
}
