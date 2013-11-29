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

package org.zeromq.support.spring;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;

public final class SpringFixture {

  private AbstractApplicationContext _springContext;
  private ZmqSpringContext _zmqSpringContext;

  @SuppressWarnings("unchecked")
  public final <T> T getBean(String beanId) {
    return (T) _springContext.getBean(beanId);
  }

  public SpringFixture setup(Class<?> clazz) {
    ContextConfiguration annotation = clazz.getAnnotation(ContextConfiguration.class);
    _springContext = new ClassPathXmlApplicationContext(annotation.locations());
    _zmqSpringContext = new ZmqSpringContext(_springContext);
    _zmqSpringContext.deploy();
    return this;
  }

  public SpringFixture cleanup() {
    if (_zmqSpringContext != null) {
      _zmqSpringContext.destroy();
    }
    return this;
  }
}
