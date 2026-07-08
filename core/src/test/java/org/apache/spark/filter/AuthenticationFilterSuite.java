/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.filter;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.junit.Test;

/**
 * Unit tests for {@link AuthenticationFilter}, the thin wrapper around Hadoop's javax
 * AuthenticationFilter. The Hadoop delegate is replaced with a recording stub via the
 * package-private {@code createDelegate()} seam, so no Hadoop/Kerberos is needed.
 */
public class AuthenticationFilterSuite {

  @Test
  public void forwardsLifecycleAndRequestsToDelegate() throws Exception {
    RecordingFilter stub = new RecordingFilter();
    AuthenticationFilter filter = new AuthenticationFilter() {
      @Override
      Filter createDelegate() {
        return stub;
      }
    };

    FilterConfig cfg = mock(FilterConfig.class);
    filter.init(cfg);
    assertSame("init must be forwarded to the Hadoop delegate", cfg, stub.initConfig);

    ServletRequest req = mock(ServletRequest.class);
    ServletResponse resp = mock(ServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    filter.doFilter(req, resp, chain);
    verify(chain).doFilter(req, resp); // the stub continues the chain

    filter.destroy();
    assertTrue("destroy must be forwarded to the delegate", stub.destroyed);
  }

  private static final class RecordingFilter implements Filter {
    private FilterConfig initConfig;
    private boolean destroyed;

    @Override
    public void init(FilterConfig filterConfig) {
      this.initConfig = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
      this.destroyed = true;
    }
  }
}
