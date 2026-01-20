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

import java.lang.reflect.Field;
import java.util.Properties;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticationFilterSuite {

  @Test
  public void bridgesJakartaRequestResponseForHadoopAuthHandler() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    CapturingAuthHandler handler = new CapturingAuthHandler();
    setField(filter, "authHandler", handler);

    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse res = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    when(req.getScheme()).thenReturn("https");
    when(req.getHeader("X-Test")).thenReturn("ok");
    when(req.getCookies()).thenReturn(null);
    when(req.getRequestURL()).thenReturn(new StringBuffer("http://example"));
    when(req.getQueryString()).thenReturn(null);

    filter.doFilter(req, res, chain);

    Assertions.assertEquals("https", handler.seenScheme);
    Assertions.assertEquals("ok", handler.seenHeader);
    verify(res).setHeader("X-From-Auth", "yes");
    verify(chain).doFilter(any(), any());
  }

  @Test
  public void wrapsShadedServletExceptionFromInit() throws Exception {
    TestableAuthenticationFilter filter = new TestableAuthenticationFilter();
    FilterConfig filterConfig = mock(FilterConfig.class);

    jakarta.servlet.ServletException ex = Assertions.assertThrows(
      jakarta.servlet.ServletException.class,
      () -> filter.callInitializeAuthHandler(
        ThrowingInitAuthHandler.class.getName(), filterConfig));

    Assertions.assertTrue(
      ex.getCause() instanceof org.apache.hadoop.shaded.javax.servlet.ServletException);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static final class TestableAuthenticationFilter extends AuthenticationFilter {
    void callInitializeAuthHandler(String className, FilterConfig filterConfig)
        throws jakarta.servlet.ServletException {
      initializeAuthHandler(className, filterConfig);
    }
  }

  private static final class CapturingAuthHandler implements AuthenticationHandler {
    String seenScheme;
    String seenHeader;

    @Override
    public void init(Properties config)
        throws org.apache.hadoop.shaded.javax.servlet.ServletException {
    }

    @Override
    public String getType() {
      return "simple";
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean managementOperation(
        AuthenticationToken token,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletRequest request,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletResponse response) {
      seenScheme = request.getScheme();
      seenHeader = request.getHeader("X-Test");
      response.setHeader("X-From-Auth", "yes");
      return true;
    }

    @Override
    public AuthenticationToken authenticate(
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletRequest request,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletResponse response)
        throws AuthenticationException {
      return AuthenticationToken.ANONYMOUS;
    }
  }

  public static final class ThrowingInitAuthHandler implements AuthenticationHandler {
    @Override
    public void init(Properties config)
        throws org.apache.hadoop.shaded.javax.servlet.ServletException {
      throw new org.apache.hadoop.shaded.javax.servlet.ServletException("boom");
    }

    @Override
    public String getType() {
      return "simple";
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean managementOperation(
        AuthenticationToken token,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletRequest request,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletResponse response) {
      return true;
    }

    @Override
    public AuthenticationToken authenticate(
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletRequest request,
        org.apache.hadoop.shaded.javax.servlet.http.HttpServletResponse response)
        throws AuthenticationException {
      return AuthenticationToken.ANONYMOUS;
    }
  }
}
