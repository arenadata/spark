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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MetricsAuthFilter}. The SPNEGO delegate is replaced
 * with a {@link RecordingFilter} via the package-private {@code createUiDelegate()}
 * seam, so no Kerberos/Hadoop machinery is needed.
 */
class MetricsAuthFilterSuite {

  private static final String TOKEN = "s3cr3t-shared-token";

  // ---- /metrics/* : bearer / open -----------------------------------------

  @Test
  void metricsWithCorrectBearerPassesThrough() throws Exception {
    RecordingFilter delegate = new RecordingFilter();
    MetricsAuthFilter f = newFilter(params("token", TOKEN), delegate);

    HttpServletRequest req = metricsRequest("Bearer " + TOKEN);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(chain, times(1)).doFilter(req, resp);
    verify(resp, never()).sendError(org.mockito.ArgumentMatchers.anyInt(),
        org.mockito.ArgumentMatchers.anyString());
    assertEquals(0, delegate.doFilterCalls, "metrics path must never hit the UI delegate");
  }

  @Test
  void metricsWithWrongTokenIsRejected() throws Exception {
    MetricsAuthFilter f = newFilter(params("token", TOKEN), new RecordingFilter());

    HttpServletRequest req = metricsRequest("Bearer not-the-token");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(resp).setHeader("WWW-Authenticate", "Bearer");
    verify(resp).sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "Invalid or missing bearer token");
    verify(chain, never()).doFilter(req, resp);
  }

  @Test
  void metricsWithMissingHeaderIsRejected() throws Exception {
    MetricsAuthFilter f = newFilter(params("token", TOKEN), new RecordingFilter());

    HttpServletRequest req = metricsRequest(null); // no Authorization header
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(resp).sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "Invalid or missing bearer token");
    verify(chain, never()).doFilter(req, resp);
  }

  @Test
  void metricsOpenWhenNoTokenConfigured() throws Exception {
    MetricsAuthFilter f = newFilter(Collections.emptyMap(), new RecordingFilter());

    HttpServletRequest req = metricsRequest(null); // no credentials at all
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(chain, times(1)).doFilter(req, resp);
    verify(resp, never()).sendError(org.mockito.ArgumentMatchers.anyInt(),
        org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  void bearerOfDifferentLengthIsRejected() throws Exception {
    // Guards the constant-time MessageDigest.isEqual path against length mismatch.
    MetricsAuthFilter f = newFilter(params("token", TOKEN), new RecordingFilter());

    HttpServletRequest req = metricsRequest("Bearer short");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(resp).sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "Invalid or missing bearer token");
    verify(chain, never()).doFilter(req, resp);
  }

  // ---- non-metrics : fall-through vs SPNEGO delegate ----------------------

  @Test
  void nonMetricsFallsThroughWhenNoSpnego() throws Exception {
    RecordingFilter delegate = new RecordingFilter();
    MetricsAuthFilter f = newFilter(params("token", TOKEN), delegate);

    HttpServletRequest req = uiRequest("/jobs/");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    verify(chain, times(1)).doFilter(req, resp);
    assertEquals(0, delegate.doFilterCalls, "delegate not created without spnego.type");
  }

  @Test
  void nonMetricsIsDelegatedWhenSpnegoConfigured() throws Exception {
    RecordingFilter delegate = new RecordingFilter();
    Map<String, String> p = params("token", TOKEN);
    p.put("spnego.type", "simple");
    MetricsAuthFilter f = newFilter(p, delegate);

    HttpServletRequest req = uiRequest("/jobs/");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    assertEquals(1, delegate.doFilterCalls, "UI path must reach the SPNEGO delegate");
    verify(chain, times(1)).doFilter(req, resp); // RecordingFilter continues the chain
  }

  @Test
  void metricsNeverReachesDelegateEvenWhenSpnegoConfigured() throws Exception {
    RecordingFilter delegate = new RecordingFilter();
    Map<String, String> p = params("token", TOKEN);
    p.put("spnego.type", "simple");
    MetricsAuthFilter f = newFilter(p, delegate);

    HttpServletRequest req = metricsRequest("Bearer " + TOKEN);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    f.doFilter(req, resp, chain);

    assertEquals(0, delegate.doFilterCalls, "scraper must never be challenged with SPNEGO");
    verify(chain, times(1)).doFilter(req, resp);
  }

  // ---- PrefixedFilterConfig : prefix stripping + token hiding --------------

  @Test
  void delegateSeesStrippedParamsAndNotToken() throws Exception {
    RecordingFilter delegate = new RecordingFilter();
    Map<String, String> p = params("token", TOKEN);
    p.put("spnego.type", "kerberos");
    p.put("spnego.kerberos.principal", "HTTP/_HOST@REALM");
    MetricsAuthFilter f = newFilter(p, delegate);

    FilterConfig seen = delegate.initConfig;
    assertEquals("kerberos", seen.getInitParameter("type"), "prefix must be stripped");
    assertEquals("HTTP/_HOST@REALM", seen.getInitParameter("kerberos.principal"));
    assertNull(seen.getInitParameter("token"), "delegate must not see our own token");
    assertNull(seen.getInitParameter("spnego.type"), "prefixed name must not leak");

    List<String> names = Collections.list(seen.getInitParameterNames());
    assertTrue(names.contains("type"));
    assertTrue(names.contains("kerberos.principal"));
    assertFalse(names.contains("token"));
    assertFalse(names.contains("spnego.type"));
  }

  // ---- helpers ------------------------------------------------------------

  private static MetricsAuthFilter newFilter(Map<String, String> initParams,
      Filter delegate) throws ServletException {
    MetricsAuthFilter f = new MetricsAuthFilter() {
      @Override
      Filter createUiDelegate() {
        return delegate;
      }
    };
    f.init(new MapFilterConfig(initParams));
    return f;
  }

  private static HttpServletRequest metricsRequest(String authorization) {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn("/metrics/prometheus/");
    when(req.getHeader("Authorization")).thenReturn(authorization);
    return req;
  }

  private static HttpServletRequest uiRequest(String uri) {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn(uri);
    return req;
  }

  private static Map<String, String> params(String k, String v) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put(k, v);
    return m;
  }

  /** A {@link Filter} that records how it was init'd and continues the chain. */
  private static final class RecordingFilter implements Filter {
    private FilterConfig initConfig;
    private int doFilterCalls;

    @Override
    public void init(FilterConfig filterConfig) {
      this.initConfig = filterConfig;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      doFilterCalls++;
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
  }

  /** Minimal {@link FilterConfig} backed by a map, for driving {@code init}. */
  private static final class MapFilterConfig implements FilterConfig {
    private final Map<String, String> params;

    MapFilterConfig(Map<String, String> params) {
      this.params = params;
    }

    @Override
    public String getFilterName() {
      return "MetricsAuthFilter";
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }

    @Override
    public String getInitParameter(String name) {
      return params.get(name);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(params.keySet());
    }
  }
}
