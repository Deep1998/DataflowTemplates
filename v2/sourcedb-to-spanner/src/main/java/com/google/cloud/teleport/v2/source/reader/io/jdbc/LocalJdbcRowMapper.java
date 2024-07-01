/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc;

import java.io.Serializable;
import java.sql.ResultSet;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

public class LocalJdbcRowMapper<T> implements JdbcIO.RowMapper<T>, Serializable {
  private final JdbcIO.RowMapper<T> delegate;

  public LocalJdbcRowMapper(JdbcIO.RowMapper<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public T mapRow(ResultSet resultSet) throws Exception {
    return delegate.mapRow(resultSet);
  }
}
