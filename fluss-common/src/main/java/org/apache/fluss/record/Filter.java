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

package org.apache.fluss.record;

import org.apache.fluss.annotation.Internal;

/**
 * Schema-aware filter that supports statistics-based filtering with tightly bound schema
 * statistics. This filter only provides batch-level filtering based on statistical information and
 * does not support row-level predicate evaluation.
 *
 * <p>The filter applies the underlying predicate to batch statistics to determine whether entire
 * batches can be filtered out based on their statistical properties.
 */
@Internal
public class Filter {

}