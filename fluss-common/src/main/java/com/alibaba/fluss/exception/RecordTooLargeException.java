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

package com.alibaba.fluss.exception;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * This record is larger than the maximum allowable size.
 *
 * @since 0.1
 */
@PublicEvolving
public class RecordTooLargeException extends ApiException {
    private static final long serialVersionUID = 1L;

    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordTooLargeException(String message) {
        super(message);
    }

    public RecordTooLargeException(Throwable cause) {
        super(cause);
    }
}
