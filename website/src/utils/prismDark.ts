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

import type {PrismTheme} from 'prism-react-renderer';

/**
 * Dark Prism theme matching the Apache Fluss deep-blue palette.
 * Used for code blocks rendered in dark contexts (e.g. dark code cards).
 */
const darkTheme: PrismTheme = {
  plain: {
    color: '#DBEAFE',           // --fluss-blue-100
    backgroundColor: '#06122B', // --fluss-blue-950
  },
  styles: [
    {
      types: ['comment', 'prolog', 'doctype', 'cdata'],
      style: {color: '#64748B', fontStyle: 'italic'},
    },
    {
      types: ['namespace'],
      style: {opacity: 0.7},
    },
    {
      types: ['string', 'attr-value', 'char', 'inserted'],
      style: {color: '#A3E635'}, // --fluss-lime
    },
    {
      types: ['number', 'boolean'],
      style: {color: '#FBBF24'},
    },
    {
      types: ['keyword', 'atrule', 'selector'],
      style: {color: '#93C5FD'}, // --fluss-blue-300
    },
    {
      types: ['function', 'class-name', 'tag'],
      style: {color: '#22D3EE'}, // --fluss-cyan
    },
    {
      types: ['builtin', 'constant', 'variable', 'property'],
      style: {color: '#C4B5FD'},
    },
    {
      types: ['operator', 'punctuation'],
      style: {color: '#94A3B8'},
    },
    {
      types: ['regex', 'important', 'deleted'],
      style: {color: '#FB7185'},
    },
    {
      types: ['attr-name'],
      style: {color: '#60A5FA'},
    },
    {
      types: ['symbol', 'url'],
      style: {color: '#67E8F9'},
    },
  ],
};

export default darkTheme;
