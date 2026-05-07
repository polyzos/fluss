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

import React from 'react';
import OriginalColorModeToggle from '@theme-original/ColorModeToggle';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

/**
 * Wraps the default Docusaurus colour-mode toggle so it doesn't render at
 * all on the landing page. On every other route the original toggle is
 * rendered unchanged. The match handles both bare `/` and the baseUrl
 * variant (e.g. `/fluss/`) plus optional trailing-slash differences.
 */
export default function ColorModeToggleWrapper(props) {
    const {pathname} = useLocation();
    const {siteConfig: {baseUrl}} = useDocusaurusContext();

    const normalize = (p) => (p.endsWith('/') ? p : p + '/');
    const isHome = normalize(pathname) === normalize(baseUrl);

    if (isHome) {
        return null;
    }
    return <OriginalColorModeToggle {...props} />;
}
