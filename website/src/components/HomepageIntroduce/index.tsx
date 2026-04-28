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

import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type ValueProp = {
  title: string;
  body: string;
  Icon: React.ComponentType<React.ComponentProps<'svg'>>;
};

const VALUE_PROPS: ValueProp[] = [
  {
    title: 'Sub-second freshness',
    body: 'Columnar streaming storage means analytics, features, and agents see new rows in seconds — no commit lag, no batch window, no lambda layer.',
    Icon: require('@site/static/img/feature_real_time.svg').default,
  },
  {
    title: 'One copy across hot and cold',
    body: 'The hot Fluss tier and the cold open-format tier (Iceberg, Paimon, Lance) share a logical schema. One union read; one source of truth.',
    Icon: require('@site/static/img/feature_lake.svg').default,
  },
  {
    title: 'Logs + tables on one substrate',
    body: 'Append-only logs for events. Primary-key tables for live state. Sub-millisecond point lookups served from the same leader that owns the log.',
    Icon: require('@site/static/img/feature_update.svg').default,
  },
  {
    title: 'Stateless, elastic compute',
    body: 'State lives on the Fluss leader, not in Flink task slots. Recovery collapses from minutes to seconds; topologies cost up to 85% less than Kafka-based equivalents.',
    Icon: require('@site/static/img/feature_lookup.svg').default,
  },
];

function ValuePropCard({title, body, Icon}: ValueProp) {
  return (
    <div className={styles.card}>
      <div className={styles.cardIcon} aria-hidden="true">
        <Icon role="img" />
      </div>
      <Heading as="h3" className={styles.cardTitle}>{title}</Heading>
      <p className={styles.cardBody}>{body}</p>
    </div>
  );
}

export default function HomepageIntroduce(): JSX.Element {
  return (
    <section className={styles.introduce}>
      <div className={clsx('container', styles.container)}>
        <div className={styles.header}>
          <span className={styles.eyebrow}>What is Apache Fluss?</span>
          <Heading as="h2" className={styles.title}>
            One substrate. Streams, tables, and the lake.
          </Heading>
          <p className={styles.lead}>
            <b>Apache Fluss (Incubating)</b> is an open-source columnar streaming
            storage system. It collapses the message broker, online KV store,
            stream-processing state backend, and lakehouse offline store into a
            single coherent foundation — so the systems above it don&apos;t have
            to keep them in sync.
          </p>
        </div>

        <div className={styles.grid}>
          {VALUE_PROPS.map((vp) => (
            <ValuePropCard key={vp.title} {...vp} />
          ))}
        </div>
      </div>
    </section>
  );
}
