# ADR-0001: Build the platform on a financial market data domain

- **Status:** Accepted
- **Date:** 2026-10-26
- **Deciders:** Ron Klem

## Status

Accepted

## Context

This repository exists to demonstrate platform engineering judgment — the
ability to design, govern, operate, and evolve a data and ML platform — rather
than to ship a specific analytical result. That goal creates an unusual
requirement: the **business domain is an instrument**, chosen to exercise
particular engineering problems, not an end in itself.

A domain that fails to exercise those problems produces a project that looks
competent and demonstrates nothing. Most portfolio datasets fail this way.
The Titanic and Iris datasets are static, tiny, and have no time dimension. A
scraped e-commerce catalogue has no ground truth and no governance obligations.
A synthetic IoT feed has volume but no semantics — nothing about it is *wrong*
in an interesting way, so data quality work on it is theatre.

The specific engineering problems this platform needs to demonstrate, and what a
domain must supply to make each one real rather than simulated:

| Problem to demonstrate | What the domain must supply |
|---|---|
| Incremental and late-arriving data | Continuous append with revisions to already-published values |
| Streaming architecture | A genuine real-time variant of the same data, not a batch file replayed |
| Data quality frameworks | Failure modes that are *semantically* wrong while structurally valid |
| Point-in-time correctness | Values that are restated after initial publication |
| Governance and lineage | A real reason to restrict access and prove provenance |
| ML platform | A forecasting target with an honest, non-trivial baseline |
| Cost and capacity engineering | Volume that grows without bound along a time axis |

The last column is the hard part. Most domains supply volume; few supply
*correctness hazards* that cannot be designed away.

### Why financial market and economic data satisfies this

**It has a real point-in-time problem.** This is the decisive property. Economic
series published by FRED are *revised*: an initial GDP or non-farm payroll
estimate is superseded by a second and third estimate weeks later. A naive
pipeline that overwrites the value loses the fact that on the original date, the
world believed something different.

This makes a whole class of platform work genuinely necessary rather than
decorative:

- **Bitemporal modelling** — every fact needs both an event time and a
  knowledge time, because "what was GDP in Q1" and "what did we believe GDP was
  in Q1, as of May" are different questions with different answers.
- **Honest ML evaluation** — training a model on revised figures and evaluating
  it on history produces look-ahead bias. The model appears to work and would
  have been unusable in production. Reproducing a training set *as it was known*
  on a given date requires platform support, not a notebook convention.
- **Immutable raw layer** — the medallion architecture's `raw` zone stops being
  a convention and becomes load-bearing: it is the only record of what the
  source actually said.

A domain without revisions cannot motivate any of this. With financial data, a
platform that ignores it is *demonstrably wrong*, which is exactly the property
needed.

**Data quality failures are semantic, not structural.** A market data feed
rarely delivers a malformed row. It delivers a structurally perfect row that is
wrong: a split-adjusted price next to an unadjusted volume, a stale quote
repeated through a halt, a close outside the day's high-low range, a ticker
reused after a delisting. Catching these requires domain-aware expectations
(`low <= close <= high`, volume continuity, corporate-action reconciliation)
rather than schema validation. That is the difference between a data quality
framework and a type checker.

**Both batch and streaming are native to the domain, not contrived.** Daily OHLCV
bars are genuinely batch. Tick and quote events are genuinely streaming. They
describe the same underlying reality at different granularities, which creates a
real reconciliation problem — do the streaming aggregates agree with the batch
bars? — and a real motivation for a Lambda or Kappa architecture discussion.
Many projects bolt streaming on; here it is intrinsic.

**Governance has an authentic motive.** Market data is licensed, access-controlled
and audited in practice. Unity Catalog's lineage, column masking, and grant
model map onto obligations a financial institution actually has, rather than
onto a hypothetical.

**Volume grows along a time axis, unbounded but controllable.** Daily bars for a
few thousand symbols over twenty years is tens of millions of rows — small
enough for a laptop, which is what makes ADR-002 viable. Tick data for the same
universe is billions of rows, which is not. The same domain therefore spans both
sides of the local/distributed boundary, and crossing it is a measured decision
rather than a narrative device.

**It is free and reproducible.** Yahoo Finance and FRED require no paid licence
and no credentials for the historical series used here, so a contributor can
clone the repository and run the platform without an account. A portfolio
project gated behind a vendor contract cannot be independently verified, which
defeats the purpose.

### The domain risk, stated plainly

Financial data invites a specific failure mode: the project drifts into **trading
strategy research**. Backtests, Sharpe ratios, and alpha claims are a different
discipline, they are easy to get subtly and unfalsifiably wrong, and they are
not what this repository is demonstrating. The mitigation is in the Decision.

## Decision

**MarketPulse will be built on financial market and economic data** sourced from
Yahoo Finance (equity and ETF OHLCV), FRED (Treasury rates, inflation and
economic indicators), and simulated trade and quote events for streaming
workloads.

**The domain is a vehicle for platform engineering, and the platform is the
deliverable.** Three constraints enforce that:

1. **No trading strategies, backtests, or alpha claims.** ML work is limited to
   forecasting and anomaly detection with explicitly stated baselines, framed as
   exercises of the ML *platform* — feature lineage, experiment tracking,
   point-in-time training sets, model monitoring — not as investment research.
2. **Point-in-time correctness is treated as a platform requirement, not a
   modelling detail.** Revisions are preserved, not overwritten. A training set
   must be reconstructible as of a given knowledge date.
3. **Every dataset must justify itself by the platform capability it exercises.**
   A source that adds volume without adding an engineering problem is not added.

## Consequences

### Positive

- Point-in-time correctness, bitemporal modelling, and look-ahead bias become
  concrete requirements with observable failure modes, rather than topics.
- Data quality work is semantic and domain-aware, which is the kind that is hard
  and therefore worth demonstrating.
- Batch and streaming coexist naturally, making the reconciliation problem real.
- Governance and lineage map to obligations that exist in the industry.
- The domain spans both sides of the local/distributed boundary, so ADR-002's
  threshold is reached by real data growth rather than asserted.
- Zero cost to clone and run; independently verifiable.

### Negative

- **Domain knowledge is a prerequisite for contribution.** Corporate actions,
  split and dividend adjustment, and exchange calendars are genuinely intricate.
  A contributor without that background will find the transformation logic
  opaque. Mitigated by documenting the domain rules alongside the code that
  implements them, but not eliminated.
- **The strategy-research temptation is permanent,** not a one-time risk. It
  requires ongoing discipline in scoping decisions.
- **Free sources are unstable.** Yahoo Finance has no API contract; its
  interface has changed without notice before and will again. Mitigated by
  isolating source-specific logic behind connector interfaces and testing
  against recorded fixtures, so a source change breaks one connector rather than
  the platform. The live-call contract test is separately marked.
- **Correctness is easy to get wrong in ways that look right.** An unadjusted
  price series produces plausible charts and wrong analytics. This raises the bar
  on the data quality framework — which is the point, but it is also a real cost.

### Neutral

- Vendor-specific semantics (adjustment conventions, timestamp conventions) live
  in connectors and are not platform concerns.
- Exchange calendars and holiday handling add complexity that is genuine domain
  work rather than platform work.

## Alternatives Considered

### Alternative: A synthetic IoT or telemetry stream

**Rejected because** synthetic data has no ground truth and no semantic failure
modes. Every anomaly is one the generator was written to produce, so detecting
it validates nothing. It supplies volume and streaming but cannot supply
revisions, point-in-time correctness, or governance obligations — which is
where the interesting platform engineering is.

### Alternative: A public e-commerce or retail dataset

**Rejected because** the available datasets are static snapshots. No continuous
arrival, no late data, no revisions, and therefore no incremental processing,
no streaming, and no bitemporal problem. It would exercise dimensional modelling
and little else.

### Alternative: Public healthcare or census data

**Rejected because** the governance story is strong — arguably stronger — but
the data is published in large infrequent batches. Streaming would be entirely
contrived, and the revision cycle is too slow to exercise incremental
processing. Genuine PII handling would also dominate the project's scope without
advancing the platform engineering goal.

### Alternative: Cryptocurrency market data

**Rejected because** it is a strictly weaker version of this choice. It supplies
24/7 streaming and free APIs, but has no revision cycle, no exchange calendar,
no corporate actions, and a thinner governance story. Its principal advantage —
no market close — removes a genuine source of engineering complexity rather than
adding one.

### Alternative: Multiple unrelated domains, one per capability

**Rejected because** it would optimise each capability demonstration in
isolation at the cost of the thing being demonstrated. A platform's value is in
coherence: shared governance, shared lineage, shared observability, a single
promotion path. Several disconnected pipelines is a portfolio of projects, not a
platform.

### Alternative: Paid market data (Polygon, Databento, Bloomberg)

**Rejected because** data quality and reliability would improve materially, but
no third party could clone and run the repository. For a project whose purpose
is to be independently inspected, reproducibility outranks data quality.

## Revisit When

- **The platform engineering goal stops being served.** If the repository's
  activity shifts toward analytical findings about markets rather than platform
  capability, the domain has become the subject and this decision has failed.
- **Free sources become unworkable.** If Yahoo Finance access degrades such that
  a contributor cannot reproduce the pipeline, either substitute a free
  alternative or revisit the reproducibility-over-quality tradeoff above.
- **A capability cannot be demonstrated in this domain.** Graph processing and
  geospatial workloads are the plausible examples. Add a narrowly scoped
  secondary dataset for that capability; do not migrate the platform.
- **Licensing changes.** If a source's terms begin restricting redistribution of
  derived data, the public-repository premise needs re-examination.

## References

- [`project_vision.md`](../../project_vision.md) — objectives and phase roadmap
- [ADR-0002](0002-polars-duckdb-for-local-development.md) — local engine choice,
  which depends on this domain's volume characteristics
- [FRED ALFRED](https://alfred.stlouisfed.org/) — vintage economic series; the
  canonical illustration of the revision problem described above
