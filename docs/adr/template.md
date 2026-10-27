# ADR-NNNN: Title stated as a decision, not a topic

- **Status:** Proposed
- **Date:** YYYY-MM-DD
- **Deciders:** Who made this call
- **Supersedes:** ADR-NNNN, or omit
- **Superseded by:** ADR-NNNN, or omit

## Status

Proposed

<!--
One of: Proposed | Accepted | Deprecated | Superseded by ADR-NNNN | Rejected.
Accepted ADRs are superseded, never edited to change a conclusion. Editing
destroys the record of what was believed and why, which is the only thing an
ADR is for.
-->

## Context

<!--
The forces in play, stated factually and without the conclusion. A reader who
disagrees with the decision should still accept this section as accurate.

Include whatever is load-bearing:
- Volume, latency, and growth numbers. Measurements, not impressions.
- Constraints: cost ceiling, team size and skills, compliance, existing systems.
- What breaks if nothing is decided.

Avoid: restating the decision, advocacy, listing the chosen technology's
features. Those belong below.
-->

## Decision

<!--
What will be done, in the active voice and specific enough to be checkable.

"We will use Polars and DuckDB for all local transformation and analytics, and
introduce PySpark only for workloads meeting the criteria in Revisit When."

Not: "We will consider lightweight engines where appropriate."
-->

## Consequences

<!--
What becomes true as a result. Both directions, honestly.

### Positive
### Negative
### Neutral

The negative section is the one that gives the record credibility. A decision
with no stated downsides has not been examined; it has been advocated for.
-->

## Alternatives Considered

<!--
Each credible alternative, and why it was rejected in terms that would survive
a challenge: a measurement, a constraint, a cost. Not "it felt heavy."

### Alternative: <name>
**Rejected because:** ...

A decision presented without alternatives is indistinguishable from a default.
-->

## Revisit When

<!--
The conditions that would overturn this decision — ideally measurable, so the
answer is observable rather than arguable.

- A working set exceeding N GB, where single-node memory becomes the binding
  constraint.
- A p95 latency above N ms sustained over a week.
- Any requirement for X, which this approach cannot satisfy.

This section is mandatory. A decision record without a reversal condition is an
opinion. Naming the thresholds in advance makes the decision falsifiable and
lets a future reader judge whether it still holds without reconstructing the
original reasoning.
-->

## References

<!--
Benchmarks, documentation, prior art, related ADRs. Link to a commit or a
version rather than to a moving target where possible.
-->
