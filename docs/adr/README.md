# Architecture Decision Records

An ADR captures a decision that was **expensive to make and would be expensive
to reverse**, along with the context that made it correct at the time.

Code shows what was built. An ADR shows that the alternatives were understood,
the tradeoff was deliberate, and the conditions for revisiting it were stated
in advance. When someone asks "why is it like this?" eighteen months later, the
answer should be a document rather than an archaeology exercise through
`git log`.

## Index

| ADR | Decision | Status | Date |
|-----|----------|--------|------|
| [0001](0001-why-a-financial-market-platform.md) | Why a financial market platform? | Accepted | 2026-10-26 |
| [0002](0002-polars-duckdb-for-local-development.md) | Why Polars and DuckDB for local development? | Accepted | 2026-10-26 |
| [0003](0003-why-yahoo-finance-for-initial-ingestion.md) | Why Yahoo Finance's chart endpoint for initial ingestion? | Accepted | 2026-10-27 |

## What warrants an ADR

Write one when a decision:

- is **hard to reverse** — a storage format, a table layout, an execution engine,
  a public interface;
- **constrains future work** — a boundary that other components must respect;
- has **credible alternatives** that a competent engineer would have chosen;
- was **contentious or surprising**, including decisions to *not* adopt
  something widely expected (this repository's deferral of Spark is the
  canonical example).

Do **not** write one for a decision that is cheap to reverse. An ADR for a log
format or a variable naming scheme dilutes the set, and a diluted set stops
being read.

## Conventions

Enforced by `tests/architecture/test_repository_conventions.py`, so a
non-conforming ADR fails CI rather than being caught in review:

1. Filename is `NNNN-kebab-case-title.md`, zero-padded to four digits.
2. The first line is `# ADR-NNNN: Title`, with a number matching the filename.
3. Numbers are **contiguous**. A gap means a record was deleted instead of
   superseded, which discards the reasoning the record existed to preserve.
4. Required sections: `## Status`, `## Context`, `## Decision`,
   `## Consequences`, `## Alternatives Considered`, `## Revisit When`.
5. The record is linked from the index table above.

### Why `## Alternatives Considered` is mandatory

A decision presented without alternatives is indistinguishable from a default.
The alternatives are the evidence that a choice was made. Each one should state
why it was rejected in terms that would survive a challenge — a measurement, a
constraint, a cost — not "it felt heavier."

### Why `## Revisit When` is mandatory

**A decision record without a reversal condition is an opinion.** Every
decision here is correct relative to a context — a data volume, a team size, a
cost ceiling — and contexts change. Stating in advance what evidence would
overturn the decision does two things: it makes the decision falsifiable, and it
lets a future reader determine whether it still holds without reconstructing the
original reasoning.

Concretely: ADR-002 chooses Polars and DuckDB over Spark for local development.
That is correct *while* the working set fits on one machine. Rather than leave
that implicit, the ADR names the thresholds that would justify Spark. When one
is crossed, the decision is revisited on evidence rather than on taste.

## Lifecycle

| Status | Meaning |
|---|---|
| `Proposed` | Under discussion. Not yet binding. |
| `Accepted` | In force. The codebase should reflect it. |
| `Deprecated` | No longer applies, with no direct replacement. |
| `Superseded by ADR-NNNN` | Replaced. The original stays for its reasoning. |
| `Rejected` | Considered and declined. Kept so the question is not reopened blindly. |

**Accepted ADRs are not edited to reflect new decisions.** They are superseded.
Editing destroys the record of what was believed and why, which is the only
thing an ADR is for. Fixing a typo is fine; changing a conclusion is not.

## Writing a new one

```bash
cp docs/adr/template.md docs/adr/0003-your-decision.md
```

Then add it to the index table. The architecture tests will tell you what is
missing:

```bash
pytest -m architecture
```

Keep it short. An ADR that nobody finishes reading has failed regardless of how
thorough it is. Two pages of specifics beats ten pages of context.
