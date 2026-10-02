# evals

The golden set (issue #38). A prompt, retrieval or threshold change gets a
number here before it ships: 100 questions over a frozen corpus, scored on
retrieval alone and on the answers the module actually returns.

## Running

```
cargo run -p evals -- --model fake
```

CI runs exactly that on every PR (the `evals` job) and fails when `recall@6`
falls below `evals/baseline.json`. `fake` needs no network.

| Flag | What it does |
| --- | --- |
| `--model fake` | the retrieval-as-answer baseline below |
| `--model anthropic[:MODEL_ID]` | a real model through the harness Anthropic adapter; reads `ANTHROPIC_API_KEY`, and `ANTHROPIC_MODEL` when no id is given |
| `--fixtures DIR` | the fixture root (default `evals/fixtures`) |
| `--out FILE` | also write the report there (stdout is the report, progress is stderr) |
| `--write-baseline` | overwrite `baseline.json` with this run's `recall@6` |

A retrieval change that legitimately moves `recall@6` re-baselines with
`--write-baseline` in the same PR. Real-model runs are committed by hand
under `evals/results/<date>-<model>.json` — a convention, not a path the
runner writes.

## Fixtures

`corpus/*.md` are frozen, verbatim MIT docs (provenance in
`fixtures/SOURCES.md`). `questions.jsonl` is 100 questions: 70 `answerable`,
20 `unanswerable`, 10 `needs_person`. An answerable question's `gold` is
`{source, phrase}` pairs — a verbatim phrase resolved to chunk ids at run
time, so the labels survive a chunker change; a phrase that resolves to no
chunk fails the run.

## Metrics

**Retrieval**, over the answerable questions' references, from `GET /search`:
a gold reference is a hit at `k` if any chunk carrying its phrase is in the
top `k` — `recall@1/3/6/10`. MRR (reciprocal rank, 0 when never retrieved)
uses the full ranked list (the route's `MAX_LIMIT`, 50), so a reference that
lands beyond 10 contributes its true rank rather than scoring zero.

**Answers.** The tenant threshold is set to `0` for the run, so every
question costs one model call, and the threshold is swept afterwards:
`answered(t)` means the module answered at `0` and confidence ≥ `t`, at the
module's whole-percent grain. The sweep is 0.00–1.00 step 0.05, plus
`DEFAULT_ANSWER_THRESHOLD` (0.60). At each threshold: `answer_rate` (answered,
over all questions); `wrong_answer_rate` (answered, over `unanswerable` ∪
`needs_person`); `citation_precision` and `citation_recall` (over the answered
answerable questions); `handoff_precision` (of the module's handoffs, the
share labelled `needs_person`).

The `fake` model is retrieval-as-answer: it always cites the top-ranked
chunk at confidence 0.9, so its answer numbers measure retrieval, not a model.

## Current numbers

Fixture rev of this PR, retrieval over the 70 answerable questions'
references: recall@1 0.400, @3 0.786, @6 0.914, @10 0.957, MRR 0.601.
`fake` at the default threshold 0.60: answer rate 1.00, wrong-answer rate
1.00, citation precision 0.40, citation recall 0.40.

## Not measured yet (follow-ups to #38)

- A real-model run: the wrong-answer rate of a real model at the default
  0.60 is not known, and the threshold curve that would justify or change
  0.60 is not published.
- The judge golden set: ~30 transcripts labelled `file`/`needs_info`/
  `reject`/`duplicate_of`, via module-escalation.
- A `workflow_dispatch` job that runs a real model and commits the result.
