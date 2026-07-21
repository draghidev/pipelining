# Draghi pipeline verification

This directory contains the maintained TLA+ models for Draghi's pipeline and its planned component boundaries.

Each runnable model has one canonical, code-faithful configuration beside its `.tla` file. Contract and library modules are composed by runnable models and need no configuration. Diagnostic configurations are grouped by purpose:

- `probes`: reachability, non-vacuity, or scope questions; their declared expectations may pass or fail.

The path supplies configuration identity and category; a config paired with its module is `shipped`. Each config declares only `EXPECT`, which cannot be inferred for probes. `verify run` succeeds only when TLC matches that expectation. [models.tsv](models.tsv) registers model ownership and impact tags; `verify` discovers configurations beneath those registered roots.

## Directory map

| Area | Purpose |
|---|---|
| `pipeline/` | End-to-end composition of source delivery, storage, item lifetime, activation, and recovery. |
| `components/` | The planned `PipelineSource`, `InFlightStore`, `ItemTenure`, and `ActivationGate` split, plus the independent two-task execution contract. |
| `mechanisms/` | Focused proofs for depth draining, queue draining, store escalation, and symmetric wake handshakes. |
| `lib/` | Shared TLA+ helpers; currently the weak-memory write/propagation operators. |
| `docs/` | Stable architecture, fidelity boundaries, and proof obligations. |

## Commands

```text
./verify check
./verify list
./verify list --kind probes
./verify list --impact item-tenure
./verify run pipeline/Pipeline.cfg
```

`verify check` is intentionally cheap: it validates registered module/file names, configuration categories and expectations, one shipped config per runnable model, and complete config registration. `verify run` accepts the displayed repository-relative config path, uses `~/tla2tools.jar`, and runs with `-workers auto`. Set `TLA2TOOLS_JAR` or `TLC_WORKERS` to override those defaults.

## Vocabulary

- **In-flight item**: committed pipeline work not yet retired.
- **Item tenure**: the lifetime during which completion, callback delivery, claiming, and retirement may legally touch one item.
- **Activation turn**: the unique right to invoke the policy for an item.
- **Delivery arm**: the item-keyed indication that a completion callback has been registered but has not completed its delivery attempt.
- **Advance license**: the acquire-or-deposit ownership used to serialize retirement passes.
- **Empty-edge handoff**: the two-sided resolution between a dispatcher publishing an activation handoff and a retirement pass observing the transition to zero resident items.
- **Progress obligation**: work that must eventually be retried or delivered through callback registration, a pending wake, or redrive.

See [architecture.md](docs/architecture.md) for component custody and [fidelity-boundaries.md](docs/fidelity-boundaries.md) for what each model does and does not establish.
