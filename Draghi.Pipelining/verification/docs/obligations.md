# Progress and safety obligations

The suite is organized around five rules.

1. Every transition that leaves work resident must identify the concrete mechanism that will revisit it: a registered completion callback, a pending advance request, a redrive request, or an already-running retirement pass.
2. Every modeled activation path arbitrates through the same activation-turn identity, including empty-edge publication, dispatcher self-grant, resident activation, and recovery replacement.
3. Generation identity is minted when a deferral becomes observable and is preserved through every later claim or recovery wait.
4. A mechanism that can be consumed without satisfying the obligation cannot be its sole guarantee. A one-shot pending bit is not equivalent to a guaranteed redrive.
5. Completion callback dispatch and item claim must never overlap the tenure reset performed by consumption and retirement.

## Ownership

| Obligation | Primary model | Verification check |
|---|---|---|
| FIFO source delivery and ownership transfer | `PipelineSource` | `SourceDeliverSuccessor` in `ExecutorNext` |
| FIFO storage observation and removal | `InFlightStore`, `QueueDrainObligation` | `ItemsClaimedInFifoOrder`, `ItemsRetireInFifoOrder`, `CompletedPrefixesEventuallyRetire` |
| Delivery callback cannot be torn by claim | `ItemTenure`, `Pipeline` | `CompletionDispatchNeverTorn`, `NoRetirementDuringCompletionDispatch`, `CompletionNeverReadsRetiredTenure` |
| At most one activation turn | `ActivationGate`, `Pipeline` | `ActivationGrantedAtMostOnce`, `ItemActivatedAtMostOnce`, `ActivationTurnNamesLiveTenure` |
| Dispatcher and final retirement pass cannot both miss a deferral | `ActivationGate` | deadlock check over the canonical fenced protocol |
| Recovery cannot overlap unresolved activation or trailing output | `Pipeline`, `ItemTaskGates` | `RecoveryDoesNotOverlapActivation`, `RecoveryWaitsForTrailingTask` |
| Pipeline task gates retirement | `ItemTaskGates` | `RetirementWaitsForPipelineTask` |
| Trailing task gates successor issue | `ItemTaskGates` | `SuccessorWaitsForTrailingTask` |
