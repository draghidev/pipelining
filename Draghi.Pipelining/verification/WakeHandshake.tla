--------------------------- MODULE WakeHandshake ---------------------------
(*
The StoreLoad / Dekker fence-pairing lost-wake, as ONE parameterized model. Two
threads each STORE their own flag then LOAD the other's; if a STORE->LOAD edge is
not full-fenced it can reorder, the LOAD reads stale, and both sides can miss each
other - the parking side then waits forever. The law:

    NoLostWake holds  iff  EVERY StoreLoad edge is fenced  (FenceA /\ FenceB)

A single unfenced (or release-only) edge on EITHER side makes the both-miss
interleaving reachable. This is not "share by pattern" - each feature that uses
this handshake is its OWN CONFIG (a point in the <FenceA, FenceB> grid), checked
here, because the features sit at DIFFERENT points in that grid and one feature's
run does not execute another's failing config.

Threads (feature-agnostic; A = the side that PARKS, B = the side that SIGNALS):

  A: STORE flagA := set        (publish my arm / presence)
     LOAD  flagB               (did the other side make progress?)
     -> observed B  => resolve (no wait);  ~observed B => PARK
  B: STORE flagB := set        (publish my progress)
     LOAD  flagA               (did A arm?)
     -> observed A => SIGNAL (wakes a parked A);  ~observed A => SKIP

Lost wake = A parked AND B skipped (both LOADs stale) => A parked forever.

------------------------------------------------------------------------------
The two pipeline features that ARE this handshake (configs below):

  D5  Drain-wakeup TCS (DrainOnCompletionAsync, Pipeline.cs ~518-538).
      A = the waiter: STORE _drainWakeupTcs := armed; LOAD IsHeld/Count.
      B = the callback/release: STORE (release latch / decrement) := done; LOAD tcs.
      The B-side store is ALREADY an Interlocked RMW (latch release / decrement),
      so FenceB is pinned TRUE; only the waiter's arm is in question. Shipped fix =
      Interlocked.Exchange arm = FenceA TRUE. Bug = Volatile.Write arm = FenceA FALSE.
      Configs: WakeHandshake.cfg (TRUE,TRUE -> HOLDS),
               WakeHandshake_DrainWakeupWitness.cfg (FALSE,TRUE -> VIOLATES).

  #5  Shutdown caller<->executor handoff (Pipeline._shutdownState /
      _shutdownCallerDone; PipelineShutdown.tla GateSpec header documents it).
      A = the executor: STORE its wait-TCS := published; LOAD _shutdownState.
      B = the caller:   STORE _shutdownState := Done;     LOAD the executor's TCS.
      BOTH were Volatile.Write in the bug (the standalone HangRepro, iter 51 on
      Apple Silicon), so FenceA = FenceB = the toggle. Fix = both Interlocked.Exchange.
      Configs: WakeHandshake.cfg (TRUE,TRUE -> HOLDS, shared with D5's fix),
               WakeHandshake_GateHandoffWitness.cfg (FALSE,FALSE -> VIOLATES),
               WakeHandshake_PartialFenceWitness.cfg (TRUE,FALSE -> VIOLATES; pins
               that ONE fence is insufficient - why the fix had to be both Exchanges).

Modeling: per-side LOAD snapshot, stale under ~Fence (worst-case: the LOAD reordered
ahead of its own STORE reads the pre-store view of the other flag). Over-approximating
the staleness is conservative for proving NoLostWake and still exhibits the real
witness. Same snapshot technique as the original DrainWakeup module (now subsumed here).

Property:
  NoLostWake - the parking side (A) eventually completes (<>(apc = "done")).
               HOLDS iff FenceA /\ FenceB.
*)

EXTENDS Naturals, TLC

CONSTANTS
  FenceA,   \* TRUE: A's STORE->LOAD is full-fenced (Interlocked.Exchange). FALSE: release-only.
  FenceB    \* TRUE: B's STORE->LOAD is full-fenced. FALSE: release-only.

VARIABLES
  flagA,    \* A's store (its arm/presence); B reads it
  flagB,    \* B's store (its progress); A reads it
  apc,      \* A pc: "idle" | "stored" | "parked" | "done"
  bpc,      \* B pc: "idle" | "stored" | "done"
  aSaw,     \* the value A's LOAD of flagB observed
  bSaw,     \* the value B's LOAD of flagA observed
  signaled  \* B signalled (wakes a parked A)

vars == << flagA, flagB, apc, bpc, aSaw, bSaw, signaled >>

TypeOK ==
  /\ flagA \in BOOLEAN
  /\ flagB \in BOOLEAN
  /\ apc \in {"idle", "stored", "parked", "done"}
  /\ bpc \in {"idle", "stored", "done"}
  /\ aSaw \in BOOLEAN
  /\ bSaw \in BOOLEAN
  /\ signaled \in BOOLEAN

Init ==
  /\ flagA = FALSE
  /\ flagB = FALSE
  /\ apc = "idle"
  /\ bpc = "idle"
  /\ aSaw = FALSE
  /\ bSaw = FALSE
  /\ signaled = FALSE

-----------------------------------------------------------------------------
\* Thread A (the parking side): STORE flagA, snapshot the LOAD of flagB. Under
\* FenceA the snapshot is the current flagB; under ~FenceA the LOAD can reorder
\* ahead of the STORE and read the pre-store view (FALSE) - it misses B.
AStore ==
  /\ apc = "idle"
  /\ flagA' = TRUE
  /\ aSaw' = (IF FenceA THEN flagB ELSE FALSE)
  /\ apc' = "stored"
  /\ UNCHANGED << flagB, bpc, bSaw, signaled >>

\* Observed B (flagB) -> resolve, no wait.
AResolve ==
  /\ apc = "stored"
  /\ aSaw
  /\ apc' = "done"
  /\ UNCHANGED << flagA, flagB, bpc, aSaw, bSaw, signaled >>

\* Did not observe B -> park, awaiting B's signal.
APark ==
  /\ apc = "stored"
  /\ ~aSaw
  /\ apc' = "parked"
  /\ UNCHANGED << flagA, flagB, bpc, aSaw, bSaw, signaled >>

\* B's signal woke A.
AWoken ==
  /\ apc = "parked"
  /\ signaled
  /\ apc' = "done"
  /\ UNCHANGED << flagA, flagB, bpc, aSaw, bSaw, signaled >>

-----------------------------------------------------------------------------
\* Thread B (the signalling side): STORE flagB, snapshot the LOAD of flagA.
BStore ==
  /\ bpc = "idle"
  /\ flagB' = TRUE
  /\ bSaw' = (IF FenceB THEN flagA ELSE FALSE)
  /\ bpc' = "stored"
  /\ UNCHANGED << flagA, apc, aSaw, signaled >>

\* Observed A (flagA) -> signal (wakes a parked A; harmless if A resolved).
BSignal ==
  /\ bpc = "stored"
  /\ bSaw
  /\ signaled' = TRUE
  /\ bpc' = "done"
  /\ UNCHANGED << flagA, flagB, apc, aSaw, bSaw >>

\* Did not observe A -> skip. If A also parked, the wake is lost.
BSkip ==
  /\ bpc = "stored"
  /\ ~bSaw
  /\ bpc' = "done"
  /\ UNCHANGED << flagA, flagB, apc, aSaw, bSaw, signaled >>

-----------------------------------------------------------------------------

Next ==
  \/ AStore \/ AResolve \/ APark \/ AWoken
  \/ BStore \/ BSignal \/ BSkip

Fairness ==
  /\ WF_vars(AStore)
  /\ WF_vars(AResolve)
  /\ WF_vars(APark)
  /\ WF_vars(AWoken)
  /\ WF_vars(BStore)
  /\ WF_vars(BSignal)
  /\ WF_vars(BSkip)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* The parking side always completes. HOLDS iff FenceA /\ FenceB; any missing
\* fence makes the both-miss interleaving reachable and A parks forever.
NoLostWake == <>(apc = "done")

=============================================================================
