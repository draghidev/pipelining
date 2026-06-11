--------------------------- MODULE DepthDrain ---------------------------
(*
Split-counter depth + drain-waiter protocol for Pipeline.DepthState v2.

The packed-word DepthState made the armer's depth-recheck and the DrainBit set
atomic in a single CAS. The split design separates:
  enq   - producer-owned monotonic counter, plain increment + release store
          (single producer per the SPSC source contract)
  comp  - completer-side monotonic counter, Interlocked.Increment
          (executor inline completions and advancer drains can overlap)
  tcs   - the drain TCS slot; non-null doubles as the arm signal

Zero-crossing protocol under test (the TCS slot doubles as the arm signal -
the publish CAS is itself the full-fence RMW, so no separate armed word):
  Completer: bump comp (full-fence RMW) -> read enq, compute depth against ITS
             bump result -> if zero, read tcs slot -> if published,
             Exchange-take + fire.
  Armer:     short-circuit if depth 0; else publish tcs (full-fence CAS)
             -> RE-CHECK depth -> if zero, Exchange-take + self-fire (idempotent
             with a racing completer via the take); else await.

Memory-model fidelity: this spec models sequential consistency, which is
faithful here because BOTH critical writes are Interlocked RMWs (full fences):
the completer's tcs-slot load cannot hoist above its comp bump, and the armer's
recheck loads cannot hoist above its publish CAS. The remaining edges
(producer's release store on enq, completers' acquire loads) only need
monotonic counter visibility, never store->load ordering. See the
volatile-semantics notes: an RMW executes with SC semantics for the purposes
of this Dekker pair.

Completer equality uses its OWN bump result (myComp), mirroring
`var comp = Interlocked.Increment(ref _completed); depth = enq - comp`:
when two completers race the final completion, exactly the last bumper can
observe enq = myComp (comp <= enq invariant), so the fire guard cannot
double-trigger and cannot be satisfied by both.

Properties:
  TypeOK            - bounds
  FireImpliesIdle   - any fire transition observed depth 0 at its read
                      (encoded in the action guards; the invariant re-checks
                      the post-state consequence tcs fired => comp = enq once
                      the producer is done)
  NoLostWake        - liveness: once the tcs is published and all items
                      complete, the tcs eventually fires. THE property the
                      packed word used to give for free.
*)

EXTENDS Naturals, TLC

\* RecheckFix: TRUE models the real protocol (armer rechecks depth after the
\* full-fenced arm). FALSE is the witness toggle: arm-then-wait with no recheck,
\* which must lose the wake when a completer's armed-read raced ahead of the arm
\* (TLC: NoLostWake fails). Keeps the spec honest.
CONSTANTS MaxEnq, Completers, RecheckFix

VARIABLES
  enq,        \* producer total
  comp,       \* completed total
  tcsState,   \* the tcs slot, doubling as the arm: "none" | "published" | "fired"
  cpc,        \* completer pc: "idle" | "bumped"
  myComp,     \* completer's own bump result
  apc         \* armer pc: "idle" | "published" | "waiting" | "done"

vars == <<enq, comp, tcsState, cpc, myComp, apc>>

TypeOK ==
  /\ enq \in 0..MaxEnq
  /\ comp \in 0..MaxEnq
  /\ comp <= enq
  /\ tcsState \in {"none", "published", "fired"}
  /\ cpc \in [Completers -> {"idle", "bumped"}]
  /\ myComp \in [Completers -> 0..MaxEnq]
  /\ apc \in {"idle", "decided", "published", "waiting", "done"}

Init ==
  /\ enq = 0
  /\ comp = 0
  /\ tcsState = "none"
  /\ cpc = [c \in Completers |-> "idle"]
  /\ myComp = [c \in Completers |-> 0]
  /\ apc = "idle"

-----------------------------------------------------------------------------
\* Producer: admits an item. Monotonic; release store modeled as atomic.

Enqueue ==
  /\ enq < MaxEnq
  /\ enq' = enq + 1
  /\ UNCHANGED <<comp, tcsState, cpc, myComp, apc>>

-----------------------------------------------------------------------------
\* Completers. Bump is the Interlocked.Increment; the subsequent check reads
\* enq fresh and uses the completer's own bump result for the zero test.

CompleteBump(c) ==
  /\ cpc[c] = "idle"
  /\ comp < enq                       \* an admitted, uncompleted item exists
  /\ comp' = comp + 1
  /\ myComp' = [myComp EXCEPT ![c] = comp + 1]
  /\ cpc' = [cpc EXCEPT ![c] = "bumped"]
  /\ UNCHANGED <<enq, tcsState, apc>>

\* Zero observed at the fresh enq read AND a published tcs: Exchange-take + fire.
CompleteFire(c) ==
  /\ cpc[c] = "bumped"
  /\ enq = myComp[c]                  \* fresh read: depth 0 at this instant
  /\ tcsState = "published"           \* slot non-null; the Exchange-take wins
  /\ tcsState' = "fired"
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, myComp, apc>>

\* Not zero at the read, or no published tcs (including: another taker won).
CompleteSkip(c) ==
  /\ cpc[c] = "bumped"
  /\ (enq # myComp[c]) \/ (tcsState # "published")
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, tcsState, myComp, apc>>

-----------------------------------------------------------------------------
\* Armer (WaitForEmptyAsync, single-caller contract): publish (the CAS is the
\* full-fence arm) -> recheck.

\* Entry depth check. A SEPARATE step from the publish, mirroring the code: the
\* Depth load sequence and the tcs CAS are distinct instructions, and the lost
\* wake lives precisely in that gap (depth read >0, everything completes, then
\* the publish lands with nobody left to observe it).
ArmShortCircuit ==
  /\ apc = "idle"
  /\ comp = enq
  /\ apc' = "done"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp>>

ArmDecide ==
  /\ apc = "idle"
  /\ comp # enq
  /\ apc' = "decided"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp>>

\* The CAS publish: unconditional once decided (the code does not re-read depth
\* between the entry check and the CAS).
ArmPublish ==
  /\ apc = "decided"
  /\ tcsState' = "published"
  /\ apc' = "published"
  /\ UNCHANGED <<enq, comp, cpc, myComp>>

\* Recheck after the full-fenced publish. Zero: Exchange-take + self-fire (a
\* racing completer may have read the slot as null before our publish became
\* visible; if it instead won the take, ours is a no-op on "fired").
ArmRecheckZero ==
  /\ RecheckFix
  /\ apc = "published"
  /\ comp = enq
  /\ tcsState' = "fired"
  /\ apc' = "done"
  /\ UNCHANGED <<enq, comp, cpc, myComp>>

\* Depth still positive: go await the tcs. Without the fix, the armer waits
\* unconditionally (the naive publish-then-wait protocol the witness disproves).
ArmRecheckWait ==
  /\ apc = "published"
  /\ (comp # enq) \/ ~RecheckFix
  /\ apc' = "waiting"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp>>

\* The awaited tcs fired: WaitForEmptyAsync returns.
ArmObserveFired ==
  /\ apc = "waiting"
  /\ tcsState = "fired"
  /\ apc' = "done"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp>>

-----------------------------------------------------------------------------

Next ==
  \/ Enqueue
  \/ \E c \in Completers : CompleteBump(c) \/ CompleteFire(c) \/ CompleteSkip(c)
  \/ ArmShortCircuit \/ ArmDecide \/ ArmPublish
  \/ ArmRecheckZero \/ ArmRecheckWait \/ ArmObserveFired

\* Fairness: completions and the armer's own steps eventually happen; the
\* producer may stop at any point (no fairness on Enqueue).
Fairness ==
  /\ \A c \in Completers :
       /\ WF_vars(CompleteBump(c))
       /\ WF_vars(CompleteFire(c))
       /\ WF_vars(CompleteSkip(c))
  /\ WF_vars(ArmShortCircuit)
  /\ WF_vars(ArmDecide)
  /\ WF_vars(ArmPublish)
  /\ WF_vars(ArmRecheckZero)
  /\ WF_vars(ArmRecheckWait)
  /\ WF_vars(ArmObserveFired)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
\* Safety: a fired tcs with the producer done and items outstanding would be a
\* premature wake. Fire guards require an instant of depth 0, so once fired,
\* there EXISTED such an instant; with monotonic counters that means every
\* item admitted before the fire completed. A later enqueue may raise depth
\* again - that is the documented WaitForEmptyAsync semantics (momentary
\* quiescence), so the invariant is on the fire instant, enforced by guards.

\* The lost-wake shape: everything done, waiter parked forever. TLC finds this
\* as a liveness violation of NoLostWake if the protocol has the hole.
NoLostWake == (tcsState = "published") ~> (tcsState = "fired")

\* The armer always returns once everything completes.
ArmerTerminates == (apc \in {"decided", "published", "waiting"}) ~> (apc = "done")

=============================================================================
