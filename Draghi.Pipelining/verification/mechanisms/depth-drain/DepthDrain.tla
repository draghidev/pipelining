--------------------------- MODULE DepthDrain ---------------------------
(*
Split-counter depth + drain-waiter protocol for Pipeline.DepthState v2.

The packed-word DepthState made the armer's depth-recheck and the DrainBit set
atomic in a single CAS. The split design separates:
  enqueuedCount   - producer-owned monotonic counter, plain increment + release store
          (single producer per the SPSC source contract)
  completedCount  - completer-side monotonic counter, Interlocked.Increment
          (executor inline completions and advancer drains can overlap)
  tcs   - the drain TCS slot; non-null doubles as the arm signal

Zero-crossing protocol under test (the TCS slot doubles as the arm signal -
the publish CAS is itself the full-fence RMW, so no separate armed word):
  Completer: bump completedCount (full-fence RMW) -> read enqueuedCount, compute depth against ITS
             bump result -> the ZERO VERDICT. The fire (OnDepthReachedZero) is
             DEFERRED by the drain paths (signal-after-advancer-release), so
             the verdict can be arbitrarily stale by fire time. The fire reads
             the tcs slot, Exchange-takes, REVALIDATES depth (the fix), and
             fires only against a still-zero depth; a stale verdict hands the
             tcs back and re-checks (the put-back is itself an arm).
  Armer:     short-circuit if depth 0; else publish tcs (full-fence CAS)
             -> RE-CHECK depth -> if zero, Exchange-take + self-fire (its zero
             is at-or-after its own arm: always legitimate); else await.

Memory-model fidelity: this spec models sequential consistency, which is
faithful here because BOTH critical writes are Interlocked RMWs (full fences):
the completer's tcs-slot load cannot hoist above its completedCount bump, and the armer's
recheck loads cannot hoist above its publish CAS. The remaining edges
(producer's release store on enqueuedCount, completers' acquire loads) only need
monotonic counter visibility, never store->load ordering. See the
volatile-semantics notes: an RMW executes with SC semantics for the purposes
of this Dekker pair.

(The drain-wakeup TCS that DrainOnCompletionAsync arms is a DIFFERENT park
point - a pure StoreLoad fence-pairing handshake - verified in WakeHandshake.tla
(DrainWakeup config). This module is the depth zero-crossing protocol, which has
load-bearing structure BEYOND the fence (the revalidate / zero verdict), so it
does not reduce to a WakeHandshake config.)

Completer equality uses its OWN bump result (completerObservedCount), mirroring
`var completedCount = Interlocked.Increment(ref _completed); depth = enqueuedCount - completedCount`:
when two completers race the final completion, exactly the last bumper can
observe enqueuedCount = completerObservedCount (completedCount <= enqueuedCount invariant), so the verdict cannot
double-trigger and cannot be satisfied by both.

The deferred-fire split (audit C4): the original spec fused the zero verdict
and the fire into one action whose depth guard was evaluated AT the fire -
fire-instant freshness the code does not have. The code's verdict is
bump-instant and the drain paths defer OnDepthReachedZero past the advancer
release, so a producer and a NEW WaitForEmptyAsync arm can land in the
deferral window: the stale zero then takes the new tcs and wakes a caller
whose entry check saw depth > 0, on evidence predating its own arm.
The canonical protocol revalidates after Exchange-taking the signal and puts it
back when depth is no longer zero.

Semantics note: firing on a momentary zero that occurred DURING the wait
(arm first, zero second, depth rises again before the fire lands) is the
documented WaitForEmptyAsync contract (idle-state convergence, not
exactly-once). The premature-wake property therefore flags only fires whose
zero verdict PRE-dates the arm (sawArm tracks whether the arm was up at
verdict time) AND whose fire lands on a non-zero depth.

Properties:
  TypeOK
  DrainWaitNeverCompletesAboveZero - no completer fire delivers a pre-arm zero onto a
                      non-zero depth
  PublishedDrainWaitEventuallyCompletes - once the tcs is published and all items
                      complete, the tcs eventually fires. THE property the
                      packed word used to give for free
                      after the arm-time recheck.
  DrainWaitPublicationEventuallyReturns - the publisher always returns once everything completes
*)

EXTENDS Naturals, TLC

CONSTANTS MaximumEnqueued, Completers

VARIABLES
  enqueuedCount,        \* producer total
  completedCount,       \* completed total
  tcsState,   \* the tcs slot: "none" | "published" | "held" | "fired"
  completerPc,        \* completer pc: "idle" | "bumped" | "zeroPending" | "took" | "putback"
  completerObservedCount,     \* completer's own bump result
  sawArm,     \* was the arm up when this completer's zero verdict was read?
  armerPc,        \* armer pc: "idle" | "decided" | "published" | "waiting" | "done"
  prematureFire

vars == <<enqueuedCount, completedCount, tcsState, completerPc, completerObservedCount, sawArm, armerPc, prematureFire>>

TypeOK ==
  /\ enqueuedCount \in 0..MaximumEnqueued
  /\ completedCount \in 0..MaximumEnqueued
  /\ completedCount <= enqueuedCount
  /\ tcsState \in {"none", "published", "held", "fired"}
  /\ completerPc \in [Completers -> {"idle", "bumped", "zeroPending", "took", "putback"}]
  /\ completerObservedCount \in [Completers -> 0..MaximumEnqueued]
  /\ sawArm \in [Completers -> BOOLEAN]
  /\ armerPc \in {"idle", "decided", "published", "waiting", "done"}
  /\ prematureFire \in BOOLEAN

Init ==
  /\ enqueuedCount = 0
  /\ completedCount = 0
  /\ tcsState = "none"
  /\ completerPc = [c \in Completers |-> "idle"]
  /\ completerObservedCount = [c \in Completers |-> 0]
  /\ sawArm = [c \in Completers |-> FALSE]
  /\ armerPc = "idle"
  /\ prematureFire = FALSE

-----------------------------------------------------------------------------
\* Producer: admits an item. Monotonic; release store modeled as atomic.

Enqueue ==
  /\ enqueuedCount < MaximumEnqueued
  /\ enqueuedCount' = enqueuedCount + 1
  /\ UNCHANGED <<completedCount, tcsState, completerPc, completerObservedCount, sawArm, armerPc, prematureFire>>

-----------------------------------------------------------------------------
\* Completers. Bump is the Interlocked.Increment; the verdict reads enqueuedCount fresh
\* one instruction later and uses the completer's own bump result; the FIRE is
\* deferred (drain paths signal after the advancer release) and reads only the
\* slot - the verdict's freshness is whatever it was at the verdict.

CompleteBump(c) ==
  /\ completerPc[c] = "idle"
  /\ completedCount < enqueuedCount                       \* an admitted, uncompleted item exists
  /\ completedCount' = completedCount + 1
  /\ completerObservedCount' = [completerObservedCount EXCEPT ![c] = completedCount + 1]
  /\ completerPc' = [completerPc EXCEPT ![c] = "bumped"]
  /\ UNCHANGED <<enqueuedCount, tcsState, sawArm, armerPc, prematureFire>>

\* The zero verdict (`return enqueuedCount - completedCount` hitting 0): records whether the arm
\* was already up - a zero observed with the arm up is the documented
\* momentary-zero case even if depth rises again before the deferred fire.
CompleteVerdictZero(c) ==
  /\ completerPc[c] = "bumped"
  /\ enqueuedCount = completerObservedCount[c]
  /\ sawArm' = [sawArm EXCEPT ![c] = tcsState \in {"published", "held"}]
  /\ completerPc' = [completerPc EXCEPT ![c] = "zeroPending"]
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerObservedCount, armerPc, prematureFire>>

CompleteVerdictNonzero(c) ==
  /\ completerPc[c] = "bumped"
  /\ enqueuedCount # completerObservedCount[c]
  /\ completerPc' = [completerPc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerObservedCount, sawArm, armerPc, prematureFire>>

\* OnDepthReachedZero's pre-check: slot empty (or already taken/fired) - the
\* obligation dies; the armer's own post-publish re-check covers a late arm.
SignalCheckForWaiter(c) ==
  /\ completerPc[c] = "zeroPending"
  /\ tcsState # "published"
  /\ completerPc' = [completerPc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerObservedCount, sawArm, armerPc, prematureFire>>

\* Exchange-take: the slot is HELD by this completer (not yet fired).
SignalClaimWaiter(c) ==
  /\ completerPc[c] = "zeroPending"
  /\ tcsState = "published"
  /\ tcsState' = "held"
  /\ completerPc' = [completerPc EXCEPT ![c] = "took"]
  /\ UNCHANGED <<enqueuedCount, completedCount, completerObservedCount, sawArm, armerPc, prematureFire>>

\* Fire only after revalidating that depth is still zero.
SignalRevalidateZeroDepth(c) ==
  /\ completerPc[c] = "took"
  /\ completedCount = enqueuedCount
  /\ tcsState' = "fired"
  /\ completerPc' = [completerPc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enqueuedCount, completedCount, completerObservedCount, sawArm, armerPc, prematureFire>>

\* Stale verdict against a live arm: hand the tcs back (the CAS-back; in code
\* it can only fail against a cancelled-and-re-armed caller, where dropping
\* the held tcs is correct - not modeled, no cancellation here).
SignalRestoreWaiterAtNonzeroDepth(c) ==
  /\ completerPc[c] = "took"
  /\ completedCount # enqueuedCount
  /\ tcsState' = "published"
  /\ completerPc' = [completerPc EXCEPT ![c] = "putback"]
  /\ UNCHANGED <<enqueuedCount, completedCount, completerObservedCount, sawArm, armerPc, prematureFire>>

\* The put-back is itself an arm: a completer that hit zero while the slot was
\* held read it as unavailable and skipped, so re-check depth after the
\* put-back and loop back to a re-take if zero materialized (the same
\* publish-then-recheck discipline as GetIdleTask's arm).
RestoredWaiterRecheckZeroDepth(c) ==
  /\ completerPc[c] = "putback"
  /\ completedCount = enqueuedCount
  /\ tcsState = "published"
  /\ tcsState' = "fired"
  /\ completerPc' = [completerPc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enqueuedCount, completedCount, completerObservedCount, sawArm, armerPc, prematureFire>>

RestoredWaiterAwaitNextZero(c) ==
  /\ completerPc[c] = "putback"
  /\ (completedCount # enqueuedCount) \/ (tcsState # "published")
  /\ completerPc' = [completerPc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerObservedCount, sawArm, armerPc, prematureFire>>

-----------------------------------------------------------------------------
\* Armer (WaitForEmptyAsync, single-caller contract): publish (the CAS is the
\* full-fence arm) -> recheck.

\* Entry depth check. A SEPARATE step from the publish, mirroring the code: the
\* Depth load sequence and the tcs CAS are distinct instructions, and the lost
\* wake lives precisely in that gap (depth read >0, everything completes, then
\* the publish lands with nobody left to observe it).
ArmShortCircuit ==
  /\ armerPc = "idle"
  /\ completedCount = enqueuedCount
  /\ armerPc' = "done"
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerPc, completerObservedCount, sawArm, prematureFire>>

ArmDecide ==
  /\ armerPc = "idle"
  /\ completedCount # enqueuedCount
  /\ armerPc' = "decided"
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerPc, completerObservedCount, sawArm, prematureFire>>

\* The CAS publish: unconditional once decided (the code does not re-read depth
\* between the entry check and the CAS).
ArmPublish ==
  /\ armerPc = "decided"
  /\ tcsState' = "published"
  /\ armerPc' = "published"
  /\ UNCHANGED <<enqueuedCount, completedCount, completerPc, completerObservedCount, sawArm, prematureFire>>

\* Recheck after the full-fenced publish. Zero with the slot still published:
\* Exchange-take + self-fire (always legitimate: this zero is at-or-after the
\* arm by construction). A completer holding the slot resolves it instead.
ArmRecheckZero ==
  /\ armerPc = "published"
  /\ completedCount = enqueuedCount
  /\ tcsState = "published"
  /\ tcsState' = "fired"
  /\ armerPc' = "done"
  /\ UNCHANGED <<enqueuedCount, completedCount, completerPc, completerObservedCount, sawArm, prematureFire>>

\* Depth still positive, or the slot is in a completer's hands (held/fired:
\* the Exchange in SignalDrainWaiter returned null and the armer awaits the
\* task) - go await. Without the fix, the armer waits unconditionally.
ArmRecheckWait ==
  /\ armerPc = "published"
  /\ (completedCount # enqueuedCount) \/ (tcsState # "published")
  /\ armerPc' = "waiting"
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerPc, completerObservedCount, sawArm, prematureFire>>

\* The awaited tcs fired: WaitForEmptyAsync returns.
ArmObserveFired ==
  /\ armerPc = "waiting"
  /\ tcsState = "fired"
  /\ armerPc' = "done"
  /\ UNCHANGED <<enqueuedCount, completedCount, tcsState, completerPc, completerObservedCount, sawArm, prematureFire>>

-----------------------------------------------------------------------------

Next ==
  \/ Enqueue
  \/ \E c \in Completers : CompleteBump(c) \/ CompleteVerdictZero(c)
       \/ CompleteVerdictNonzero(c) \/ SignalCheckForWaiter(c) \/ SignalClaimWaiter(c)
       \/ SignalRevalidateZeroDepth(c) \/ SignalRestoreWaiterAtNonzeroDepth(c)
       \/ RestoredWaiterRecheckZeroDepth(c) \/ RestoredWaiterAwaitNextZero(c)
  \/ ArmShortCircuit \/ ArmDecide \/ ArmPublish
  \/ ArmRecheckZero \/ ArmRecheckWait \/ ArmObserveFired

\* Fairness: completions and the armer's own steps eventually happen; the
\* producer may stop at any point (no fairness on Enqueue).
Fairness ==
  /\ \A c \in Completers :
       /\ WF_vars(CompleteBump(c))
       /\ WF_vars(CompleteVerdictZero(c))
       /\ WF_vars(CompleteVerdictNonzero(c))
       /\ WF_vars(SignalCheckForWaiter(c))
       /\ WF_vars(SignalClaimWaiter(c))
       /\ WF_vars(SignalRevalidateZeroDepth(c))
       /\ WF_vars(SignalRestoreWaiterAtNonzeroDepth(c))
       /\ WF_vars(RestoredWaiterRecheckZeroDepth(c))
       /\ WF_vars(RestoredWaiterAwaitNextZero(c))
  /\ WF_vars(ArmShortCircuit)
  /\ WF_vars(ArmDecide)
  /\ WF_vars(ArmPublish)
  /\ WF_vars(ArmRecheckZero)
  /\ WF_vars(ArmRecheckWait)
  /\ WF_vars(ArmObserveFired)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* No completer fire delivers a PRE-ARM zero onto a non-zero depth: the caller
\* would wake on evidence predating its own arm (its entry check saw > 0).
DrainWaitNeverCompletesAboveZero == ~prematureFire

\* The lost-wake shape: everything done, waiter parked forever. TLC finds this
\* as a liveness violation of PublishedDrainWaitEventuallyCompletes if the protocol has the hole.
PublishedDrainWaitEventuallyCompletes == (tcsState \in {"published", "held"}) ~> (tcsState = "fired")

\* The armer always returns once everything completes.
DrainWaitPublicationEventuallyReturns ==
  (armerPc \in {"decided", "published", "waiting"}) ~> (armerPc = "done")

=============================================================================
