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
the completer's tcs-slot load cannot hoist above its comp bump, and the armer's
recheck loads cannot hoist above its publish CAS. The remaining edges
(producer's release store on enq, completers' acquire loads) only need
monotonic counter visibility, never store->load ordering. See the
volatile-semantics notes: an RMW executes with SC semantics for the purposes
of this Dekker pair.

(The drain-wakeup TCS that DrainOnCompletionAsync arms is a DIFFERENT park
point - a pure StoreLoad fence-pairing handshake - verified in WakeHandshake.tla
(DrainWakeup config). This module is the depth zero-crossing protocol, which has
load-bearing structure BEYOND the fence (the revalidate / zero verdict), so it
does not reduce to a WakeHandshake config.)

Completer equality uses its OWN bump result (myComp), mirroring
`var comp = Interlocked.Increment(ref _completed); depth = enq - comp`:
when two completers race the final completion, exactly the last bumper can
observe enq = myComp (comp <= enq invariant), so the verdict cannot
double-trigger and cannot be satisfied by both.

The deferred-fire split (audit C4): the original spec fused the zero verdict
and the fire into one action whose depth guard was evaluated AT the fire -
fire-instant freshness the code does not have. The code's verdict is
bump-instant and the drain paths defer OnDepthReachedZero past the advancer
release, so a producer and a NEW WaitForEmptyAsync arm can land in the
deferral window: the stale zero then takes the new tcs and wakes a caller
whose entry check saw depth > 0, on evidence predating its own arm.
RevalidateFix=TRUE models the fix (Exchange-take -> depth re-check -> fire,
else CAS-back + re-check-after-put-back); FALSE is the blind take-and-fire.

Semantics note: firing on a momentary zero that occurred DURING the wait
(arm first, zero second, depth rises again before the fire lands) is the
documented WaitForEmptyAsync contract (idle-state convergence, not
exactly-once). The premature-wake property therefore flags only fires whose
zero verdict PRE-dates the arm (sawArm tracks whether the arm was up at
verdict time) AND whose fire lands on a non-zero depth.

Properties:
  TypeOK
  NoPrematureWake   - no completer fire delivers a pre-arm zero onto a
                      non-zero depth (fails with RevalidateFix=FALSE)
  NoLostWake        - liveness: once the tcs is published and all items
                      complete, the tcs eventually fires. THE property the
                      packed word used to give for free
                      (fails with RecheckFix=FALSE).
  ArmerTerminates   - the armer always returns once everything completes
*)

EXTENDS Naturals, TLC

\* RecheckFix: TRUE models the real protocol (armer rechecks depth after the
\* full-fenced arm). FALSE is the witness toggle: arm-then-wait with no recheck,
\* which must lose the wake when a completer's armed-read raced ahead of the arm
\* (TLC: NoLostWake fails). RevalidateFix: TRUE models the revalidating fire
\* (the C4 fix); FALSE is the blind deferred fire (NoPrematureWake fails).
CONSTANTS MaxEnq, Completers, RecheckFix, RevalidateFix

VARIABLES
  enq,        \* producer total
  comp,       \* completed total
  tcsState,   \* the tcs slot: "none" | "published" | "held" | "fired"
  cpc,        \* completer pc: "idle" | "bumped" | "zeroPending" | "took" | "putback"
  myComp,     \* completer's own bump result
  sawArm,     \* was the arm up when this completer's zero verdict was read?
  apc,        \* armer pc: "idle" | "decided" | "published" | "waiting" | "done"
  prematureFire

vars == <<enq, comp, tcsState, cpc, myComp, sawArm, apc, prematureFire>>

TypeOK ==
  /\ enq \in 0..MaxEnq
  /\ comp \in 0..MaxEnq
  /\ comp <= enq
  /\ tcsState \in {"none", "published", "held", "fired"}
  /\ cpc \in [Completers -> {"idle", "bumped", "zeroPending", "took", "putback"}]
  /\ myComp \in [Completers -> 0..MaxEnq]
  /\ sawArm \in [Completers -> BOOLEAN]
  /\ apc \in {"idle", "decided", "published", "waiting", "done"}
  /\ prematureFire \in BOOLEAN

Init ==
  /\ enq = 0
  /\ comp = 0
  /\ tcsState = "none"
  /\ cpc = [c \in Completers |-> "idle"]
  /\ myComp = [c \in Completers |-> 0]
  /\ sawArm = [c \in Completers |-> FALSE]
  /\ apc = "idle"
  /\ prematureFire = FALSE

-----------------------------------------------------------------------------
\* Producer: admits an item. Monotonic; release store modeled as atomic.

Enqueue ==
  /\ enq < MaxEnq
  /\ enq' = enq + 1
  /\ UNCHANGED <<comp, tcsState, cpc, myComp, sawArm, apc, prematureFire>>

-----------------------------------------------------------------------------
\* Completers. Bump is the Interlocked.Increment; the verdict reads enq fresh
\* one instruction later and uses the completer's own bump result; the FIRE is
\* deferred (drain paths signal after the advancer release) and reads only the
\* slot - the verdict's freshness is whatever it was at the verdict.

CompleteBump(c) ==
  /\ cpc[c] = "idle"
  /\ comp < enq                       \* an admitted, uncompleted item exists
  /\ comp' = comp + 1
  /\ myComp' = [myComp EXCEPT ![c] = comp + 1]
  /\ cpc' = [cpc EXCEPT ![c] = "bumped"]
  /\ UNCHANGED <<enq, tcsState, sawArm, apc, prematureFire>>

\* The zero verdict (`return enq - comp` hitting 0): records whether the arm
\* was already up - a zero observed with the arm up is the documented
\* momentary-zero case even if depth rises again before the deferred fire.
CompleteVerdictZero(c) ==
  /\ cpc[c] = "bumped"
  /\ enq = myComp[c]
  /\ sawArm' = [sawArm EXCEPT ![c] = tcsState \in {"published", "held"}]
  /\ cpc' = [cpc EXCEPT ![c] = "zeroPending"]
  /\ UNCHANGED <<enq, comp, tcsState, myComp, apc, prematureFire>>

CompleteVerdictNonzero(c) ==
  /\ cpc[c] = "bumped"
  /\ enq # myComp[c]
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, tcsState, myComp, sawArm, apc, prematureFire>>

\* OnDepthReachedZero's pre-check: slot empty (or already taken/fired) - the
\* obligation dies; the armer's own post-publish re-check covers a late arm.
FireCheckNull(c) ==
  /\ cpc[c] = "zeroPending"
  /\ tcsState # "published"
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, tcsState, myComp, sawArm, apc, prematureFire>>

\* Exchange-take: the slot is HELD by this completer (not yet fired).
FireTake(c) ==
  /\ cpc[c] = "zeroPending"
  /\ tcsState = "published"
  /\ tcsState' = "held"
  /\ cpc' = [cpc EXCEPT ![c] = "took"]
  /\ UNCHANGED <<enq, comp, myComp, sawArm, apc, prematureFire>>

\* The blind deferred fire (~RevalidateFix, the shipped shape): fires whatever
\* arm the slot holds, on a verdict of arbitrary staleness. Premature iff the
\* verdict predated the arm AND depth is non-zero at the fire.
FireHeldBlind(c) ==
  /\ ~RevalidateFix
  /\ cpc[c] = "took"
  /\ tcsState' = "fired"
  /\ prematureFire' = (prematureFire \/ (comp # enq /\ ~sawArm[c]))
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, myComp, sawArm, apc>>

\* The revalidating fire (RevalidateFix): fire only against a still-zero depth.
RevalidateZero(c) ==
  /\ RevalidateFix
  /\ cpc[c] = "took"
  /\ comp = enq
  /\ tcsState' = "fired"
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, myComp, sawArm, apc, prematureFire>>

\* Stale verdict against a live arm: hand the tcs back (the CAS-back; in code
\* it can only fail against a cancelled-and-re-armed caller, where dropping
\* the held tcs is correct - not modeled, no cancellation here).
RevalidateNonzeroPutback(c) ==
  /\ RevalidateFix
  /\ cpc[c] = "took"
  /\ comp # enq
  /\ tcsState' = "published"
  /\ cpc' = [cpc EXCEPT ![c] = "putback"]
  /\ UNCHANGED <<enq, comp, myComp, sawArm, apc, prematureFire>>

\* The put-back is itself an arm: a completer that hit zero while the slot was
\* held read it as unavailable and skipped, so re-check depth after the
\* put-back and loop back to a re-take if zero materialized (the same
\* publish-then-recheck discipline as GetIdleTask's arm).
PutbackRecheckZero(c) ==
  /\ cpc[c] = "putback"
  /\ comp = enq
  /\ tcsState = "published"
  /\ tcsState' = "fired"
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, myComp, sawArm, apc, prematureFire>>

PutbackRecheckDone(c) ==
  /\ cpc[c] = "putback"
  /\ (comp # enq) \/ (tcsState # "published")
  /\ cpc' = [cpc EXCEPT ![c] = "idle"]
  /\ UNCHANGED <<enq, comp, tcsState, myComp, sawArm, apc, prematureFire>>

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
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp, sawArm, prematureFire>>

ArmDecide ==
  /\ apc = "idle"
  /\ comp # enq
  /\ apc' = "decided"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp, sawArm, prematureFire>>

\* The CAS publish: unconditional once decided (the code does not re-read depth
\* between the entry check and the CAS).
ArmPublish ==
  /\ apc = "decided"
  /\ tcsState' = "published"
  /\ apc' = "published"
  /\ UNCHANGED <<enq, comp, cpc, myComp, sawArm, prematureFire>>

\* Recheck after the full-fenced publish. Zero with the slot still published:
\* Exchange-take + self-fire (always legitimate: this zero is at-or-after the
\* arm by construction). A completer holding the slot resolves it instead.
ArmRecheckZero ==
  /\ RecheckFix
  /\ apc = "published"
  /\ comp = enq
  /\ tcsState = "published"
  /\ tcsState' = "fired"
  /\ apc' = "done"
  /\ UNCHANGED <<enq, comp, cpc, myComp, sawArm, prematureFire>>

\* Depth still positive, or the slot is in a completer's hands (held/fired:
\* the Exchange in SignalDrainWaiter returned null and the armer awaits the
\* task) - go await. Without the fix, the armer waits unconditionally.
ArmRecheckWait ==
  /\ apc = "published"
  /\ (comp # enq) \/ (tcsState # "published") \/ ~RecheckFix
  /\ apc' = "waiting"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp, sawArm, prematureFire>>

\* The awaited tcs fired: WaitForEmptyAsync returns.
ArmObserveFired ==
  /\ apc = "waiting"
  /\ tcsState = "fired"
  /\ apc' = "done"
  /\ UNCHANGED <<enq, comp, tcsState, cpc, myComp, sawArm, prematureFire>>

-----------------------------------------------------------------------------

Next ==
  \/ Enqueue
  \/ \E c \in Completers : CompleteBump(c) \/ CompleteVerdictZero(c)
       \/ CompleteVerdictNonzero(c) \/ FireCheckNull(c) \/ FireTake(c)
       \/ FireHeldBlind(c) \/ RevalidateZero(c) \/ RevalidateNonzeroPutback(c)
       \/ PutbackRecheckZero(c) \/ PutbackRecheckDone(c)
  \/ ArmShortCircuit \/ ArmDecide \/ ArmPublish
  \/ ArmRecheckZero \/ ArmRecheckWait \/ ArmObserveFired

\* Fairness: completions and the armer's own steps eventually happen; the
\* producer may stop at any point (no fairness on Enqueue).
Fairness ==
  /\ \A c \in Completers :
       /\ WF_vars(CompleteBump(c))
       /\ WF_vars(CompleteVerdictZero(c))
       /\ WF_vars(CompleteVerdictNonzero(c))
       /\ WF_vars(FireCheckNull(c))
       /\ WF_vars(FireTake(c))
       /\ WF_vars(FireHeldBlind(c))
       /\ WF_vars(RevalidateZero(c))
       /\ WF_vars(RevalidateNonzeroPutback(c))
       /\ WF_vars(PutbackRecheckZero(c))
       /\ WF_vars(PutbackRecheckDone(c))
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
NoPrematureWake == ~prematureFire

\* The lost-wake shape: everything done, waiter parked forever. TLC finds this
\* as a liveness violation of NoLostWake if the protocol has the hole.
NoLostWake == (tcsState \in {"published", "held"}) ~> (tcsState = "fired")

\* The armer always returns once everything completes.
ArmerTerminates == (apc \in {"decided", "published", "waiting"}) ~> (apc = "done")

=============================================================================
