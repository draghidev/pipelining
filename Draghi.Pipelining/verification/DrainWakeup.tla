--------------------------- MODULE DrainWakeup ---------------------------
(*
Pipeline.DrainOnCompletionAsync's drain-wakeup TCS Dekker pair (Pipeline.cs
~518-538, "DRAIN-WAKEUP DEKKER BUG", June 11 2026, slon-recovery-strand-hang
backlog item).

Waiter (DrainOnCompletionAsync):

    while (advancing.IsHeld || waiters.Count > 0)
    {
        var tcs = new TaskCompletionSource(...);
        // ARM. The fix uses Interlocked.Exchange (full fence); the pre-fix
        // shape used Volatile.Write (release-only, no fence between the
        // store and following loads).
        Interlocked.Exchange(ref _drainWakeupTcs, tcs);
        // RECHECK after arm.
        if (!advancing.IsHeld && waiters.Count == 0) { disarm; break; }
        await tcs.Task;
    }

Signaler (callback drain / advancer release):

    // CHANGE condition (IsHeld := false via Interlocked release of latch,
    // or count-- via Interlocked decrement).
    SignalDrainWakeupIfWaiting()
        => Volatile.Read(ref _drainWakeupTcs)?.TrySetResult();

The Dekker pair (each thread does STORE then LOAD; without matching
fences either LOAD can hoist above its own STORE):

   Waiter:    STORE tcs := armed             LOAD needsWait
   Signaler:  STORE needsWait := false       LOAD tcs

The pre-fix bug (ArmFence = FALSE): Volatile.Write on the arm gives the
store release-only semantics, so the waiter's needsWait LOAD can be
reordered ABOVE the tcs STORE. The waiter reads stale needsWait = true
and parks. The signaler's needsWait STORE happens (Interlocked, fenced),
its tcs LOAD reads the still-null slot (the waiter's store has not
propagated), and skips the signal. Both threads convinced the other had
it. Lost wake = CompleteAsync hangs forever (~1/45k stress iterations,
June 2026 dump).

The fix (ArmFence = TRUE): Interlocked.Exchange on the arm is the matching
half. Its RMW pairs with the signaler's already-fenced condition write,
forming a sequenced Dekker pair: at least one thread sees the other's
store. Either the waiter reads needsWait = false and disarms, or the
signaler reads tcs = armed and signals.

Modeling: explicit per-thread snapshots that, under ~ArmFence, may read
the PRE-write value of the other thread's variable. Under ArmFence, the
snapshot reads the post-write value.

Properties:
  TypeOK
  NoLostWake     - once both threads have completed their pair AND needsWait
                   is false (signaler done), tcs eventually fires
                   (FAILS with ArmFence = FALSE; holds with TRUE)
  WaiterReturns  - liveness: the waiter eventually exits the loop
                   (FAILS under the bug; holds under the fix)
*)

EXTENDS Naturals, TLC

\* ArmFence: TRUE models Interlocked.Exchange (full fence). FALSE models
\* Volatile.Write (release-only) - the pre-fix shape.
CONSTANTS ArmFence

VARIABLES
  needsWait,        \* TRUE while signaler hasn't yet fired its condition mutation
  tcs,              \* "none" | "armed" | "fired"
  wpc,              \* waiter pc: "loop" | "armed" | "parked" | "done"
  spc,              \* signaler pc: "idle" | "mutated" | "loaded" | "done"
  waiterSawNeedsWait, \* the value the waiter's recheck LOAD observes (snapshot)
  signalerSawTcs    \* the value the signaler's TCS LOAD observes (snapshot)

vars == <<needsWait, tcs, wpc, spc, waiterSawNeedsWait, signalerSawTcs>>

TypeOK ==
  /\ needsWait \in BOOLEAN
  /\ tcs \in {"none", "armed", "fired"}
  /\ wpc \in {"loop", "armed", "parked", "done"}
  /\ spc \in {"idle", "mutated", "loaded", "done"}
  /\ waiterSawNeedsWait \in BOOLEAN
  /\ signalerSawTcs \in {"none", "armed", "fired"}

Init ==
  /\ needsWait = TRUE      \* the waiter is in the loop because needsWait is true
  /\ tcs = "none"
  /\ wpc = "loop"
  /\ spc = "idle"
  /\ waiterSawNeedsWait = TRUE
  /\ signalerSawTcs = "none"

-----------------------------------------------------------------------------
\* Waiter (DrainOnCompletionAsync).

\* The ARM step. With ArmFence = TRUE (Interlocked.Exchange) the store is a
\* full-fence RMW; the subsequent recheck LOAD is sequenced AFTER it and so
\* observes the signaler's mutation if it has landed. With ArmFence = FALSE
\* (Volatile.Write) the store is release-only; the recheck LOAD can be
\* reordered to BEFORE the arm and observe a stale needsWait = TRUE - that
\* is the lost-wake hole.
WaiterArm ==
  /\ wpc = "loop"
  /\ tcs' = "armed"
  /\ wpc' = "armed"
  \* Snapshot the value the waiter will see at recheck. Under the fix, the
  \* fence forces fresh-needsWait; under the bug, the snapshot is taken from
  \* the PRE-arm view of needsWait so the recheck can be stale.
  /\ waiterSawNeedsWait' = IF ArmFence THEN needsWait ELSE TRUE
  /\ UNCHANGED <<needsWait, spc, signalerSawTcs>>

\* The RECHECK: needsWait observed via the snapshot. False -> disarm and
\* break out of the loop. True -> park on the armed tcs.
WaiterRecheckBreak ==
  /\ wpc = "armed"
  /\ ~waiterSawNeedsWait
  /\ tcs' = "none"  \* disarm
  /\ wpc' = "done"
  /\ UNCHANGED <<needsWait, spc, waiterSawNeedsWait, signalerSawTcs>>

WaiterPark ==
  /\ wpc = "armed"
  /\ waiterSawNeedsWait
  /\ wpc' = "parked"
  /\ UNCHANGED <<needsWait, tcs, spc, waiterSawNeedsWait, signalerSawTcs>>

\* The awaited tcs fires; the waiter returns.
WaiterAwaitedFires ==
  /\ wpc = "parked"
  /\ tcs = "fired"
  /\ wpc' = "done"
  /\ UNCHANGED <<needsWait, tcs, spc, waiterSawNeedsWait, signalerSawTcs>>

-----------------------------------------------------------------------------
\* Signaler (callback drain release / advancer release).

\* The CHANGE step: mutate needsWait to FALSE. The signal-side mutation is
\* via Interlocked.Exchange (latch release) or Interlocked.Decrement (count)
\* - either way, a full-fence RMW, so the subsequent tcs LOAD is sequenced
\* AFTER it from the signaler's own thread perspective. The cross-thread
\* pairing with the waiter's fence is what matters: under ArmFence the
\* signaler's tcs LOAD pairs with the waiter's arm STORE and sees armed if
\* the waiter armed first; under ~ArmFence the tcs LOAD can be stale and
\* miss the waiter's armed.
SignalerChangeCondition ==
  /\ spc = "idle"
  /\ needsWait' = FALSE
  /\ spc' = "mutated"
  /\ UNCHANGED <<tcs, wpc, waiterSawNeedsWait, signalerSawTcs>>

\* The signaler reads the tcs slot.
SignalerLoadTcs ==
  /\ spc = "mutated"
  \* Under the fix, the signaler's tcs LOAD sees the current tcs value. Under
  \* the bug, the signaler can still see the PRE-arm tcs (the waiter's arm
  \* STORE is reorderable and may not be globally visible yet).
  /\ signalerSawTcs' = IF ArmFence THEN tcs ELSE "none"
  /\ spc' = "loaded"
  /\ UNCHANGED <<needsWait, tcs, wpc, waiterSawNeedsWait>>

\* Signal if the snapshot said armed; otherwise skip (no waiter parked yet
\* from the signaler's view).
SignalerFire ==
  /\ spc = "loaded"
  /\ signalerSawTcs = "armed"
  /\ tcs' = "fired"
  /\ spc' = "done"
  /\ UNCHANGED <<needsWait, wpc, waiterSawNeedsWait, signalerSawTcs>>

SignalerSkip ==
  /\ spc = "loaded"
  /\ signalerSawTcs # "armed"
  /\ spc' = "done"
  /\ UNCHANGED <<needsWait, tcs, wpc, waiterSawNeedsWait, signalerSawTcs>>

-----------------------------------------------------------------------------

Next ==
  \/ WaiterArm \/ WaiterRecheckBreak \/ WaiterPark \/ WaiterAwaitedFires
  \/ SignalerChangeCondition \/ SignalerLoadTcs \/ SignalerFire \/ SignalerSkip

Fairness ==
  /\ WF_vars(WaiterArm)
  /\ WF_vars(WaiterRecheckBreak)
  /\ WF_vars(WaiterPark)
  /\ WF_vars(WaiterAwaitedFires)
  /\ WF_vars(SignalerChangeCondition)
  /\ WF_vars(SignalerLoadTcs)
  /\ WF_vars(SignalerFire)
  /\ WF_vars(SignalerSkip)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* The waiter's loop eventually exits. FAILS under ArmFence = FALSE: the
\* lost-wake interleaving leaves the waiter parked forever.
WaiterReturns == (wpc \in {"loop", "armed", "parked"}) ~> (wpc = "done")

\* Once the signaler has done its job (needsWait = FALSE) and the waiter has
\* armed, the tcs either fires or is disarmed. FAILS under ArmFence = FALSE.
NoLostWake == (tcs = "armed" /\ ~needsWait) ~> (tcs \in {"fired", "none"})

================================================================================
