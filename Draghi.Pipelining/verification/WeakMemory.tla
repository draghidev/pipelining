------------------------------ MODULE WeakMemory ------------------------------
(* Helpers for modeling weak-memory writes in TLA+ specs of .NET code.

   Pattern: each modeled field has two state variables, <field> (writer-local)
   and <field>Visible (globally observable). They diverge only when a write is
   modeled as release-only (Volatile.Write / STLR); a separate Propagate<Field>
   transition with WF_vars closes the gap, modeling cache coherence.

   Use FencedWriteOk for Interlocked.Exchange (seq-cst) or for plain reference
   writes (the CLR emits STLR for reference stores on ARM64 to support concurrent
   GC; the release fence is a guaranteed side effect of that choice, separate from
   the GC write barrier itself which is just card-mark bookkeeping).
   Use WeakWriteOk for Volatile.Write or value-T plain writes. Readers should
   observe <field>Visible.

   Caveats: store-side visibility only. Reader-side reordering and load hoisting
   aren't modeled (use herd7 for litmus-test-level questions). Two relaxed writes
   to different fields can be observed in any order; if ordering matters, fence
   the second write. *)

(* Full-fence write: both local and visible updated to `value`. *)
FencedWriteOk(local_next, visible_next, value) ==
  /\ local_next = value
  /\ visible_next = value

(* Release-only / plain write: local updates, visible lags. Pair with PropagateOk. *)
WeakWriteOk(local_next, visible_next, value, visible_current) ==
  /\ local_next = value
  /\ visible_next = visible_current

(* Visibility propagation. Use with WF_vars(...) for liveness. *)
PropagateOk(local, visible_next) ==
  visible_next = local

=============================================================================
