# Initial-publication provenance

The signed annotated tag `initial-publication` identifies the source snapshot first published as
Draghi.Pipelining. `initial-publication.tag` is the exact Git tag object, and its detached
OpenTimestamps proof is `initial-publication.tag.ots`.

The tag points to commit `59802d90922b719222ae9ed5dc83917d329e6acf` and is signed by Nino
Floris with OpenPGP primary-key fingerprint:

```text
0772 2EE9 8C00 2F36 922E  2C7C 622A 9882 5766 7DFB
```

Verify that the checked-in object matches the tag and that its signature is valid:

```text
git cat-file tag initial-publication | cmp - provenance/initial-publication.tag
git verify-tag initial-publication
```

Verify the independent timestamp with an OpenTimestamps client:

```text
ots verify provenance/initial-publication.tag.ots
```

The initial proof contains calendar attestations while awaiting confirmation. After it is anchored
in Bitcoin, `ots upgrade` can embed the complete attestation into the same proof file.
