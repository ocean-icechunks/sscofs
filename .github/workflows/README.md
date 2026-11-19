# One-time Icechunk copy

`copy-best-icechunk.yml` runs only from **Actions → Copy best Icechunk to Source
Cooperative (one time) → Run workflow**. It has no schedule and requires no
stored AWS secrets: Source Cooperative uses the same OIDC role as the nightly
access check. The AWS source must allow anonymous listing and reads, as used
by the notebook.

| Source | Default destination | Inventory |
| --- | --- | --- |
| `s3://nextgen-dmac-cloud-ingest-cmorse-dev/sscofs/sscofs-icechunk-best/` | `s3://ocean-icechunks/sscofs/sscofs-icechunk-best/` on `https://data.source.coop` | Counts and bytes are recorded in the run summary; the full dated inventory is saved as a run artifact. |

The destination preserves the source prefix and every relative object key,
including Icechunk configuration, branches, snapshots, manifests, and chunks.
No version folder is added: this is a copy of the existing named store. No
objects are excluded or deleted. Embedded virtual references retain their
original URLs; copying the repository does not relocate the external data.

Stop writes and garbage collection on the source for the entire real run.
Run once with the default **dry_run=true** to review the source inventory and
planned uploads. Then run with **dry_run=false** to perform the copy. The
destination prefix can be changed within `sscofs/`, including to a scratch
prefix for a trial. These workflow files only prepare the operation; creating
them does not start a transfer.

The copy streams through the runner rather than staging the store on its disk.
Rclone uses separate S3 remotes so public AWS reads never receive Source
Cooperative credentials. Source Cooperative uses path-style addressing and
multipart uploads with 64 MiB parts, signed Accept-Encoding disabled, and no
stored checksums, following the known working proxy settings. Authentication
uses `us-west-2` to match the supplied OIDC configuration.

`--immutable` prevents replacement of objects rclone detects as different;
existing objects of the same size may be skipped. The subsequent full download
comparison detects byte differences even when sizes match. Failures leave the
copied objects in place. Rerun to resume an interrupted copy; if an existing
object differs, use a fresh destination prefix and investigate the old copy.
Do not use this workflow to overwrite an existing store. Avoid other writers
at the destination while it runs.

A successful real run requires a full byte comparison of every source and
destination object, no extra destination objects, and an unchanged source
inventory across the run. Budget bandwidth and time for downloading both
copies during verification. A failed or interrupted destination is incomplete
and must not be used by readers. No live copy or verification has been run as
part of adding this workflow.
