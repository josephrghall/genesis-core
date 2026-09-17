# ADR-0008: Protected Store security boundary

Status: Accepted

Protected mode is optional. Canonical Record, derived Index, and local recovery artifacts operate within the selected library root whether protected mode is enabled or not.

`configure_protected` requires an existing root. Core canonicalizes that root and creates two persistent pieces of boundary evidence under `.genesis`: a versioned storage-boundary manifest and a unique sentinel. The manifest records the protected mode, the configured root fingerprint, and the expected sentinel.

On Unix, the root fingerprint contains the canonical path, device ID, and inode. On Windows, it contains the canonical path; the sentinel remains mandatory. On protected open and before protected writes, Core requires the selected root to exist, recomputes and compares its fingerprint, and verifies that the sentinel content still matches. Missing, invalid, or mismatched evidence fails closed.

This is configured-root identity validation. It prevents Core from accepting a different root as the configured protected Store when the recorded identity evidence or sentinel no longer matches.

It is not a complete filesystem access-control or process sandbox. Core does not currently guarantee host path ownership, reject every symlink or reparse-point substitution, provide encryption or key custody, or defend against a privileged host attacker. Deployments that require stronger controls must layer them outside this baseline, for example with stricter path/symlink policy, host ownership and permission policy, container or service-account restrictions, encryption, and OS/process sandboxing.
