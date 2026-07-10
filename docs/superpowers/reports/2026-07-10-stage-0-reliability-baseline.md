# Stage 0 Reliability Baseline

- Branch: feat/lark-push-filtering
- Pre-existing tracked modifications: 49 files
- Baseline tests: 378 passed, 12 skipped
- Coverage: 53 percent
- Application RSS: approximately 1.88 GiB of 2 GiB
- PostgreSQL size: approximately 1.99 GiB
- checkpoint_blobs: approximately 1.29 GiB
- checkpoint_writes: approximately 679 MiB
- Known lock state: uv.lock is stale

## Preserved behavioral changes

- Exchange Sent/Drafts Chinese and English folder aliases
- Explicit Lark SDK imports
- Existing formatter and lint cleanup

## Safety rule

No later task may reset or overwrite this baseline commit. Each task stages only its declared paths.
