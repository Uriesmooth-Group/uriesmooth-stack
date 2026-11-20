# Uriesmooth Stack

Android-first automation core for Uriesmooth Ltd.

## Setup in Termux

```bash
./scripts/setup.sh
.venv/bin/pytest
.venv/bin/alphauriel list
.venv/bin/alphauriel run hello
```

## Background jobs

```bash
.venv/bin/alphauriel enqueue hello
.venv/bin/alphauriel worker
```

In another Termux session:

```bash
.venv/bin/alphauriel jobs
```

The worker processes persistent SQLite jobs and retries failures up to the configured attempt limit. The scheduler is a simple interval loop; keep Android battery restrictions in mind.

## Job execution deadlines

The default cooperative job execution deadline is 300 seconds.

Configure it with:

```bash
ALPHAURIEL_JOB_TIMEOUT_SECONDS=300
```

Context-aware apps can call `context["check_cancelled"]()` and inspect
`context["remaining_seconds"]()`.

Deadline expiry is treated as a job failure and follows the retry policy.
Lease loss is also exposed through the execution context.

**Limitation:** This is cooperative cancellation. It cannot forcibly
terminate a blocking Python thread or an app that ignores its context.
Hard execution limits require process isolation and resource controls.
