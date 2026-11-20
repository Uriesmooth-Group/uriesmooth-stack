# Uriesmooth Stack Architecture

The initial runtime is Python-first and Android/Termux-compatible.

- `alphauriel_pascal/registry.py`: discovers app folders with valid `app.json` and `app.py`.
- `queue.py`: stores persistent job state in SQLite.
- `worker.py`: claims and processes queued jobs, records results and retries failures.
- `cli.py`: exposes app listing, direct runs, queueing, worker, job status and a simple interval scheduler.
- `apps/`: one folder per registered app.

## Android limitations

`tmux` keeps terminal sessions available while Termux remains alive. Android battery controls, process termination and device shutdown can still interrupt jobs. Use Termux:Boot and battery settings only after validating the service; production-critical work should eventually move to a reliable server.

## Security

Never commit `.env`, credentials, tokens or private keys. App code runs with the same OS permissions as the Termux user; only install trusted apps.

## Job execution deadlines

The worker supplies apps with a cooperative execution context containing
a UTC deadline, remaining-time function, cancellation predicate, and
cancellation check. Deadline expiry is handled as a job failure; lease
loss prevents the worker from committing a result.

This does not forcibly stop blocking Python code. Production hard time
limits require process isolation and resource controls.
