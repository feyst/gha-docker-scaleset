# gha-docker-scaleset

One small daemon that runs **ephemeral GitHub Actions runners as Docker
containers**: a fresh container per job, removed when the job is done, with a
single cap on how many run at the same time. No Kubernetes, no inbound
webhook, and it works for repositories on a personal account.

```
            long-poll (outbound)                 docker.sock
GitHub  <---------------------->  gha-docker-scaleset  ------->  runner container per job
 jobs for scale set A, B, C        one listener per scale set      (removed after the job)
                                   one global max_runners
```

It is built on GitHub's own [runner scale set client](https://github.com/actions/scaleset),
the same API that Actions Runner Controller uses, instead of the classic
"register N runners and let them wait" model.

## Why

The classic self-hosted runner is one process that registers itself and waits
for one job at a time. To run jobs in parallel you keep N containers alive,
and on a personal account you cannot share them across repositories, so every
repository gets its own idle containers. Long-lived runners also collect state:
a stale registration after an unclean shutdown, a self-update that a restart
policy interrupts, and work directories that concurrent jobs trample.

With a runner scale set, GitHub tells one listener how many jobs are waiting,
and the listener creates a just-in-time runner that takes exactly one job and
then disappears. This daemon does that with Docker:

- **Zero idle containers.** A runner exists only while there is a job for it.
- **One cap across everything.** `max_runners` limits the containers of all
  scale sets together; each scale set can have its own lower cap. A slot freed
  by one scale set goes straight to the next one waiting.
- **A clean container per job**, with its own work directory that is deleted
  afterwards.
- **Behind NAT is fine.** The listener long-polls GitHub; nothing needs to
  reach the daemon.
- **Restarts are safe.** The scale set stays registered, running runners are
  taken over, and containers that stopped while the daemon was down are
  cleaned up along with their registration.
- **Image updates wait for an idle moment.** The runner image is pulled
  periodically; a new version is only used once no runner is alive, together
  with a fresh copy of its externals (see below).

## Quick start

```sh
make image                     # builds gha-docker-scaleset:dev
cp config.example.yml config.yml && $EDITOR config.yml
```

```yaml
# compose.yml
services:
  scaleset:
    image: gha-docker-scaleset:dev
    restart: unless-stopped
    stop_grace_period: 30s
    command: [-config, /etc/gha-docker-scaleset/config.yml]
    environment:
      GITHUB_TOKEN: ${GITHUB_TOKEN}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config.yml:/etc/gha-docker-scaleset/config.yml:ro
      # Same path inside and outside: the daemon deletes work directories.
      - /var/lib/gha-docker-scaleset:/var/lib/gha-docker-scaleset
      # The parent of externals_dir, same path: the daemon reads the stamp
      # that says which image the externals were copied from.
      - /home/runner:/home/runner
```

Then point your workflows at the labels of a scale set, e.g.
`runs-on: [self-hosted, Linux, X64]`.

### Credentials

Either `GITHUB_TOKEN` (a personal access token) or a GitHub App via
`GITHUB_APP_CLIENT_ID`, `GITHUB_APP_INSTALLATION_ID` and
`GITHUB_APP_PRIVATE_KEY` (or `GITHUB_APP_PRIVATE_KEY_FILE`). For a
repository-level scale set a classic token needs the `repo` scope; a
fine-grained token needs *Administration: read and write* on the repository.
See GitHub's [authentication docs](https://docs.github.com/en/actions/tutorials/use-actions-runner-controller/authenticate-to-the-api).

## Jobs that start containers: the same-path rule

A runner that uses the host's Docker socket starts job containers
(`container:`, `services:`) on the **host's** daemon, and passes its own paths
as bind mounts. The host resolves those paths on the host. So everything the
runner mounts into a job container must exist at the same path on the host:

- **the work directory** — each runner gets `work_dir/<runner-name>`, mounted
  at the same path;
- **the externals** — the node binaries every JavaScript action runs on,
  mounted into a job container as `/__e`. They live inside the runner image,
  so the daemon copies them to `externals_dir` on the host after pulling an
  image, stamps the copy with the image ID, and mounts that copy into every
  runner. It only does this while no runner is alive; after waiting 30
  minutes it holds new runners back until the running ones finish.

Without this a container job fails with `cannot open /__w/_temp/…sh` or
`/__e/node24/bin/node: no such file or directory`. If none of your jobs start
containers, leave `externals_dir` empty.

Anything else you mount into runners and pass on to job containers follows the
same rule: add it to `mounts` as `path:path`.

### Do not share a tool cache between runners

It is tempting to mount one tool cache (`/opt/hostedtoolcache`) into every
runner so that `setup-node` and friends do not download on every job. Don't:
CI jobs tend to start at the same moment and install the same version into
the same directory at once. Measured with four simultaneous jobs: all four
failed with `ENOENT`/`ENOTEMPTY` while copying node into the cache, after which
`setup-node` fell back to the image's system node.

Give every runner its own cache inside its work directory instead, by making
the tool directory variable empty so the runner falls back to
`<work directory>/_tool`:

```yaml
runner:
  env:
    AGENT_TOOLSDIRECTORY: ""   # or RUNNER_TOOL_CACHE, whichever your image sets
```

It is on the same path as on the host (so a job container gets it as `/__t`)
and disappears with the work directory. The price is one download per job.

## Using another runner image

The default is GitHub's `ghcr.io/actions/actions-runner`. Any image that
contains the runner works; set `command` to its `run.sh`. An image that runs as
root needs `RUNNER_ALLOW_RUNASROOT: "1"` in `env`. For example, for
`myoung34/github-runner`:

```yaml
runner:
  image: myoung34/github-runner:latest
  command: [/actions-runner/run.sh]
  env: {RUNNER_ALLOW_RUNASROOT: "1"}
  externals_dir: /actions-runner/externals
```

The runner's self-update is switched off for the scale set (a container that
lives for one job should not spend it updating); keep the image current
instead, which the daemon does by pulling it every `image_refresh`.

## Security

Mounting the Docker socket into a runner gives every job root on the host.
That is the same trade-off as the classic runner images that do this, and it is
only acceptable for repositories whose workflows you trust. **Do not use it for
public repositories that accept pull requests from forks.**

## Limitations

- The scale set API client is in public preview.
- On github.com, a scale set with several labels works; GitHub Enterprise
  Server needs 3.18+ with a feature flag for that.
- State is kept in memory; after a restart the daemon rebuilds it from the
  containers it labelled.
