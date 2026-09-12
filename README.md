# gha-docker-scaleset

**One small daemon that gives you GitHub Actions runners on your own machine —
a fresh Docker container per job, nothing idling in between.**

```
        outbound long-poll                     docker.sock
GitHub <──────────────────> gha-docker-scaleset ──────────> one container per job
                             one listener per repo            removed when done
                             one shared max_runners
```

No Kubernetes. No inbound webhook, so it works behind NAT. And it works for
repositories on a **personal account** — you do not need an organization.

It is built on [`actions/scaleset`](https://github.com/actions/scaleset),
GitHub's own client for the runner scale set API, the same API that Actions
Runner Controller uses.

---

## Why not just run the runner image?

The classic self-hosted runner registers itself and waits for one job at a
time. To run jobs in parallel you keep N containers alive, and on a personal
account you cannot share them between repositories, so every repository gets
its own idle containers. They also collect state: a stale registration after an
unclean shutdown, a self-update that a restart policy interrupts halfway, work
directories that concurrent jobs trample, caches that quietly differ per runner.

A runner scale set turns that around. GitHub tells one listener how many jobs
are waiting; the listener creates a just-in-time runner that takes exactly one
job and disappears.

|                        | Classic runner containers | **gha-docker-scaleset** | ARC | `dockerscaleset` example |
|------------------------|---------------------------|--------------------------|-----|--------------------------|
| Idle containers        | one per parallel job      | **none**                 | none | none |
| Needs Kubernetes       | no                        | **no**                   | yes | no |
| Works on a personal account | yes                  | **yes**                  | yes | yes |
| Cap across repositories | —                        | **yes**                  | per scale set | per scale set |
| Survives a restart     | yes                       | **yes, adopts runners**  | yes | no, deletes the scale set |
| Jobs using `container:`/`services:` | yes, with manual setup | **yes** | yes, in dind mode | not handled |
| Intended for production | yes                      | **yes**                  | yes | explicitly not |

---

## Quick start

You need Docker, and a token that may administer the repository (see
[Credentials](#credentials)).

**1. Write a `config.yml`:**

```yaml
max_runners: 6            # never more than 6 runner containers at once

runner:
  image: ghcr.io/actions/actions-runner:latest
  command: [/home/runner/run.sh]
  mounts:
    - /var/run/docker.sock:/var/run/docker.sock
  work_dir: /var/lib/gha-docker-scaleset/work
  externals_dir: /home/runner/externals

scale_sets:
  - name: my-repo
    url: https://github.com/OWNER/REPO
    labels: [self-hosted, Linux, X64]
    max_runners: 4        # this repository's own cap, within the global one
```

**2. Build and start it** — see [`examples/compose.yml`](examples/compose.yml).
There is no published image yet, so build it from a checkout:

```sh
make image                                   # builds gha-docker-scaleset:dev
GITHUB_TOKEN=ghp_… docker compose up -d
```

**3. Point a workflow at it.** The labels above are what `runs-on` matches, so
existing workflows usually need no change at all:

```yaml
jobs:
  test:
    runs-on: [self-hosted, Linux, X64]
```

That is it. Push a commit and watch:

```
$ docker compose logs -f
INFO Listening for jobs        scaleSet=my-repo labels="[self-hosted Linux X64]" maxRunners=4
INFO Runner started            scaleSet=my-repo runner=my-repo-6e005605
INFO Job started               runner=my-repo-6e005605 repository=OWNER/REPO job=Tests
INFO Job completed             runner=my-repo-6e005605 job=Tests result=succeeded
INFO Runner removed            runner=my-repo-6e005605
```

---

## What it does for you

**Zero idle containers.** A runner exists only while there is a job for it.

**One cap over everything.** `max_runners` limits all scale sets together, so
three repositories cannot suddenly put twelve jobs on one machine. Each scale
set can have its own lower cap. A slot freed by one repository goes to the next
one waiting, within a second.

**A clean container per job,** with its own work directory that is removed
afterwards. No leftovers from the previous job, ever.

**Restarts are safe.** Change the config and restart: running runners are
adopted and finish their job, the scale set stays registered, and containers
that stopped while the daemon was down are cleaned up along with their
registration at GitHub.

**Image updates that do not break a running job.** The runner image is pulled
periodically. A new version is only taken into use once no runner is alive,
together with a fresh copy of its externals — and if that moment never comes,
the daemon holds new runners back after 30 minutes to create one.

**It stays quiet.** A runner that dies before it ever got a job is logged with
the last lines of its output, so you see *why* instead of seeing a job hang.

---

## Configuration

| Key | Default | What it does |
|---|---|---|
| `max_runners` | – | Hard cap on runner containers across all scale sets. |
| `runner.image` | `ghcr.io/actions/actions-runner:latest` | Image every runner starts from. |
| `runner.command` | `[/home/runner/run.sh]` | The runner's start script inside that image. |
| `runner.env` | – | Extra environment variables for every runner. |
| `runner.mounts` | – | Extra binds, as `host:container[:options]`. |
| `runner.work_dir` | `/var/lib/gha-docker-scaleset/work` | Host directory holding one work directory per runner. |
| `runner.externals_dir` | – | The runner's externals directory; empty disables copying (see below). |
| `runner.image_refresh` | `24h` | How often the image is pulled. |
| `scale_sets[].name` | – | Scale set name, unique within its runner group. |
| `scale_sets[].url` | – | Repository, organization or enterprise URL. |
| `scale_sets[].labels` | the name | What `runs-on` matches. |
| `scale_sets[].max_runners` | `max_runners` | This scale set's own cap. |
| `scale_sets[].runner_group` | `default` | Runner group to register in. |

### Credentials

Either a personal access token in `GITHUB_TOKEN`, or a GitHub App via
`GITHUB_APP_CLIENT_ID`, `GITHUB_APP_INSTALLATION_ID` and
`GITHUB_APP_PRIVATE_KEY` (or `GITHUB_APP_PRIVATE_KEY_FILE`).

For a repository-level scale set a classic token needs the `repo` scope; a
fine-grained token needs *Administration: read and write* on the repository.
GitHub documents the full matrix
[here](https://docs.github.com/en/actions/tutorials/use-actions-runner-controller/authenticate-to-the-api).

---

## Jobs that start containers: the same-path rule

This is the part that silently breaks in every do-it-yourself setup, so it is
worth reading once.

A runner that uses the host's Docker socket starts its job containers
(`container:`, `services:`) on the **host's** daemon, and hands it its own paths
as bind mounts. The host resolves those paths on the host. So everything the
runner passes on must exist at the same path outside the container:

- **the work directory** — every runner gets `work_dir/<runner-name>`, mounted
  at exactly that path. Mount `work_dir` into the daemon at the same path too,
  so it can clean up afterwards;
- **the externals** — the node binaries every JavaScript action runs on, handed
  to a job container as `/__e`. They live inside the runner image, so the
  daemon copies them to `externals_dir` on the host, stamps the copy with the
  image ID, and mounts that copy into every runner.

Get this wrong and a container job fails with `cannot open /__w/_temp/….sh` or
`/__e/node24/bin/node: no such file or directory`. If none of your jobs start
containers, leave `externals_dir` empty and mount nothing extra.

### Do not share a tool cache between runners

It is tempting to mount one tool cache (`/opt/hostedtoolcache`) into every
runner so `setup-node` does not download on every job. Don't. CI jobs start at
the same moment and install the same version into the same directory at once.
Measured with four simultaneous jobs: all four failed with `ENOENT` and
`ENOTEMPTY` while copying node into the cache, after which `setup-node` fell
back to the image's ancient system node.

Give every runner its own cache inside its work directory instead, by making
the tool directory variable empty so the runner falls back to
`<work directory>/_tool`:

```yaml
runner:
  env:
    AGENT_TOOLSDIRECTORY: ""   # or RUNNER_TOOL_CACHE, whichever your image sets
```

It then sits at the same path as on the host (so a job container gets it as
`/__t`) and disappears with the work directory. The price is one download per
job. The same reasoning applies to a package manager cache in `$HOME`: use
`actions/cache` in the workflow, not a shared directory.

---

## Using a different runner image

The default is GitHub's `ghcr.io/actions/actions-runner`. Any image containing
the runner works; point `command` at its `run.sh`. An image that runs as root
needs `RUNNER_ALLOW_RUNASROOT`. For [`myoung34/github-runner`](https://github.com/myoung34/docker-github-actions-runner):

```yaml
runner:
  image: myoung34/github-runner:latest
  command: [/actions-runner/run.sh]
  env:
    RUNNER_ALLOW_RUNASROOT: "1"
    AGENT_TOOLSDIRECTORY: ""
  externals_dir: /actions-runner/externals
```

The runner's self-update is switched off for the scale set — a container that
lives for one job should not spend it updating — so keep the image current
instead, which the daemon does for you every `image_refresh`.

---

## Security

Mounting the Docker socket into a runner gives every job root on the host. That
is the same trade-off as every runner image that offers "docker support", and
it is only acceptable for repositories whose workflows you trust.
**Do not use it for a public repository that accepts pull requests from forks:**
a fork can change the workflow, and the workflow has your socket.

Leave the socket out of `mounts` if your jobs do not need Docker.

---

## Troubleshooting

**Nothing happens, the job stays queued.** Check that the labels of your scale
set match `runs-on` exactly, and that the daemon logs `Listening for jobs`.
Runners only exist while a job needs them, so an empty list under *Settings →
Actions → Runners* is normal.

**`Runner container stopped before it ran a job`.** The log line carries the
container's last output. Usually the image is too old for GitHub, or the start
command in `command` is wrong for this image.

**A container job cannot find its files.** See
[the same-path rule](#jobs-that-start-containers-the-same-path-rule).

**Everything is slower than with long-lived runners.** Every job starts clean,
so caches in `$HOME` are gone. Use `actions/cache` in the workflow.

---

## Development

Go runs in a container, so you need nothing but Docker:

```sh
make test     # go test -race ./...
make vet
make image    # builds gha-docker-scaleset:dev
```

## Limitations

- The scale set API client is in public preview; its interfaces can still move.
- A scale set with several labels works on github.com; GitHub Enterprise Server
  needs 3.18+ with a feature flag.
- On a personal account a scale set belongs to one repository, so list each
  repository separately. They still share `max_runners`.
- State lives in memory; after a restart the daemon rebuilds it from the
  containers it labelled.

## License

[MIT](LICENSE)
