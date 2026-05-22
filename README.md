# verlet

Download and explore Verlet datasets from the command line.

## Install

```bash
curl -sSL https://raw.githubusercontent.com/verlet/verlet-cli/main/install.sh | bash
```

This creates an isolated venv at `~/.verlet/venv` and symlinks `verlet` into `~/.local/bin`.

Or install manually:

```bash
pip install verlet
```

For development:

```bash
git clone https://github.com/verlet-robotics/verlet-cli.git
cd verlet-cli
pip install -e .
```

## Quick Start

```bash
# Authenticate — showcase access code, or a platform account
verlet auth login --kind showcase     # prompt for your access code
verlet auth login                     # platform account (device flow)

# Browse the datasets you can access
verlet datasets list

# Inspect one dataset
verlet datasets info <slug>

# Download a whole dataset
verlet datasets download <slug>
```

## Commands

Datasets — both teleoperation and egocentric (ego) — are served through one
unified `verlet datasets` surface. Showcase access codes see only the datasets
their code is granted; downloads are gated and whole-dataset.

> **Removed:** `verlet ego list/info/download` was retired. Use
> `verlet datasets list/info/download` instead — ego data is now addressed by
> dataset slug, not by internal segment ID.

### Authentication

```bash
verlet auth login --kind showcase   # showcase access code -> short-lived JWT
verlet auth login                   # platform account (OAuth device flow)
verlet auth logout                  # remove stored credentials
verlet auth status                  # show the active credential
```

### Datasets (`verlet datasets`)

```bash
# List the datasets you can access
verlet datasets list
verlet datasets list --task cooking --robot aria   # client-side filters
verlet datasets list --json

# Inspect a dataset (showcase codes also see their grants)
verlet datasets info <slug>

# Download a whole dataset
verlet datasets download <slug>
verlet datasets download <slug> -o ./data          # custom output directory
verlet datasets download <slug> --variant raw      # pick a variant you're granted
verlet datasets download <slug> --scope samples    # free-sample subset (showcase)
verlet datasets download <slug> --dry-run          # show the plan only
```

## Data Layout

Downloaded data is organized per dataset. Ego datasets land one directory per
segment; teleop datasets one directory per episode:

```
verlet-data/<slug>/
  segment_000000/        # ego
    rgb.mp4
    depth.mkv
    pose.parquet
  episode_000000/        # teleop
    episode_000000.parquet
    videos/<camera>.mp4
  meta/
```
