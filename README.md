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
# Authenticate with your access code
verlet login

# Browse available data
verlet ego list
verlet ego list --detailed --category Kitchen

# Download data
verlet ego download -o ./data --category Kitchen

# Check segment details
verlet ego info station-1__episode_042_seg5
```

## Commands

### Authentication

```bash
verlet login       # Prompt for access code, store JWT
verlet logout      # Remove stored credentials
```

### EgoDex Hand Pose Data (`verlet ego`)

```bash
# List categories and segment counts
verlet ego list
verlet ego list --task station-1
verlet ego list --category Kitchen
verlet ego list --detailed

# Download segments
verlet ego download                              # all segments to ./verlet-data/ego/
verlet ego download -o ./data                    # custom output directory
verlet ego download --category Kitchen           # filter by category
verlet ego download --include "*.hdf5,*.mp4"     # only specific file types
verlet ego download --exclude "*.egorec,*.rrd"   # skip large files
verlet ego download --parallel 16                # concurrency (default 8)
verlet ego download --dry-run                    # show download plan only

# Segment info
verlet ego info SEGMENT_ID
```

## Data Layout

Downloaded data is organized by station and episode:

```
verlet-data/ego/
  station-1/
    episode_042_seg5/
      segment.egorec
      hands.npz
      overlay.mp4
      recording.rrd
      egodex/manipulation/0.hdf5
      egodex/manipulation/0.mp4
```
