# mem_perf

`mem_perf` is a standalone memory benchmark tool inspired by `mlc --max_bandwidth`.

## Features

- Core pinning (`--cores`)
- Configurable read/write ratio (`--read-percent`)
- Random or sequential access (`--access`)
- Realtime bandwidth output
- MLC-like latency-vs-bandwidth table mode (`--latency-sweep`)

## Build

```bash
cd mem_perf
make
```

## Standard Bandwidth Run

```bash
./mem_perf -a seq -r 100 -t 5 -i 1000 -b 128
```

Arguments:

- `--cores` or `-c`: core list, e.g. `0,2,4-7` (default: all available cores)
- `--read-percent` or `-r`: read ratio `[0,100]` (default: `50`)
- `--time` or `-t`: run seconds (default: `10`)
- `--interval-ms` or `-i`: realtime print interval in ms (default: `1000`)
- `--buffer-mb` or `-b`: per-thread buffer size in MB (default: `100`)
- `--access` or `-a`: `random` or `seq` (default: `random`)

Note:

- Total allocated background buffer = `buffer-mb * active_threads`

## Latency vs Bandwidth Sweep (MLC-like Output)

Run:

```bash
./mem_perf --latency-sweep -S 2 -a seq -r 100 -b 64
```

Example with explicit load points:

```bash
./mem_perf --latency-sweep -S 1 -P 0,50,100 -a seq -r 100 -b 64
```

Sweep-specific arguments:

- `--latency-sweep` or `-L`: enable latency-vs-bandwidth table output
- `--sweep-seconds` or `-S`: seconds per load point (default: `2`)
- `--sweep-pcts` or `-P`: load percentages, e.g. `0,25,50,75,90,100`
- `--probe-core` or `-p`: probe core id (default: last selected core)
- `--probe-mb` or `-m`: pointer-chasing probe working set in MB (default: `256`)

Output columns:

```text
load_pct,active_bg_threads,measured_bw_GBps,probe_latency_ns
```

All numeric outputs are printed as rounded integers.
