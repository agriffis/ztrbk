# ztrbk

ZFS snapshot manager inspired by [btrbk](https://github.com/digint/btrbk)

## Introduction

ztrbk is a lightweight backup and snapshot management tool for ZFS filesystems, written in Clojure using [Babashka](https://babashka.org/). It automates snapshot creation, replication to target datasets, with flexible retention policies similar to btrbk.

Key features:
- Automatic snapshot creation with customizable naming
- Incremental replication to target datasets
- Flexible retention policies with preserve-min and preserve tiers
- Support for hourly, daily, weekly, monthly, and yearly retention periods
- Dry-run mode for safe testing
- EDN configuration format

## Quickstart

1. **Install Babashka** (if not already installed):
   ```bash
   # macOS
   brew install bbedit/babashka/babashka

   # Linux
   bash <(curl -s https://raw.githubusercontent.com/babashka/babashka/master/install)
   ```

2. **Install ztrbk**:
   ```bash
   wget https://raw.githubusercontent.com/agriffis/ztrbk/refs/heads/main/ztrbk.bb
   chmod +x ztrbk.bb
   sudo mv ztrbk.bb /usr/local/bin/ztrbk
   ```

3. **Create a configuration file** (see example below):
   ```bash
   sudo nano /etc/ztrbk.edn
   ```

4. **Test with dry-run**:
   ```bash
   sudo ztrbk -n run
   ```

5. **Run for real**:
   ```bash
   sudo ztrbk run
   ```

6. **Set up a cron job** for automated execution:
   ```crontab
   # Run hourly
   0 * * * * /usr/local/bin/ztrbk run
   ```

## Example Configuration

Create `/etc/ztrbk.edn` with your backup policy:

```clojure
{:global
 {:prefix "ztrbk_"
  :preserve-hour-of-day 0
  :preserve-day-of-week :sunday
  :preserve-week-of-month 1
  :preserve-month-of-year :january
  :snapshot-preserve-min :all
  :snapshot-preserve :no
  :target-preserve-min :all
  :target-preserve :no}

 :datasets
 [{:source "tank/data"
   ;; Keep all snapshots for 14 days, then tiered retention
   :snapshot-preserve-min {:days 14}
   :snapshot-preserve {:days 14 :weeks 8 :months 24}
   :targets
   [{:target "backup/data"
     ;; Keep all for 30 days, then more aggressive retention on backup
     :target-preserve-min {:days 30}
     :target-preserve {:days 60 :months :all :years 10}}]}

  ;; Simple example: only keep latest snapshot
  {:source "tank/temp"
   :snapshot-preserve-min :latest
   :snapshot-preserve :no
   :targets [{:target "tank/temp-backup"
              :target-preserve-min :latest
              :target-preserve :no}]}

  ;; Example: aggressive local retention, longer-term on backup
  {:source "tank/documents"
   :snapshot-preserve-min {:days 7}
   :snapshot-preserve {:days 7 :weeks 4 :months 12}
   :targets [{:target "backup/documents"
              :target-preserve-min {:months 3}
              :target-preserve {:months 12 :years :all}}]}]}
```

### Configuration Options

**Global and per-dataset/target options:**
- `:prefix` - Snapshot name prefix (default: `"ztrbk_"`)
- `:preserve-hour-of-day` - Hour when day starts, 0-23 (default: `0`)
- `:preserve-day-of-week` - Day for weekly snapshots: `:sunday` through `:saturday`, or 0-7 (default: `:sunday`)
- `:preserve-week-of-month` - Week for monthly snapshots, 1-4 (default: `1`)
- `:preserve-month-of-year` - Month for yearly snapshots: `:january` through `:december`, or 1-12 (default: `:january`)
- `:snapshot-preserve-min` - Local snapshot minimum retention (default: `:all`)
- `:snapshot-preserve` - Local snapshot tiered retention (default: `:no`)
- `:target-preserve-min` - Target snapshot minimum retention (default: `:all`)
- `:target-preserve` - Target snapshot tiered retention (default: `:no`)

**Preserve-min values:**
- `:all` - Keep everything forever (default)
- `:latest` - Keep only the newest snapshot
- `:no` - Don't keep anything (rely on preserve policy only)
- `{:hours N}`, `{:days N}`, `{:weeks N}`, `{:months N}`, `{:years N}` - Keep all snapshots within duration

**Preserve policy format (map with any combination):**
- `{:hours N}` - Keep N hourly snapshots
- `{:days N}` - Keep N daily snapshots
- `{:weeks N}` - Keep N weekly snapshots
- `{:months N}` - Keep N monthly snapshots
- `{:years N}` - Keep N yearly snapshots
- Use `:all` for unlimited (e.g., `{:months :all}`)

**How retention works:**
1. `preserve-min` keeps ALL snapshots within the specified duration
2. `preserve` keeps specific snapshots (hourly/daily/weekly/monthly/yearly) beyond preserve-min
3. Snapshots are destroyed only if they fall outside both policies

## Feature Differences from btrbk

While ztrbk is inspired by btrbk's approach to snapshot management, there are several key differences:

### Similarities
- Two-tier retention policy (preserve-min + preserve)
- Support for hourly, daily, weekly, monthly, yearly retention periods
- Incremental replication
- Dry-run mode
- Flexible configuration

### Differences

| Feature | btrbk | ztrbk |
|---------|-------|-------|
| **Implementation** | Perl | Clojure (Babashka) |
| **Filesystem** | Btrfs | ZFS |
| **Config format** | Custom text | EDN (Clojure data) |
| **Remote backups** | SSH push/pull | Local targets only (currently) |
| **Scheduling** | External (cron) | External (cron) |
| **Raw backups** | Supported | Not implemented |
| **Archive mode** | Supported | Not implemented |
| **Resume support** | Yes | No |
| **Lockfiles** | Yes | No |
| **Rate limiting** | Yes | No |
| **Transaction log** | Optional | No |
| **Stream compression** | Supported | Uses ZFS native (`-w` flag) |

### Simplified Features

ztrbk intentionally simplifies some aspects:
- **No SSH support yet**: Currently only supports local targets (but ZFS send/receive can be piped through SSH manually)
- **No subvolume handling**: Works with ZFS datasets, not Btrfs subvolumes
- **Simpler config**: EDN format is more structured but requires learning Clojure syntax
- **No transaction log**: Operations are not logged to a transaction file
- **No lockfile management**: Single execution assumed (use external locking if needed)

### Extended Features

Some areas where ztrbk differs:
- **EDN configuration**: Native Clojure data structures allow for more programmatic configuration
- **Babashka runtime**: Fast startup time, no compilation needed
- **ZFS-specific**: Takes advantage of ZFS features like raw encrypted sends (`-w`)

## Credit and Inspiration

This project was inspired by [btrbk](https://github.com/digint/btrbk) by Axel Burri, an excellent backup tool for Btrfs filesystems. The retention policy design, particularly the two-tier approach with preserve-min and preserve, is based on btrbk's proven strategy.

**Important**: ztrbk is a **clean-room implementation** created by studying btrbk's documentation and user-facing behavior, NOT by examining its source code. Any similarities in approach are due to solving the same problem (snapshot management) in similar ways. All code in ztrbk was written independently for ZFS.

Thank you to Axel Burri and the btrbk contributors for their excellent work and clear documentation!

## License

Written in 2025 by Aron Griffis <aron@arongriffis.com>

To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to this software to the public domain worldwide. This software is distributed without any warranty.

CC0 Public Domain Dedication at http://creativecommons.org/publicdomain/zero/1.0/
