# pg_profile changelog

## 0.0.5
- Growth column in "Databases stats" report section
- Bgwriter and WAL-generation stats in new report section "Cluster stats"
- Explicit lock_timeout setting to 5 minutes in snapshot functions
- Snapshot now uses pg_advisory_lock on "magic" number 2174049485089987259. More than one snapshot() functions running is not allowed.
- "Top SQL by temp usage" section now shows temp utilization for workareas in "Work_" columns, and for temporary tables in "Local_" columns. Thanks, @lesovsky
- New report section section "Top Delete/Update tables with vacuum run count"
- New report section "Top growing indexes"
- Tables, Indexes and Functions names moved to dedicated tables


## 0.0.4a
- bugfix of #1 with postgresql on non-standard port, thanks @triwada

## 0.0.4
- queryid in pgCenter style
- normalized query storage table
- fixed report bug when processing statements with html tags
- minor fixes

## 0.0.3
- Baseline feature (exclude snapshots from default retention policy)
- Functions for displaying available snapshots and baselines
- Minor optimizations in report building functions

## 0.0.2
- extension parameters pg_profile.topn and pg_profile.retention

## 0.0.1
- first alpha version

