# pg_profile changelog

## 0.1.1

Due to total refactoring done in this release, migration from 0.0.7 is very difficult, so right now migration from 0.0.7 is not supported.

- Code reorganization, new Makefile
- Sequential scanned table now sorted by scanned pages estimation
- Order correction in top tables by read IO
- TOAST tables is now calculated and shown with main tables
- More fields in "Top SQL by I/O wait time" section
- Non-superuser install (see Privileges in doc)
- Collect postgres GUC parameters historic values and show them in reports
- Tablespaces support: size, growth and objects belong (Daria Vilkova)
- Fixed object rename issue
- New sections in report: "Top SQL by shared dirtied", "Top SQL by shared written", "Top tables by gets", "Top indexes by gets"
- Improved statement list in reports - now clicked statement_id is highlighted
- Workaround for amazon RDS
- Database sizes is now shown in reports (Daria Vilkova)
- Reports can be generated using time intervals (tstzrange type)
- *snapshot_show()* now displays info about stats reset, also report contains information about stats reset
- a lot of bugfixes and other improvements

## 0.0.7

- Interval compare reports
- Sequential scans now ordered by appox. seq. scanned pages (based on relation size)
- Added report examples in repository (issue #5 reported by @saifulmuhajir)
- Simplified baseline management for local node

## 0.0.6
- Collect data from other PostgreSQL clusters

## 0.0.5a
- bugfix: In index stats base relation size was displayed as IX size

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

