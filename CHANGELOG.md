# pg_profile changelog

## 0.3.3
- minor fix: Field _~SeqBytes_ of report section "Top tables by estimated sequentially scanned volume" was having value of first interval in cells of first and second interval.

## 0.3.2
- restored statements _queryid_ compatibility with current version of pgcenter utility
- query text storage improved
- linear interpolation of relation sizes slightly optimized

## 0.3.1
- relation sizes collection policy added
- data export and import feature added
- support of pg_stat_kcache extension version 2.2.0
- server description added
- FIX: template databases are now detected by pg_database.datistemplate field

## 0.2.1
- detailed sample taking timings collection feature.
- removed unmeaning zeros from report tables
- Latest two samples report function get_report_latest() added.
- FIX: time range based report-generating calls seldom failed. Reported by @cuprumtan
- FIX: kcache v.2.1.3 support #11 by @guilhermebrike
- FIX: field %Elapsed of sections "Top SQL by planning time" and "Top SQL by execution time"
- FIX: corrected statistics reset detection condition
- FIX: corrected diff reports join condition
- pg_stat_statements *queryid* field is shown in reports
- fixes and optimizations

## 0.1.4
- snapshot() function now tracks time elapsed taking sample on each server
- Top indexes by blocks fetched/read report section now contains index scan count field
- New _take_sample_subset()_ function can be used in parallel samples collection in case of many servers.
- fix of #9 reported by @Guzya - incorrect database and tablespace size in report

## 0.1.3
- Hotfix of #8 reported by @cuprumtan

## 0.1.2

- pg_stat_statements version 1.8 support:
  - Statement planning time
  - Statement WAL generation
- fix: "Hit(%)" field was incorrectly calculated
- Baseline creation on time range is now supported
- CPU timings are now shown in "Top SQL by elapsed time" if kcache extension was available
- I/O timing statistics added to "Statements statistics by database" section
- "WAL segments archived" and "WAL segments archive failed" statistics added to "Cluster statistics" section
- Workaround for pg_table_size and AccessExclusiveLocks. Now in many cases snapshot will be taken successfully without size of locked relation.
- Top vacuum and analyze count tables in reports
- Implicit index vacuum I/O load estimation
- Added trigger functions timing statistics
- Many pg_profile functions renamed to be more standard-compatible (see [doc/pg_profile.md](https://github.com/zubkov-andrei/pg_profile/blob/0.1.2/doc/pg_profile.md) file)
  - *gets* renamed to *fetches*
  - *snapshots* renamed to *samples*, and *snapshot()* function renamed to *take_sample()*, but for backward compatibility *snapshot()* call is possible.
  - *retention* renamed to *max_sample_age*
  - *node* renamed to *server*
  - *pages* renamed to *blocks*
- fixed issue #7 reported by @kayform
- Added a lot of table header hints

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

