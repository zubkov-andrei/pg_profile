# pg_profile
This extension for PostgreSQL helps you to find out most resource intensive activities in your PostgreSQL database. This extension is in beta-testing state.
## Concepts
This extension is based on standard statistics views of PostgreSQL. It is written in pl/pgsql and doesn't need any external libraries or software, but PostgreSQL database itself, and a cron-like tool performing periodic tasks. Initially developed and tested on PostgreSQL 9.6, but may work in previous versions too (not tested).

Some sort of historic repository will be created in your database by this extension. This repository will hold statistics "snapshots" for your database, just as Oracle AWR do. Snapshot is taken by calling _snapshot()_ function. PostgreSQL doesn't have any job-like engine, so you'll need to use cron.

Periodic snapshots can help you finding most resource intensive statements in the past. Suppose, you were reported performance degradation several hours ago. No problem, you can build a report between several snapshots to see load profile of your database between snapshots. It's worse using some monitoring tool such a Zabbix to know exact time when performance issues was happening. There is several tools to send PostgreSQL stats into Zabbix, but I'm using my own tool, written in Java - it collects for me statistics from many Oracle and Postgres servers and sends data to Zabbix.

Of course, you can make an explicit snapshot before running any batch processing, and after it will end.

Any time you making a snapshot, _pg_stat_statements_reset()_ will be called, ensuring you will not loose statements due to reaching value of _pg_stat_statements.max_. Also, report will contain section, informing you if captured statements count in any snapshot reaches 90% of _pg_stat_statements.max_.
## Prerequisites
Extensions, you'll need:
* pg_stat_statements (for collecting statements stats) - see PostgreSQL documentation for prooper setup
* dblink (for collecting object stats from cluster databases)

Ensure you set statistics collecting parameters:
```
track_activities = on
track_counts = on
track_io_timing = on
track_functions = on
```
And pg_stat_statements parameters (your values may differ):
```
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.max = 1000
pg_stat_statements.track = 'top'
pg_stat_statements.save = off
```
You must install and use _pg_profile_ extension as cluster superuser (for example, _postgres_), because only superusers can see all statements in _pg_stat_statements_ view. And user, that will make snapshots must be able to login in any database of your cluster without providing a password. Dblink is used for collecting object statistics. Peer authentication preferred - make sure your _pg_hba.conf_ allows this.
## Installation
### Step 1 Installation of extension files
There is two ways for installing extension files:
* Makefile provided, so you can use
```
# make install
```
But for it to work, you'll need develop-packages of PostgreSQL.
* Or you can just manual copy files of extension (_pg_profile*_) to PostgreSQL extensions location, which is
```
# cp pg_profile* `pg_config --sharedir`/extension
```
### Step 2 Installing extensions
Most easy way is to install everything in public schema of postgres database:
```
postgres=# CREATE EXTENSION dblink;
postgres=# CREATE EXTENSION pg_stat_statements;
postgres=# CREATE EXTENSION pg_profile;
```
If you want to install pg_profile in other schema, just create it, and install extension in that schema (pg_stat_statements is recommended to be in public schema):
```
postgres=# CREATE EXTENSION dblink;
postgres=# CREATE EXTENSION pg_stat_statements;
postgres=# CREATE SCHEMA profile;
postgres=# CREATE EXTENSION pg_profile SCHEMA profile;
```
All objects will be created in schema, defined by SCHEMA clause. Installation in dedicated schema is recommended way - extension will create its own tables, sequences and functions. It is a good idea to keep them separate. If you do not whant to specify schema qualifier when using module, consider changing _search_path_ setting.
### Step 3 Update to new version
New versions of pg_profile will contain all necessary to update from any previous version. So, in case of update you will only need to install extension files (see Step 1) and update this extension, like this:
```
postgres=# alter extension pg_profile update;
```
All your historic data will remain unchanged if possible.
## Using pg_profile
### Setting extension parameters
You can define extension parameters like any other parameter in _postgresql.conf_ or in _postgresql.auto.conf_. Default values a shown in following list:
* _pg_profile.topn = 20_ - Number of top objects (statements, relations, etc.), to be reported in each sorted report table. Also, this parameter affects size of a snapshot - the more objects you want appear in your report, the more objects we need to keep in a snapshot.
* _pg_profile.retention = 7_ - Retention time of snapshots in days. Snapshots, aged _pg_profile.retention_ days and more will be automatically deleted on next _snapshot()_ call.
### Creating snapshots
You must create at least 3 snapshots to be able to build a report between 2nd and 3rd snapshot.
Snapshots are taken by calling _snapshot()_ function. I'm using cron of user _postgres_ like this:
```
*/30 * * * *   psql -c 'SELECT profile.snapshot()' > /dev/null 2>&1
```
Schema qualifier can be omitted, if extension installed in public schema:
```
*/30 * * * *   psql -c 'SELECT snapshot()' > /dev/null 2>&1
```
Call this command several times, if is returns increasing numbers of snapshots, you're done:
```
$ psql -c 'SELECT profile.snapshot()'
 snapshot
----------
      200
(1 row)
```
To display existing snapshots, you call function _snapshot_show(days integer)_. The only parameter is _days_, it defines interval in days in the past for displaying snapshots. This parameter may be omitted (or set to null) for displaying all snapshots:
```
postgres=# select * from profile.snapshot_show(1);
 snapshot |       date_time
----------+------------------------
...
      480 | 2017-09-11 10:00:01+03
      481 | 2017-09-11 10:30:01+03
      482 | 2017-09-11 11:00:01+03
      483 | 2017-09-11 11:30:02+03
      484 | 2017-09-11 12:00:02+03
      485 | 2017-09-11 12:30:01+03
      486 | 2017-09-11 13:00:02+03
(48 rows)
```
### Building a report
You build a report in HTML format using two snap_is's:
```
$ psql -qt -c "SELECT profile.report(480,482)" -o report_480_482.html
```
Now you can view file _report_480_482.html_ in any web browser.

This AWR-like report will contain several tables describing database cluster load profile. Here you will find queries, with most time elapsed, most gets, most reads, I/O waits, and so on. You will see per database statistics, such as hit ratio, calls, reads, and so on. There will be statistics for database objects - most DML-intensive tables, most scanned tables, most growth tables, and so on. Finally, you will be reported on most readed tables and indexes.

#### Sections of a report
The report will contain following sections:
* Cluster statistics
  * Databases stats
  * Statements stats by database
* SQL Query stats
  * Top SQL by elapsed time
  * Top SQL by executions
  * Top SQL by I/O wait time
  * Top SQL by gets
  * Top SQL by temp usage
  * Complete List of SQL Text
* Schema objects stats
  * Most scanned tables
  * Top DML tables
  * Top growth tables
  * Unused indexes
* I/O Schema objects stats
  * Top tables by I/O
  * Top indexes by I/O
* User function stats
  * Top functions by total time
  * Top functions by executions
* Vacuum related stats
  * Tables ordered by dead tuples ratio
  * Tables ordered by modified tuples ratio
### Baselines
Baseline is a snapshot sequence, excluded from default snapshot retention policy. Each baseline can have individual retention period, or does not have it at all - in this case baseline won't deleted automatically. 
You can use baselines when you want to save information about database workload on certain time period. For example, you may want to save snapshots, gathered during load testing, or during nominal load on your system.
Future releases will be able to compare load, captured in two different time intervals. Right now, you can save snapshot series in time and build reports on included snapshots.
Baseline functions:
- __baseline_new(name varchar(25), start_id integer, end_id integer, days integer)__ - create a baseline
  - _name_ - baseline name. Each baseline must have unique name.
  - _start_id_, _end_id_ - first and last snapshots, included in baseline.
  - _days_ - baseline retention time. Defined in days since _now()_. This paremeter may be omitted (or be set to _null_), meaning infinite retention.
- __baseline_drop(name varchar(25))__ - drop a baseline
  - _name_ - baseline name to drop. This paremeter may be omitted (or be set to _null_), in this case all existing baselines will be dropped. Dropping a baseline does not mean immediate drop of all snapshots, they are just excluded from any baselines. Snapshots, to be deleted based on default retention policy, will be deleted only on next _snapshot()_ call.
- __baseline_keep(name varchar(25), days integer)__ - change retention of baselines
  - _name_ - baseline name. This paremeter may be omitted (or be set to _null_) to change retention of all existing baselines.
  - _days_ - retention time of a baseline in days since _now()_. Also, may be omitted (or set to nul) to set infinite retention.
- __baseline_show()__ - displays existing baselines. Call this function to get information about existing baselines (names, snapshot intervals, and retention times)
```
postgres=# select * from profile.baseline_show();
```
### What you need to remember...
1. PostgreSQL collects execution statistics __after__ execution is complete. If single execution of a statement lasts for several snapshots, it will affect statistics of only last snapshot (in which it was completed).
1. When this extension is in use reset of any PostgreSQL statistics may affect accuracy of a next snapshot. 
## TODO-list
- [X] Some sort of "baselines" - snapshot series, excluded from default retention policy
- [X] Query IDs compatible with pgcenter utility
- [ ] Differential reports
