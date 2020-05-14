# pg_profile module documentation

This extension for PostgreSQL helps you to find out most resource-consuming activities in your PostgreSQL databases.
## Concepts
This extension is based on statistics views of PostgreSQL and contrib extension *pg_stat_statements*. It is written in pure pl/pgsql and doesn't need any external libraries or software, but PostgreSQL database itself, and a cron-like tool performing periodic tasks. Initially developed and tested on PostgreSQL 9.6, and may be incompatible with earlier releases.

Historic repository will be created in your database by this extension. This repository will hold statistics "snapshots" for your postgres clusters. Snapshot is taken by calling _snapshot()_ function. PostgreSQL doesn't have any job-like engine, so you'll need to use *cron*.

Periodic snapshots can help you finding most resource intensive activities in the past. Suppose, you were reported performance degradation several hours ago. Resolving such issue, you can build a report between two snapshots bounding performance issue period to see load profile of your database. It's worse using a monitoring tool such as Zabbix to know exact time when performance issues was happening.

You can take an explicit snapshot before running any batch processing, and after it will be done.

Any time you make a snapshot, _pg_stat_statements_reset()_ will be called, ensuring you will not loose statements due to reaching *pg_stat_statements.max*. Also, report will contain section, informing you if captured statements count in any snapshot reaches 90% of _pg_stat_statements.max_.

*pg_profile*, installed in one cluster is able to collect statistics from other clusters, called *nodes*. You just need to define some nodes, providing names and connection strings and make sure connection can be established to all databases of all nodes. Now you can track statistics on your standbys from master, or from any other node. Once extension is installed, a *local* node is automatically created - this is a node for cluster where *pg_profile* resides.

## Extension architecture

Extension consists of four parts:

* **Historic repository** is a storage for snapshots data. Repository is a set of extension tables.
* **Snapshot management engine** is a set of functions, used for create *snapshots* and support repository by removing obsolete snapshots data from it.
* **Report engine** is a set of functions used for report generation based on data from historic repository.
* **Administrative functions** allows you to create and manage *nodes* and *baselines*.

## Prerequisites
Although pg_profile is usually installed in the target cluster, it also can collect performance data from other clusters. Hence, we have prerequisites for *pg_profile* database, and for *nodes*.

### pg_profile database prerequisites
_pg_profile_ extension depends on extensions _plpgsql_ and _dblink_.
### Nodes prerequisites
The only mandatory  requirement for node cluster is the ability to connect from pg_profile database using provided node connection string. All other requirements are optional, but they can improve completeness of gathered statistics.

Consider setting following Statistics Collector parameters:

```
track_activities = on
track_counts = on
track_io_timing = on
track_functions = all/pl
```

If you need statement statistics in reports, then database, mentioned in node connection string must have _pg_stat_statements_ extension configured. Set *pg_stat_statements* parameters to meet your needs (see PostgreSQL documentation):
* _pg_stat_statements.max_ - low setting for this parameter may cause some statements statistics to be wiped out before snapshot is taken. Report will warn you if your _pg_stat_statements.max_ is seems to be undersized.
* _pg_stat_statements.track = 'top'_ - _all_ value will affect accuracy of _%Total_ fields for statements-related sections of report.
## Installation
### Step 1 Installation of extension files
* Copy files of extension (pg_profile*) to PostgreSQL extensions location, which is
```
# cp pg_profile* `pg_config --sharedir`/extension
```
Just make sure you are using appropriate *pg_config*.

### Step 2 Creating extensions
The most easy way is to install everything in public schema of a database:
```
postgres=# CREATE EXTENSION dblink;
postgres=# CREATE EXTENSION pg_stat_statements;
postgres=# CREATE EXTENSION pg_profile;
```
If you want to install *pg_profile* in other schema, just create it, and install extension in that schema:
```
postgres=# CREATE EXTENSION dblink;
postgres=# CREATE EXTENSION pg_stat_statements;
postgres=# CREATE SCHEMA profile;
postgres=# CREATE EXTENSION pg_profile SCHEMA profile;
```
All objects will be created in schema, defined by SCHEMA clause. Installation in dedicated schema <u>is the recommended way</u> - the extension will create its own tables, views, sequences and functions. It is a good idea to keep them separate. If you do not want to specify schema qualifier when using module, consider changing _search_path_ setting.
### Step 3 Update to new version
New versions of pg_profile will contain all necessary to update from any previous one. So, in case of update you will only need to install extension files (see Step 1) and update the extension, like this:
```
postgres=# ALTER EXTENSION pg_profile UPDATE;
```
All your historic data will remain unchanged if possible.
## Privileges
Using *pg_profile* with superuser privileges does not have any issues, but if you want to avoid using superuser permissions, here is the guide:
### On pg_profile database
Let's create an unprivileged user:
```
create role profile_usr login password 'pwd';
```
Create a schema for pg_profile installation:
```
create schema profile authorization profile_usr;
```
Grant usage permission on schema, where _dblink_ extension resides:
```
grant usage on schema public to profile_usr;
```
Create the extension using *profile_usr* account:
```
postgres=> create extension pg_profile schema profile;
```
### On node database
Create a user for *pg_profile* to connect:
```
create role profile_mon login password 'pwd_mon';
```
Make sure this user have permissions to connect to any database in a cluster (by default, it is), and _pg_hba.conf_ will permit such connection from _pg_profile_ database host. Also, we need *pg_read_all_stats* privilege, and execute privilege on pg_stat_statements_reset:
```
grant pg_read_all_stats to profile_mon;
grant execute on function pg_stat_statements_reset TO profile_mon;
```
### Node setup at pg_profile database
If you install _pg_profile_ on a target database, then you need to update connection string for _local_ node, providing username and password:
```
select node_connstr('local','dbname=postgres port=5432 user=profile_mon password=pwd_mon');
```
Otherwise, you need to create a node for a target cluster, providing appropriate connection string (see *Managing nodes* section below). Also, you may want to disable *local* node.

## Using pg_profile
### Setting extension parameters
You can define extension parameters in _postgresql.conf_. Default values:
* _pg_profile.topn = 20_ - Number of top objects (statements, relations, etc.), to be reported in each sorted report table. Also, this parameter affects size of a snapshot - the more objects you want appear in your report, the more objects we need to keep in a snapshot.
* _pg_profile.retention = 7_ - Retention time of snapshots in days. Snapshots, aged _pg_profile.retention_ days and more will be automatically deleted on next _snapshot()_ call. 
### Managing nodes
Once installed, extension will create one enabled *local* node - this is the cluster, where extension is installed.
Nodes management functions:

- **node_new(node, connstr, node_enabled = true, retention = NULL)**
  Creates a new node with *node* name (must be unique) and connection string. If _enabled_ is set, node will be included in the common _snapshot()_ call. *retention* parameter will override _pg_profile.retention_ setting for this node.

- **node_drop(node)**
  Drops a node and all its snapshots.

- **node_enable(node)**
  Includes node in common snapshot() call.

- **node_disable(node)**
  Excludes node from common snapshot() call.

- **node_rename(node, new_name)**
  Renames a node.

- **node_retention(node, retention)**

  Set new retention period (in days) for a node. *retention* is integer value. To reset a node retention sessing set it to NULL.

- **node_set_db_exclude(node name, exclude_db name[])**

  Set exclude databases list for a node. Used in cases, when you unable to connect to some databases in clester (for example in Amazon RDS instances).

- **node_connstr(node, new_connstr)**
  Set new connection string for a node.

- **node_show()**
Display existing nodes.

Node creation example:

```
SELECT profile.node_new('omega','host=name_or_ip dbname=postgres port=5432');
```

### Snapshots

Snapshots will hold statistic information about database load from previous snapshot.

#### Snapshot functions

Snapshot management functions:

* **snapshot()**

  Function *snapshot()* will create snapshot for all *enabled* nodes. Nodes snapshots will be taken serially one by one. Function returns a table:

  ```
  node        name,
  result      text
  ```

  Where:

  * *node* is a node name
  * *result* is a result of taken snapshot. It can be 'OK' if snapshot was taken successively, and will contain error text in case of exception

  Such return makes it easy to control snapshots creation using SQL query.

* **snapshot(node)**

  Will take snapshot for specified node. Use it when you want to use different snapshots frequencies, or if you want to take snapshots in parallel using several sessions.

* **snapshot_show([node,] days)**

  Returns a table, containing existing snapshots of a *node* (*local* node assumed if *node* is omitted) for *days* last days:

      snapshot		integer,
      snapshot_time 	timestamp (0) with time zone,
      dbstats_reset 	timestamp (0) with time zone,
      clustats_reset 	timestamp (0) with time zone
  Where:

  * *snapshot* is a snapshot identifier
  * *snapshot_time* is a time when this snapshot was taken
  * *dbstats_reset* and *clustats_reset* is usual null, but will contain *pg_stat_database* and *pg_stat_bgwriter* statistics reset timestamp if it was happend from previous snapshot

Snapshot-creating functions are also supports the node repository - it will delete obsolete snapshots and baselines with respect to *retention policy*.

#### Creating snapshots

You must create at least 2 snapshots to be able to build your first report between 1st and 2nd snapshot.
Snapshots for all enabled nodes are taken by calling _snapshot()_ function. There is no need in frequent snapshots creation - usual essential frequency is one or two snapshots per hour. You can use cron-like tool to schedule snapshots creation. Example with 30 min period:

```
*/30 * * * *   psql -c 'SELECT profile.snapshot()' > /dev/null 2>&1
```
However, such call has no error checking on *snapshot()* function results. Consider using more smart usage of *snapshot()* function, providing results to your monitoring system.

Function will return 'OK' for all nodes with successfully  taken snapshots, and show error text for failed nodes:
```
select * from snapshot();
   node   |                                result                                 
----------+-----------------------------------------------------------------------
 ok_node  | OK
 err_node | could not establish connection                                       +
          | SQL statement "SELECT dblink_connect('node_connection',node_connstr)"+
          | PL/pgSQL function snapshot(integer) line 52 at PERFORM               +
          | PL/pgSQL function snapshot() line 22 at assignment                   +
          | FATAL:  database "nodb" does not exist
(2 rows)
```

#### Snapshot retention

We can't store snapshots forever, thus we have a retention policy. You can define retentions on three levels:

* Setting parameter *pg_profile.retention* in postgresql.conf file. This is a common retention, it is effective if none of others is defined.
* Define node *retention* setting while creating a node, or using *node_retention()* function for existing node. This retention overrides common *pg_profile.retention* setting for a specific node.
* Create a baseline (see below). Baseline will override retention period for included snapshots with highest priority.

#### Listing snapshots

Use *snapshot_show()* function to get list of existing snapshots in repository. This function will show detected statistics reset times.

### Baselines

Baseline is a named snapshot sequence, having its own retention setting. Baseline can be used in report-building functions as snapshot interval. Undefined baseline retention means infinite retention.
You can use baselines to save information about database workload on certain time period. For example, you may want to save snapshots, gathered during load testing, or during regular load on your system.
Baseline management functions:

- __baseline_new([node name,] baseline_name varchar(25), start_id integer, end_id integer, days integer)__ - create a baseline

  - _node_ - node name. *local* node assumed if omitted
  - _name_ - baseline name. Each baseline must have unique name within one node.
  - _start_id_, _end_id_ - first and last snapshots, included in baseline.
  - _days_ - baseline retention time. Defined in integer days since _now()_. This parameter may be omitted (or be set to _null_), meaning infinite retention.

- __baseline_drop(node name, name varchar(25))__ - drop a baseline

  - _node_ - node name. *local* node assumed if omitted
  - _name_ - baseline name to drop. Dropping a baseline does not mean immediate drop of all its snapshots, they are just excluded from baseline, thus is not more covered with baseline retention. 

- __baseline_keep([node name,] name varchar(25), days integer)__ - change retention of baselines

  - _node_ - node name. *local* node assumed if omitted
  - _name_ - baseline name. This parameter may be omitted (or be set to _null_) to change retention of all existing baselines.
  - _days_ - retention time of a baseline in days since _now()_. Also, may be omitted (or set to null) to set infinite retention.

- __baseline_show(node name)__ - displays existing baselines. Call this function to get information about existing baselines (names, snapshot intervals, and retention times)

  - _node_ - node name. *local* node assumed if omitted

  ```
  postgres=# SELECT * FROM profile.baseline_show('local');
  ```

### Reports

Reports are generated by reporting functions in HTML markup. There are two types of reports available in *pg_profile*:

* **Regular reports**, containing statistical information about instance workload during report interval
* **Differential reports**, containing data from two intervals with same objects statistic values located one next to other, making it easy to compare the workload

#### Regular report functions

* **report([node,] start_id, end_id [, description])** - generate report by snapshots identifiers

  * *node* is node name. *local* node assumed if omitted
  * *start_id* is interval begin snapshot identifier
  * *end_id* is interval end snapshot identifier
  * *description* is a text memo - it will be included in report as report description

* **report([node,] time_range tstzrange [, description])** - generate report by time range.

  * *node* is node name. *local* node assumed if omitted
  * *time_range* is time range (*tstzrange* type)
  * *description* is a text memo - it will be included in report as report description

  This function will generate report on smallest snapshot interval, covering provided *time_range*.

* **report([node], baseline [, description])** - generate report, using baseline as snapshot interval
  
  * *node* is node name. *local* node assumed if omitted
  * *baseline* is a baseline name
  * *description* is a text memo - it will be included in report as report description

#### Differential report functions

You can generate differential report using snapshot identifiers, baselines and time ranges as interval bounds:

* **report_diff([node,] start1_id, end1_id, start2_id, end2_id [, description])** - generate differential report on two intervals by snapshot identifiers
  * *node* is node name. *local* node assumed if omitted
  * *start1_id*, *end1_id* - snapshot identifiers of first interval
  * *start2_id*, *end2_id* - snapshot identifiers of second interval
  * *description* is a text memo - it will be included in report as report description

* **report_diff([node,] baseline1, baseline2 [, description])** - generate differential report on two intervals, defined by basename names
  * *node* is node name. *local* node assumed if omitted
  * *baseline1* - baseline name of first interval
  * *baseline2* - baseline name of second interval
  * *description* is a text memo - it will be included in report as report description

* **report_diff([node,] time_range1 tstzrange, time_range2 tstzrange [, description])** - generate differential report on two intervals, defined by time ranges
  * *node* is node name. *local* node assumed if omitted
  * *time_range1* - first interval time range
  * *time_range2* - second interval time range
  * *description* is a text memo - it will be included in report as report description

Also, you can use some combinations of the above:

* **report_diff([node,] baseline, time_range [, description])**
* **report_diff([node,] time_range, baseline [, description])**
* **report_diff([node,] start1_id, end1_id, baseline [, description])**
* **report_diff([node,] baseline, start2_id, end2_id [, description])**

Report generation example:
```
$ psql -Aqtc "SELECT profile.report(480,482)" -o report_480_482.html
```
For any other node, use it's name:
```
$ psql -Aqtc "SELECT profile.report('omega',12,14)" -o report_omega_12_14.html
```
Report generation using time ranges:

```
psql -Aqtc "select profile.report(tstzrange('2020-05-13 11:51:35+03','2020-05-13 11:52:18+03'))" -o report_range.html
```

Also, time ranges is useful in generating periodic reports. Let's build last 24-hour report:

```
psql -Aqtc "select profile.report(tstzrange(now() - interval '1 day',now()))" -o report_daily.html
```

Now you can view report file in any web browser.

#### Sections of a report
Report tables and their columns are described in this section.
* Cluster statistics
  * Databases statistics
  
    Contains per-database statistics during report interval, based on *pg_stat_database* view.
  
    * *Database* - database name
    * *Commits* - count of commited transactions (*xact_commit*)
    * *Rollbacks* - count of rolled back transactions (*xact_rollback*)
    * *Deadlocks* - count of deadlocks detected (*deadlocks*)
    * *BlkHit%(read/hit)* - percentage of pages got from buffers within all pages got. Total count of read and hit pages are in parenthesis. (*blks_read* and *blks_hit*)
    * *Tup Ret/Fet* - returned and fetched tuples count (*tup_returned* and *tup_fetched*)
    * *Tup Ins* - inserted tuples count (*tup_inserted*)
    * *Tup Upd* - updated tuples count (*tup_updated*)
    * *Tup Del* - deleted tuples count (*tup_deleted*)
    * *Temp Size(Files)* - amount of data written to temporary files and number of temporary files (*temp_bytes* and *temp_files*)
    * *Size* - database size in the end of report interval (pg_database_size())
    * *Growth* - database growth during report interval (pg_database_size())
  
  * Statements statistics by database
  
    Contains per-database aggregated total statistics of *pg_stat_statements* data (if during interval *pg_stat_statements* extension was available)
  
    * *Database* - database name
    * *Calls* - total count of all statements executions (sum of *calls*)
    * *Total time(s)* - total time of all statements (sum of *total_time*)
    * *Shared gets* - total processed shared pages count (sum of *shared_blks_read + shared_blks_hit*)
    * *Local gets* - total processed local pages count (sum of *local_blks_read + local_blks_hit*)
    * *Shared dirtied* - total changed shared pages count (sum of *shared_blks_dirtied*)
    * *Local dirtied* - total changed local pages count (sum of *local_blks_dirtied*)
    * *Work_r (blk)*, *Work_w (blk)* - total count of read and written pages for joins and sorts (sums of *temp_blks_read* and *temp_blks_written*)
    * *Local_r (blk)* - total count of read temporary pages, used in temporary tables (sum of *local_blks_read*)
    * *Local_w (blk)* - total count of written temporary pages, used in temporary tables (sum of *local_blks_written*)
    * *Statements* - total count of captured statements
    
  * Cluster statistics
  
    This table contains data from *pg_stat_bgwriter* view
  
    * *Scheduled checkpoints* - total count of checkpoints, completed on schedule due to *checkpoint_timeout* parameter (*checkpoints_timed* field)
    * *Requested checkpoints* - total count of other checkpoints: due to values of *max_wal_size*, *archive_timeout* and CHECKPOINT commands (*checkpoints_req* field)
    * *Checkpoint write time (s)* - total time spent in writing checkpoints in seconds (*checkpoint_write_time* field)
    * *Checkpoint sync time (s)* - total time spent in syncing checkpoints in seconds (*checkpoint_sync_time* field)
    * *Checkpoints pages written* - total number of buffers, written by checkpointer (*buffers_checkpoint* field)
    * *Background pages written* - total count of buffers, written by background writer process (*buffers_clean* field)
    * *Backend pages written* - total count of buffers, written by backends (*buffers_backend* field)
    * *Backend fsync count* - total count of backend fsync calls (*buffers_backend_fsync* field)
    * *Bgwriter interrupts (too many buffers)* - total count of background writer interrupts due to reaching value of the *bgwriter_lru_maxpages* parameter.
    * *Number of buffers allocated* - total count of buffers allocated
    * *WAL generated* - total amount of WAL generated
  
  * Tablespaces statistics
  
    This table contains information about tablespaces sizes:
  
    * *Tablespace* - tablespace name
    * *Path* - tablespace path
    * *Size* - tablespace size as it was at time of last snapshot in report interval
    * *Growth* - tablespace growth during report interval
  
* SQL Query statistics

  This report section contains tables of top statements during report interval sorted by several important statistics. Data captured from *pg_stat_statements* view if it was available at time of snapshots.

  * Top SQL by elapsed time

    This table contains top _pg_profile.topn_ statements sorted by *total_time* field of *pg_stat_statements* view

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this query, in seconds (*total_time* field)
    * *%Total* - *total_time* of this statement as a percentage of total time of all statements in a cluster
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Mean(ms)* - mean time spent in the statement, in milliseconds (*mean_time* field)
    * *Min(ms)* - minimum time spent in the statement, in milliseconds (*min_time* field)
    * *Max(ms)* - maximum time spent in the statement, in milliseconds (*max_time* field)
    * *StdErr(ms)* - population standard deviation of time spent in the statement, in milliseconds (*stddev_time* field)
    * *Executions* - count of statement executions (*calls* field)

  * Top SQL by executions

    Top _pg_profile.topn_ statements sorted by *total_time* field of *pg_stat_statements* view

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.

    * *Database* - Statement database name (derived from *dbid* field)

    * *Executions* - count of statement executions (*calls* field)

    * *%Total* - *calls* of this statement as a percentage of total *calls* of all statements in a cluster

    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)

    * *Mean(ms)* - mean time spent in the statement, in milliseconds (*mean_time* field)

    * *Min(ms)* - minimum time spent in the statement, in milliseconds (*min_time* field)

    * *Max(ms)* - maximum time spent in the statement, in milliseconds (*max_time* field)

    * *StdErr(ms)* - population standard deviation of time spent in the statement, in milliseconds (*stddev_time* field)

    * *Elapsed(s)* - amount of time spent executing this query, in seconds (*total_time* field)

  * Top SQL by I/O wait time

    Top _pg_profile.topn_ statements sorted by read and write time (*blk_read_time* + *blk_write_time*)

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *IO(s)* - amount of time spent on reading and writing (I/O time) by this statement in seconds (*blk_read_time* + *blk_write_time*)
    * *R(s)* - amount of time spent on reading by this statement in seconds (*blk_read_time*)
    * *W(s)* - amount of time spent on writing by this statement in seconds (*blk_write_time*)
    * *%Total* - I/O time of this statement as a percentage of total I/O time for all statements in a cluster
    * *Reads* - number of pages read by this statement divided in three sub-columns: *Shr* - shared reads (*shared_blks_read* field), *Loc* - local reads (*local_blks_read* field) and *Tmp* - temp reads (*temp_blks_read* field)
    * *Writes* - number of pages written by this statement divided in three sub-columns: *Shr* - shared writes (*shared_blks_written* field), *Loc* - local writes (*local_blks_written* field) and *Tmp* - temp writes (*temp_blks_written* field)
    * *Executions* - number of executions for this statement (*calls* field)

  * Top SQL by gets

    Top _pg_profile.topn_ statements sorted by read and hit pages, helping to detect the most data processing statements.

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Gets* - number of retrieved pages (expression: *shared_blks_hit* + *shared_blks_read*)
    * *%Total* - *gets* of this statement as a percentage of total *gets* for all statements in a cluster
    * *Hits(%)* - percentage of pages got from buffers within all pages got
    * *Executions* - number of executions for this statement (*calls* field)

  * Top SQL by shared reads

    Top _pg_profile.topn_ statements sorted by shared reads, helping to detect most read intensive statements.

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Reads* - number of shared read pages for this statement (*shared_blks_read* field)
    * *%Total* - shared reads for this statement as a percentage of total shared reads for all statements in a cluster
    * *Hits(%)* - percentage of pages got from buffers within all pages got
    * *Executions* - number of executions for this statement (*calls* field)

  * Top SQL by shared dirtied

    Top _pg_profile.topn_ statements sorted by shared dirtied buffer count, helping to detect most data changing statements.

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Dirtied* - number of shared dirtied buffers for this statement (*shared_blks_dirtied* field)
    * *%Total* - shared dirtied buffers for this statement as a percentage of total shared dirtied buffers for all statements in a cluster
    * *Hits(%)* - percentage of pages got from buffers within all pages got
    * *Executions* - number of executions for this statement (*calls* field)

  * Top SQL by shared written

    Top _pg_profile.topn_ statements, which had to perform writes sorted by written pages count.

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Written* - number of buffers written by this statement (*shared_blks_written* field)
    * *%Total* - number of buffers written by this statement as a percentage of total buffers written by all statements in a cluster
    * *%BackendW* - number of buffers written by this statement as a percentage of all buffers written in a cluster by backends (*buffers_backend* field of *pg_stat_bgwriter* view)
    * *Hits(%)* - percentage of pages got from buffers within all pages got
    * *Executions* - number of executions for this statement (*calls* field)

  * Top SQL by temp usage

    Top _pg_profile.topn_ statements sorted by temp I/O, calculated as the sum of *temp_blks_read*, *temp_blks_written*, *local_blks_read* and *local_blks_written* fields

    * *Query ID* - Query identifier as a hash of database, user and query text. Compatible with *pgcenter* utility.
    * *Database* - Statement database name (derived from *dbid* field)
    * *Elapsed(s)* - amount of time spent executing this statement, in seconds (*total_time* field)
    * *Rows* - number of rows retrieved or affected by the statement (*rows* field)
    * *Gets* - number of retrieved pages (expression: *shared_blks_hit* + *shared_blks_read*)
    * *Hits(%)* - percentage of pages got from buffers within all pages got
    * *Work_w(blk)* - number of written pages used as work memory (*temp_blks_written* field)
    * *%Total* - *temp_blks_written* of this statement as a percentage of total *temp_blks_written* for all statements in a cluster
    * *Work_r(blk)* - number of read pages used as work memory (*temp_blks_read* field)
    * *%Total* - *temp_blks_read* of this statement as a percentage of total *temp_blks_read* for all statements in a cluster
    * *Local_w(blk)* - number of written pages used in temporary tables (*local_blks_written* field)
    * *%Total* - *local_blks_written* of this statement as a percentage of *local_blks_written* for all statements in a cluster
    * *Local_r(blk)* - number of read pages used in temporary tables (*local_blks_read* field)
    * *%Total* - *local_blks_read* of this statement as a percentage of *local_blks_read* for all statements in a cluster
    * *Executions* - number of executions for this statement (*calls* field)

  * Complete List of SQL Text

    Query texts of all statements mentioned in report. You can use *Query ID* link in any statistic table to get there and see query text.

* Schema objects statistics
  
  This section of report contains top database objects, using statistics from Statistics Collector views.
  
  * Most seq. scanned tables
  
    Top database tables sorted by approximately count of pages, read by sequential scans. Based on *pg_stat_all_tables* view. Here you can search for tables, possibly lacks some index on it.
  
    * *DB* - database name of the table
    * *Tablespace* - tablespace name, where the table is located
    * *Schema* - schema name of the table
    * *Table* - table name
    * *SeqScan* - number of sequential scans performed on the table (*seq_scan* field)
    * *SeqPages* - number of pages, read by sequential scans. This column is estimation - it is calculated as a sum of *pg_relation_size()* * *seq_scan* for each snapshot.
    * *IxScan* - number of index scans initiated on this table (*idx_scan* field)
    * *IxFet* - number of live rows fetched by index scans (*idx_tup_fetch* field)
    * *Ins* - number of rows inserted (*n_tup_ins* field)
    * *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
    * *Del* - number of rows deleted (*n_tup_del* field)
    * *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)
  
  * Top tables by gets
  
    Top tables sorted by pages got. *Get* is relation page, being processed from disk (read), or from shared buffers (hit). Tables in this list are sorted by sum of page gets for table relation, its indexes, TOAST of a table (if exists), and TOAST index (if exists). This section can focus your attention on tables with excessive page processing. Based on data of *pg_statio_all_tables* view.
  
    * *DB* - database name of the table
  
    * *Tablespace* - tablespace name, where the table is located
  
    * *Schema* - schema name of the table
  
    * *Table* - table name
  
    * *Heap* - statistics for relation gets (*heap_blks_read* + *heap_blks_hit*)
  
    * *Ix* - statistics for all relation indexes gets (*idx_blks_read* + *idx_blks_hit*)
  
    * *TOAST* - statistics for TOAST-table gets (*toast_blks_read* + *toast_blks_hit*)
  
    * *TOAST-Ix* - statistics for TOAST index pages (*tidx_blks_read* + *tidx_blks_hit*)
  
      Here is a bug - before PostgreSQL 13 column *TOAST-Ix* can have incorrectly multiplied values. Use section "Top indexes by gets" for recheck. Fixed with this commit: https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=ef11051bbe96ea2d06583e4b3b9daaa02657dd42
  
    Each gets statistic in this table is divided in two columns:
  
    * *Pages* - number of page gets for relation heap, index, TOAST or TOAST index
    * *%Total* - page gets for relation heap, index, TOAST or TOAST index as a percentage of all gets in a whole cluster
  
  * Top DML tables
  
    Top tables sorted by amount of DML-affected rows, i.e. sum of *n_tup_ins*, *n_tup_upd* and *n_tup_del*  (including TOAST tables).
  
    * *DB* - database name of the table
    * *Tablespace* - tablespace name, where the table is located
    * *Schema* - schema name of the table
    * *Table* - table name
    * *Ins* - number of rows inserted (*n_tup_ins* field)
    * *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
    * *Del* - number of rows deleted (*n_tup_del* field)
    * *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)
    * *SeqScan* - number of sequential scans performed on the table (*seq_scan* field)
    * *SeqFet* - number of live rows fetched by sequential scans (*seq_tup_read* field)
    * *IxScan* - number of index scans initiated on this table (*idx_scan* field)
    * *IxFet* - number of live rows fetched by index scans (*idx_tup_fetch* field)
  
  * Top Delete/Update tables with vacuum run count
  
    Top tables sorted by amount of operations, causing autovacuum load, i.e. sum of *n_tup_upd* and *n_tup_del* (including TOAST tables). Consider fine-tune of vacuum-related parameters based on provided vacuum and analyze run statistics.
  
    * *DB* - database name of the table
    * *Tablespace* - tablespace name, where the table is located
    * *Schema* - schema name of the table
    * *Table* - table name
    * *Ins* - number of rows inserted (*n_tup_ins* field)
    * *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
    * *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)
    * *Del* - number of rows deleted (*n_tup_del* field)
    * *Vacuum* - number of times this table has been manually vacuumed (not counting VACUUM FULL) (*vacuum_count* field)
    * *AutoVacuum* - number of times this table has been vacuumed by the autovacuum daemon (*autovacuum_count* field)
    * *Analyze* - number of times this table has been manually analyzed (*analyze_count* field)
    * *AutoAnalyze* - number of times this table has been analyzed by the autovacuum daemon (*autoanalyze_count* field)
  
  * Top growing tables
  
    Top tables sorted by growth
  
    * *DB* - database name of the table
    * *Tablespace* - tablespace name, where the table is located
    * *Schema* - schema name of the table
    * *Table* - table name
    * *Size* - table size, as it was at the moment of last snapshot in report interval
    * *Growth* - table growth
    * *Ins* - number of rows inserted (*n_tup_ins* field)
    * *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
    * *Del* - number of rows deleted (*n_tup_del* field)
    * *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)
  
  * Top indexes by gets
  
    Top indexes sorted by pages got. *Get* is index page, being processed from disk (read), or from shared buffers (hit). Based on data of *pg_statio_all_indexes* view.
  
    * *DB* - database name of the index
    * *Tablespace* - tablespace name, where the index is located
    * *Schema* - schema name of the index
    * *Table* - table name
    * *Index* - index name
    * *Pages* - pages got from this index (*idx_blks_read* + *idx_blks_hit*)
    * *%Total* - *gets* for this index as a percentage of all *gets* in a whole cluster
  
  * Top growing indexes
  
    Top indexes sorted by growth
  
    * *DB* - database name of the index
    * *Tablespace* - tablespace name, where the index is located
    * *Schema* - schema name of the index
    * *Table* - table name
    * *Index* - index name
    * *Index Size* - index size, as it was at the moment of last snapshot in report interval
    * *Index Growth* - index growth
    * *Table Ins* - number of rows inserted to underlying table (*n_tup_ins* field)
    * *Table Upd* - number of rows updated in underlying table (with HOT) (*n_tup_upd* field)
    * *Table Del* - number of rows deleted from underlying table (*n_tup_del* field)
  
  * Unused indexes
  
    Non-scanned indexes during report interval sorted by DML operations on underlying tables, causing index support. Constraint indexes are excluded.
  
    * *DB* - database name of the index
    * *Tablespace* - tablespace name, where the index is located
    * *Schema* - schema name of the index
    * *Table* - table name
    * *Index* - index name
    * *Index Size* - index size, as it was at the moment of last snapshot in report interval
    * *Index Growth* - index growth
    * *Table Ins* - number of rows inserted into underlying table (*n_tup_ins* field)
    * *Table Upd* - number of rows updated in underlying table (without HOT) (*n_tup_upd* - *n_tup_hot_upd*)
    * *Table Del* - number of rows deleted from underlying table (*n_tup_del* field)
  
* I/O Schema objects stats
  
  This section of report contains top I/O-related database objects, using statistics from Statistics Collector views.
  
  * Top tables by read I/O
  
    Top tables sorted by page reads. Tables in this list are sorted by sum of page reads for table relation, its indexes, TOAST of a table (if exists), and TOAST index (if exists). This section can focus your attention on tables with excessive pages reading. Based on data of *pg_statio_all_tables* view.
  
    * *DB* - database name of the table
  
    * *Tablespace* - tablespace name, where the table is located
  
    * *Schema* - schema name of the table
  
    * *Table* - table name
  
    * *Heap* - statistics for relation page reads (*heap_blks_read*)
  
    * *Ix* - statistics for all relation indexes page reads (*idx_blks_read*)
  
    * *TOAST* - statistics for TOAST-table page reads (*toast_blks_read*)
  
    * *TOAST-Ix* - statistics for TOAST index page reads (*tidx_blks_read*)
  
      Here is a bug - before PostgreSQL 13 this field can have incorrectly multiplied values. Use section "Top indexes by gets" for recheck. Fixed with this commit: https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=ef11051bbe96ea2d06583e4b3b9daaa02657dd42
  
    Each gets statistic in this table is divided in two columns:
  
    * *Pages* - number of page reads for relation heap, index, TOAST or TOAST index
    * *%Total* - page reads for relation heap, index, TOAST or TOAST index as a percentage of all page reads in a whole cluster
  
  * Top indexes by read I/O
  
    Top indexes sorted by page reads. Based on data of *pg_statio_all_indexes* view.
  
    * *DB* - database name of the index
    * *Tablespace* - tablespace name, where the index is located
    * *Schema* - schema name of the index
    * *Table* - table name
    * *Index* - index name
    * *Pages* - page reads from this index (*idx_blks_read*)
    * *%Total* - page reads for this index as a percentage of all page reads in a whole cluster
  
* User function statistics
  
  This report section contains top functions in cluster, based on *pg_stat_user_functions* view.
  
  * Top functions by total time
  
    Top functions sorted by time elapsed.
  
    * *DB* - database name of the function
    * *Schema* - schema name of the index
    * *Function* - function name
    * *Executions* - number of times this function has been called (*calls* field)
    * *Total time* - total time spent in this function and all other functions called by it, in milliseconds (*total_time* field)
    * *Self time* - total time spent in this function itself, not including other functions called by it, in milliseconds (*self_time* field)
    * *Mean time* - mean time of single function execution
    * *Mean self time* - mean self time of single function execution
  
  * Top functions by executions
  
    Top functions sorted by time elapsed.
  
    * *DB* - database name of the function
    * *Schema* - schema name of the index
    * *Function* - function name
    * *Executions* - number of times this function has been called (*calls* field)
    * *Total time* - total time spent in this function and all other functions called by it, in milliseconds (*total_time* field)
    * *Self time* - total time spent in this function itself, not including other functions called by it, in milliseconds (*self_time* field)
    * *Mean time* - mean time of single function execution
    * *Mean self time* - mean self time of single function execution
  
* Vacuum related stats
  
  This section contains modified tables with last vacuum run. Statistics is valid for last snapshot in report interval. Based on *pg_stat_all_tables* view.
  
  * Tables ordered by dead tuples ratio
  
    Top tables, bigger than 5 MB in size sorted by dead tuples ratio.
  
    * *DB* - database name of the table
    * *Schema* - schema name of the table
    * *Table* - table name
    * *Live* - estimated number of live rows (*n_live_tup*)
    * *Dead* - estimated number of dead rows (n_dead_tup)
    * *%Dead* - dead rows of the table as a percentage of all rows in the table
    * *Last AV* - last time at which this table was vacuumed by the autovacuum daemon (*last_autovacuum*)
    * *Size* - table size
  
  * Tables ordered by modified tuples ratio
  
    Top tables, bigger than 5 MB in size sorted by modified tuples ratio.
  
    * *DB* - database name of the table
    * *Schema* - schema name of the table
    * *Table* - table name
    * *Live* - estimated number of live rows (*n_live_tup*)
    * *Dead* - estimated number of dead rows (n_dead_tup)
    * *Mods* - estimated number of rows modified since this table was last analyzed (*n_mod_since_analyze*)
    * *%Mod* - modified rows of the table as a percentage of all rows in the table
    * *Last AA* - last time at which this table was analyzed by the autovacuum daemon
    * *Size* - table size
  
* Cluster settings during report interval

  This section of a report contains PostgreSQL GUC parameters, and values of functions *version()*, *pg_postmaster_start_time()*, *pg_conf_load_time()* and field *system_identifier* of *pg_control_system()* function during report interval.

  * *Setting* - name of a parameter
  * *reset_val* - reset_val field of *pg_settings* view. Bold font is used to show settings, changed during report interval.
  * *Unit* - setting unit
  * *Source*  - configuration file, where this setting defined, line number after semicolon.
  * *Notes* - contains 'def' value if setting has a default value. Also this field will contain timestamp of snapshot, when this value was observed first time during report interval.

### What you need to remember...
1. PostgreSQL collects execution statistics __after__ execution is complete. If single execution of a statement lasts for several snapshots, it will affect statistics of only last snapshot (in which it was completed). And you can't get statistics on still running statements. Also, maintenance processes like vacuum and checkpointer will update statistics only on completion.
1. Resetting any PostgreSQL statistics may affect accuracy of a next snapshot.
1. Exclusive locks on relations conflicts with calculating relation size. So, long transactions, performing DDL on tables will "hang" snapshot operation until end of a transaction. _lock_timeout_ is set to 3s, so if _snapshot()_ function will be unable to acquire a lock for 3 seconds, it will fail, and no snapshot will be generated.
