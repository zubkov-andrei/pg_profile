# pg_profile module documentation

This extension for PostgreSQL helps you to find out most resource-consuming activities in your PostgreSQL databases.
## Concepts
Extension is based on statistic views of PostgreSQL and several contrib extensions. It is written in pure pl/pgsql and doesn't need any external libraries or software, but PostgreSQL database itself, and a cron-like tool performing periodic tasks. Initially developed and tested on PostgreSQL 9.6, extension may be incompatible with earlier releases.

*pg_profile* will use *pg_stat_kcache* data, providing information about statements CPU usage and filesystem load (if this extension is available).

Historic repository will be created in your database by this extension. This repository will hold statistic "samples" for your postgres clusters. Sample is taken by calling _take_sample()_ function. PostgreSQL doesn't have any job-like engine, so you'll need to use *cron*.

Periodic samples can help you finding most resource intensive activities in the past. Suppose, you were reported performance degradation several hours ago. Resolving such issue, you can build a report between two samples bounding performance issue period to see load profile of your database. It's worth using a monitoring tool such as Zabbix to know exact time when performance issues was happening.

You can take an explicit sample before running any batch processing, and after it will be done.

Any time you are taking a sample, _pg_stat_statements_reset()_ will be called, ensuring you wont loose statements due to reaching *pg_stat_statements.max*. Also, report will contain section, informing you if captured statements count in any sample reaches 90% of _pg_stat_statements.max_. Resetting of _pg_stat_statements_ data will affect monitoring systems.

If *pg_wait_sampling* extension is available, _pg_profile_ will collect summary wait statistics of the PostgresSQL instance and reset the *pg_wait_sampling_profile* view data. This behaviour can also affect monitoring systems.

*pg_profile*, installed in one cluster is able to collect statistics from other clusters, called *servers*. You just need to define some servers, providing names and connection strings and make sure connection can be established to all databases of all *servers*. Now you can track, for example, statistics on your standbys from master, or from any other server. Once extension is installed, a *local* server is automatically created - this is a server for cluster where *pg_profile* resides.

## Extension architecture

Extension consists of four parts:

* **Historic repository** is a storage for sampling data. Repository is a set of extension tables.
* **Sample management engine** is a set of functions, used for taking *samples* and support repository by removing obsolete sample data from it.
* **Report engine** is a set of functions used for report generation based on data from historic repository.
* **Administrative functions** allows you to create and manage *servers* and *baselines*.

## Prerequisites
Although *pg_profile* is usually installed in the target cluster, it also can collect performance data from other clusters. Hence, we have prerequisites for *pg_profile* database, and for *servers*.

### pg_profile database prerequisites
_pg_profile_ extension depends on extensions _plpgsql_ and _dblink_.
### Servers prerequisites
The only mandatory  requirement for server cluster is the ability to connect from pg_profile database using provided server connection string. All other requirements are optional, but they can improve completeness of gathered statistics.

Consider setting following Statistics Collector parameters:

```
track_activities = on
track_counts = on
track_io_timing = on
track_wal_io_timing = on      # Since Postgres 14
track_functions = all/pl
```

If you need statement statistics in reports, then database, mentioned in server connection string must have _pg_stat_statements_ extension configured. Set *pg_stat_statements* parameters to meet your needs (see PostgreSQL documentation):
* _pg_stat_statements.max_ - low setting for this parameter may cause some statements statistics to be wiped out before sample is taken. Report will warn you if your _pg_stat_statements.max_ is seems to be undersized.
* _pg_stat_statements.track = 'top'_ - before Postgres 14 _all_ value will affect accuracy of _%Total_ fields for statements-related sections of a report.
## Installation
### Step 1 Installation of extension files
Extract extension files (see [Releases](https://github.com/zubkov-andrei/pg_profile/releases) page) to PostgreSQL extensions location, which is

```
# tar xzf pg_profile-<version>.tar.gz --directory $(pg_config --sharedir)/extension
```
Just make sure you are using appropriate *pg_config* utility.

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
All objects will be created in schema, defined by SCHEMA clause. Installation in dedicated schema <u>is the recommended way</u> - the extension will create its own tables, views, sequences and functions. It is a good idea to keep them separate. If you don't want to specify schema qualifier when using module, consider changing _search_path_ setting.
### Step 3 Update to new version
New versions of *pg_profile* will contain migration script (when possible). So, in case of update you will only need to install extension files (see Step 1) and update the extension, like this:
```
postgres=# ALTER EXTENSION pg_profile UPDATE;
```
## Privileges
Using *pg_profile* with a superuser privileges does not have any issues, but if you want to avoid using superuser permissions, here is the guide:
### On pg_profile database
Create an unprivileged user:
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
### On server database
Create a user for *pg_profile* to connect:
```
create role profile_mon login password 'pwd_mon';
```
Make sure this user have permissions to connect to any database in a cluster (by default, it is), and _pg_hba.conf_ will permit such connection from _pg_profile_ database host. Also, we need *pg_read_all_stats* privilege, and execute privilege on pg_stat_statements_reset:
```
grant pg_read_all_stats to profile_mon;
grant execute on function pg_stat_statements_reset TO profile_mon;
```
### Server setup at pg_profile database
Non-superusers can't establish connection without a password. Best way to provide password is using a password file.

N.B. pg_profile will connect to all databases on a server, thus password file must use a wildcard as a database name.

As an insecure way, you can provide a password in connection string:
```
select set_server_connstr('local','dbname=postgres port=5432 user=profile_mon password=pwd_mon');
```

## Using pg_profile
### Setting extension parameters
You can define extension parameters in _postgresql.conf_. Default values:
* _pg_profile.topn = 20_ - Number of top objects (statements, relations, etc.), to be reported in each sorted report table. Also, this parameter affects size of a sample - the more objects you want to appear in your report, the more objects we need to keep in a sample.
* _pg_profile.max_sample_age = 7_ - Retention time of samples in days. Samples, aged _pg_profile.max_sample_age_ days and more will be automatically deleted on next _take_sample()_ call.
* _pg_profile.track_sample_timings = off_ - when this parameter is on, _pg_profile_ will track detailed sample taking timings.
* _pg_profile.max_query_length = 20000_ - query length limit for reports. All queries in a report will be truncated to this length. This setting does not affect query text collection - during a sample full query texts are collected, thus can be obtained.
### Managing servers
Once installed, extension will create one enabled *local* server - this is for cluster, where extension is installed.

Servers management functions:

* **create_server(server name, server_connstr text, server_enabled boolean = TRUE, max_sample_age integer = NULL, description text = NULL)** - creates a new server description
Function arguments:
  * *server* - server name (must be unique)
  * *server_connstr* - server connection string
  * *enabled* - server enable flag. When is set, server will be included in the common _take_sample()_ call
  * *max_sample_age* - server sample retention parameter overrides global _pg_profile.max_sample_age_ setting for this server
  * *description* - server description text. Will be included in reports

* **drop_server(server name)**
  Drops a server and all its samples.

* **enable_server(server name)**
  Includes server in common take_sample() call.

* **disable_server(server name)**
  Excludes server from common take_sample() call.

* **rename_server(server name, new_name name)**
  Renames a server.

* **set_server_max_sample_age(server name, max_sample_age integer)**
  Set new retention period (in days) for a server. *max_sample_age* is integer value. To reset a server *max_sample_age* setting set it to NULL.

* **set_server_db_exclude(server name, exclude_db name[])**
  Set exclude databases list for a server. Used in cases, when you unable to connect to some databases in cluster (for example in Amazon RDS instances).

* **set_server_connstr(server name, new_connstr text)**
  Set new connection string for a server.

* **set_server_description(server name, description text)**
  Set new server description.

* **show_servers()**
Display existing servers.

Server creation example:

```
SELECT profile.create_server('omega','host=name_or_ip dbname=postgres port=5432');
```
### Rare relation sizes collection
Postgres relation size functions may take considerable amount of time to collect sizes of all relations in a database. Also those functions require *AccessExclusiveLock* on a relation. However daily relation sizes collection may be quite enough for you. *pg_profile* is able to skip relation sizes collection while taking samples guided by server _size collection policy_. Policy is defined as a daily window when relation size collection is permitted, and a minimal gap between two samples with relation sizes collected. Thus when size collection policy is defined sample taking function will collect relation sizes only when sample is taken in a window and the previous sample with sizes is older then gap. Top growing tables/indexes report sections will be available in a report only if it bounded by samples with sizes collected. See _with_growth_ parameter of a _get_report_ function description for further reference.
Function *set_server_size_sampling* defines the _size collection policy_:
* *set_server_size_sampling(server name, window_start time with time zone = NULL, window_duration interval hour to second = NULL, sample_interval interval day to minute = NULL)*
  * *server* - server name
  * *window_start* - size collection window start time
  * *window_duration* - size collection window duration
  * *sample_interval* - minimum time gap between two samples with relation sizes collected

Example:
```
SELECT set_server_size_sampling('local','23:00+03',interval '2 hour',interval '8 hour');
```

Function *show_servers_size_sampling* show defined sizes collection policy for all servers:
```
postgres=# SELECT * FROM show_servers_size_sampling();
 server_name | window_start | window_end  | window_duration | sample_interval
-------------+--------------+-------------+-----------------+-----------------
 local       | 23:00:00+03  | 01:00:00+03 | 02:00:00        | 08:00:00
```

When you build a report between samples either of which lacks relation sizes data then relation growth sections of a report will be based on *pg_class.relpages* data. However, *with_growth* parameter of report generation functions will expand report bounds to nearest samples with relation sizes data collected and growth data will be more accurate.

### Samples

Every sample contains statistic information about database workload since previous sample.

#### Sample functions

* **take_sample()**
  Function *take_sample()* will collect a sample for all *enabled* servers. Server samples will be taken serially one by one. Function returns a table:
  ```
  server      name,
  result      text,
  elapsed     interval
  ```
  Where:
  * *server* is a server name
  * *result* is a result of taken sample. It can be 'OK' if sample was taken successively, and will contain error text in case of exception
  * *elapsed* is a time elapsed taking a sample for *server*
  Such return makes it easy to control samples creation using SQL query.
* **take_sample_subset([sets_cnt integer], [current_set integer])**
  Due to serial samples processing *take_sample()* function can take considerable amount of time. Function *take_sample_subset()* will take samples for a subset of enabled servers. It is convenient for parallel samples collection. *sets_cnt* is number of servers subsets. *current_set* is a subset to process, taking values between 0 and *sets_cnt - 1* inclusive.
  Function returns a table:
  ```
  server      name,
  result      text,
  elapsed     interval
  ```
  Where:
  * *server* is a server name
  * *result* is a result of taken sample. It can be 'OK' if sample was taken successively, and will contain error text in case of exception
  * *elapsed* is a time elapsed taking a sample for *server*
* **take_sample(server name [, skip_sizes boolean])**
  Will collect a sample for specified server. Use it, for example, when you want to use different sampling frequencies, or if you want to take explicit sample on a server.
  * *server* - name of a server to take sample
  * *skip_sizes* - override server relation size collection policy. Policy applies only when *skip_size* argument is omitted or set to null. *false* value of *skip_sizes* argument cause take sample with relation sizes, while *true* will cause skipping relation sizes collection during a sample.
* **show_samples([server name,] [days integer])**
  Returns a table, containing existing samples of a *server* (*local* server assumed if *server* is omitted) for *days* last days (all existing samples if omitted):
  ```
  sample            integer,
  sample_time       timestamp (0) with time zone,
  sizes_collected   boolean,
  dbstats_reset     timestamp (0) with time zone,
  clustats_reset    timestamp (0) with time zone,
  archstats_reset   timestamp (0) with time zone
  ```
  Where:
  * *sample* is a sample identifier
  * *sample_time* is a time when this sample was taken
  * *sizes_collected* is set if all relation sizes was collectid in this sample
  * *dbstats_reset*, *clustats_reset* and *archstats_reset* is usual null, but will contain *pg_stat_database*, *pg_stat_bgwriter* and *pg_stat_archiver* statistics reset timestamp if it was happend since previous sample
Sample-collecting functions are also supports the server repository - it will delete obsolete samples and baselines with respect to *retention policy*.
* **delete_samples([server_name name] [, start_id integer, end_id integer])**
  Function *delete_samples()* can be used to manually delete samples of specified server.
  * *server_name* - name of a server to delete samples. Server named 'local' is used if omitted
  * *start_id*, *end_id* - bounding identifiers of sample interval to delete (inclusive). This function will delete all samples of a server if bounding identifier is omitted
* **delete_samples(server_name name, time_range tstzrange)**
  You can use this function to delete all samples contained in specified time range interval
  * *server_name* - name of a server to delete samples.
  * *time_range* - time range of samples to delete

#### Taking samples

You must create at least 2 samples to be able to build your first report between 1st and 2nd samples.
Samples for all enabled servers are taken by calling _take_sample()_ function. There is no need in frequent samples creation - usual essential frequency is one or two samples per hour. You can use cron-like tool to schedule samples creation. Example with 30 min period:

```
*/30 * * * *   psql -c 'SELECT profile.take_sample()' > /dev/null 2>&1
```
However, such call has no error checking on *take_sample()* function results. Consider using more smart usage of *take_sample()* function, providing results to your monitoring system.

Function will return 'OK' for all servers with successfully taken samples, and show error text for failed servers:
```
select * from take_sample();
  server   |                                   result                                    |   elapsed
-----------+-----------------------------------------------------------------------------+-------------
 ok_node   | OK                                                                          | 00:00:00.48
 fail_node | could not establish connection                                             +| 00:00:00
           | SQL statement "SELECT dblink_connect('server_connection',server_connstr)"  +|
           | PL/pgSQL function take_sample(integer) line 69 at PERFORM                  +|
           | PL/pgSQL function take_sample_subset(integer,integer) line 27 at assignment+|
           | SQL function "take_sample" statement 1                                     +|
           | FATAL:  database "nodb" does not exist                                      |
(2 rows)
```

#### Sample data retention

We can't store sample data forever, thus we have a retention policy. You can define retentions on three levels:

* Setting parameter *pg_profile.max_sample_age* in postgresql.conf file. This is a common retention, it is effective if none of others is defined.
* Define server *max_sample_age* setting while creating a server, or using *set_server_max_sample_age()* function for existing server. This setting overrides global *pg_profile.max_sample_age* setting for a specific server.
* Create a baseline (see below). Baseline will override retention period for included samples with highest priority.

#### Listing samples

Use *show_samples()* function to get list of existing samples in the repository. This function will show detected statistics reset times.

#### Sample taking timings
_pg_profile_ will collect detailed sample taking timing statistics when parameter _pg_profile.track_sample_timings_ is on. Results can be obtained from _v_sample_timings_ view. _v_sample_timings_ fields description:
* _server_name_ - sampled server name
* _sample_id_ - sample identifier
* _sample_time_ - when the sample was taken
* _event_ - sample taking stage
* _time_spent_ - amount of time spent in the event

Event descriptions:
* **total** - Taking the sample (all stages).
* **connect** -  Making dblink connection to the server.
* **get server environment** - Getting server GUC parameters, available extensions, etc.
* **collect database stats** - Querying the pg_stat_database view for statistics on databases.
* **calculate database stats** - Calculating differential statistics on databases since the previous sample.
* **collect tablespace stats** - Querying the pg_tablespace view for statistics on tablespaces.
* **collect statement stats** - Collecting statistics on statements using the _pg_stat_statements_ and _pg_stat_kcache_ extensions.
* **query pg_stat_bgwriter** - Collecting cluster statistics using the _pg_stat_bgwriter_ view.
* **query pg_stat_wal** - Collecting cluster WAL statistics using the _pg_stat_wal_ view.
* **query pg_stat_archiver** - Collecting cluster statistics using the _pg_stat_archiver_ view.
* **collect object stats** - Collecting statistics on database objects. Includes following events:
  * **db:_dbname_ collect tables stats** - Collecting statistics on tables for the _dbname_ database.
    * **db:_dbname_ collect limited table sizes** - Collecting vacuumed or sequentially scanned table sizes when sizes collection is disabled.
  * **db:_dbname_ collect indexes stats** - Collecting statistics on indexes for the _dbname_ database.
    * **db:_dbname_ collect limited index sizes** - Collecting index sizes for vacuumed tables when sizes collection is disabled.
  * **db:_dbname_ collect functions stats** - Collecting statistics on functions for the _dbname_ database.
* **maintain repository** - Executing support routines.
* **calculate tablespace stats** - Calculating differential statistics on tablespaces.
* **calculate object stats** - Calculating differential statistics on database objects. Includes following events:
  * **calculate tables stats** - Calculating differential statistics on tables of all databases.
  * **calculate indexes stats** - Calculating differential statistics on indexes of all databases.
  * **calculate functions stats** - Calculating differential statistics on functions of all databases.
* **calculate cluster stats** - Calculating cluster differential statistics.
* **calculate archiver stats** - Calculating archiever differential statistics.
* **delete obsolete samples** - Deleting obsolete baselines and samples.

### Baselines

Baseline is a named sample sequence, having its own retention setting. Baseline can be used in report-building functions as a sample interval. Undefined baseline retention means infinite retention.
You can use baselines to save information about database workload on certain time period. For example, you may want to save samples, gathered during load testing, or during regular load on your system for further reference.
Baseline management functions:

* __create_baseline([server name,] baseline_name varchar(25), start_id integer, end_id integer [, days integer])__ - create a baseline
  * _server_ - server name. *local* server is assumed if omitted
  * _name_ - baseline name. Each baseline must have unique name within a server.
  * _start_id_, _end_id_ - first and last samples, included in baseline.
  * _days_ - baseline retention time. Defined in integer days since _now()_. This parameter may be omitted (or set to _null_), meaning infinite retention.

* __create_baseline([server name,] baseline_name varchar(25), time_range tstzrange [, days integer])__ - create a baseline
  * _server_ - server name. *local* server is assumed if omitted
  * _name_ - baseline name. Each baseline must have unique name within a server.
  * _time_range_ - baseline time interval. Baseline will include all available samples, overlapping this interval.
  * _days_ - baseline retention time. Defined in integer days since _now()_. This parameter may be omitted (or be set to _null_), meaning infinite retention.

* __drop_baseline([server name,] name varchar(25))__ - drop a baseline
  * _server_ - server name. *local* server is assumed if omitted
  * _name_ - baseline name to drop. Dropping a baseline does not mean immediate drop of all its samples, they are just excluded from baseline, thus is not more covered with baseline retention.

* __keep_baseline([server name,] name varchar(25) [, days integer])__ - change retention of baselines
  * _server_ - server name. *local* server is assumed if omitted
  * _name_ - baseline name. This parameter may be omitted (or be set to _null_) to change retention of all existing baselines.
  * _days_ - retention time of a baseline in days since _now()_. Also, may be omitted (or set to null) to set infinite retention.

* __show_baselines([server name])__ - displays existing baselines. Call this function to get information about existing baselines (names, sample intervals, and retention times)
  * _server_ - server name. *local* server is assumed if omitted

  ```
  postgres=# SELECT * FROM profile.baseline_show('local');
  ```

### Data export and import
Collected samples can be exported from instance of *pg_profile* extension and than loaded into another one. This feature is useful when you want to move servers from one instance to another, or when you need to send collected data to your support team.

#### Data export
Data is exported as a regular table by function export_data(). You can use any method to export this table from your database. For example, you can use *copy* command of psql to obtain single .csv file:
```
postgres=# \copy (select * from export_data()) to 'export.csv'
```
By default *export_data()* function will export all samples of all configured servers. However you can limit export to only one server, and further limit sample range too:

* **export_data([server name, [min_sample_id integer,] [max_sample_id integer]] [, obfuscate_queries boolean])** - export collected data
  * *server* is a server name. All servers is assumed if omitted
  * *min_sample_id* and *max_sample_id* - export bounding sample identifiers (inclusive). Null value of *min_sample_id* bound cause export of all samples till *max_sample_id*, and null value of *max_sample_id* cause export of all samples since *min_sample_id*.
  * *obfuscate_queries* - use this parameter only when you want to hide query texts - they will be exported as MD5 hash.

#### Data import
Data can be imported from local table only, thus previously exported data is need to be loaded first. In our case with *copy* command:
```
postgres=# CREATE TABLE import (section_id bigint, row_data json);
CREATE TABLE
postgres=# \copy import from 'export.csv'
COPY 6437
```
Now we can perform data import, providing this table to *import_data()* function:
```
postgres=# SELECT * FROM import_data('import');
```
Although server descriptions is also imported, your local *pg_profile* servers with matching names will prohibit import operations. Consider temporarily rename those servers or use *server_name_prefix* parameter to avoid conflicts. If you'll need to import new data for previously imported servers, they will be matched by system identifier, so fell free to rename imported sever as you wish. All servers are imported in *disabled* state.
*import_data()* function takes only the imported table:
* **import_data(data regclass, server_name_prefix text)**
  * *data* - table containing exported data
  * *server_name_prefix* - prefix used for importing servers
This function returns number of rows actually loaded in extension tables.
After successful import operation you can drop *import* table.

### Reports

Reports are generated in HTML markup by reporting functions. There are two types of reports available in *pg_profile*:

* **Regular reports**, containing statistical information about instance workload during report interval
* **Differential reports**, containing data from two intervals with same objects statistic values located one next to other, making it easy to compare the workload

#### Regular report functions

* **get_report([server name,] start_id integer, end_id integer [, description text [, with_growth boolean]])** - generate report by sample identifiers
* **get_report([server name,] time_range tstzrange [, description text [, with_growth boolean]])** - generate report for the shortest sample interval, covering provided *time_range*.
* **get_report([server name], baseline varchar(25) [, description text [, with_growth boolean]])** - generate report, using baseline as samples interval
* **get_report_latest([server name])** - generate report for two latest samples
Function arguments:
  * *server* - server name. *local* server is assumed if omitted
  * *start_id* - interval begin sample identifier
  * *end_id* - interval end sample identifier
  * *time_range* - time range (*tstzrange* type)
  * *baseline* - a baseline name
  * *with_growth* - a flag, requesting interval expansion to nearest bounds with relation growth data available. Default value is *false*
  * *description* - a text memo, it will be included in report as a report description

#### Differential report functions

You can generate differential report using sample identifiers, baselines and time ranges as interval bounds:

* **get_diffreport([server name,] start1_id integer, end1_id integer, start2_id integer, end2_id integer [, description text [, with_growth boolean]])** - generate differential report on two intervals by sample identifiers
* **get_diffreport([server name,] baseline1 varchar(25), baseline2 varchar(25) [, description text [, with_growth boolean]])** - generate differential report on two intervals, defined by basename names
* **get_diffreport([server name,] time_range1 tstzrange, time_range2 tstzrange [, description text [, with_growth boolean]])** - generate differential report on two intervals, defined by time ranges
  * *server* is server name. *local* server is assumed if omitted
  * *start1_id*, *end1_id* - sample identifiers of first interval
  * *start2_id*, *end2_id* - sample identifiers of second interval
  * *baseline1* - baseline name of first interval
  * *baseline2* - baseline name of second interval
  * *time_range1* - first interval time range
  * *time_range2* - second interval time range
  * *description* is a text memo - it will be included in report as report description
  * *with_growth* is a flag, requesting both intervals expansion to nearest bounds with relation growth data available. Default value is *false*

Also, you can use some combinations of the above:

* **get_diffreport([server name,] baseline varchar(25), time_range tstzrange [, description text [, with_growth boolean]])**
* **get_diffreport([server name,] time_range tstzrange, baseline varchar(25) [, description text [, with_growth boolean]])**
* **get_diffreport([server name,] start1_id integer, end1_id integer, baseline varchar(25) [, description text [, with_growth boolean]])**
* **get_diffreport([server name,] baseline varchar(25), start2_id integer, end2_id integer [, description text [, with_growth boolean]])**

Report generation example:
```
$ psql -Aqtc "SELECT profile.get_report(480,482)" -o report_480_482.html
```
For any other server, use it's name:
```
$ psql -Aqtc "SELECT profile.get_report('omega',12,14)" -o report_omega_12_14.html
```
Report generation using time ranges:

```
psql -Aqtc "select profile.get_report(tstzrange('2020-05-13 11:51:35+03','2020-05-13 11:52:18+03'))" -o report_range.html
```

Also, time ranges is useful for generating periodic reports. Let's build last 24-hour report:

```
psql -Aqtc "select profile.get_report(tstzrange(now() - interval '1 day',now()))" -o last24h_report.html
```

Now you can view report file using any web browser.

## Sections of a report
Report tables and their columns are described in this section. The most objects mentioned in the report can be selected to highlight all related entries in a report.
### Server statistics

#### Database statistics

Contains per-database statistics during report interval, based on *pg_stat_database* view.

* *Database* - database name
* *Transactions* - database transaction statistics
  * *Commits* - number of committed transactions (*xact_commit*)
  * *Rollbacks* - number of rolled back transactions (*xact_rollback*)
  * *Deadlocks* - number of deadlocks detected (*deadlocks*)
* *Checksums* - checksum failures (if any)
  * *Failures* - number of block checksum failures detected
  * *Last* - last checksum failure detected
* *Block statistics* - database blocks read and hit statistics
  * *Hit(%)* - buffer cache hit ratio
  * *Read* - number of disk blocks read in this database (*blks_read*)
  * *Hit* - number of times disk blocks were found already in the buffer cache (*blks_hit*)
* *Block I/O times* - time spent on data blocks I/O
  * *Read* - time spent reading data file blocks by backends, in seconds
  * *Write* - time spent writing data file blocks by backends, in seconds
* *Tuples* - tuples statistics section
  * *Ret* - number of returned tuples (*tup_returned*)
  * *Fet* - number of fetched tuples (*tup_fetched*)
  * *Ins* - inserted tuples count (*tup_inserted*)
  * *Upd* - number of updated tuples (*tup_updated*)
  * *Del* - number of deleted tuples (*tup_deleted*)
* *Temp files* - temporary files statistics
  * *Size* - total amount of data written to temporary files by queries in this database (*temp_bytes*)
  * *Files* - number of temporary files created by queries in this database (*temp_files*)
* *Size* - database size at the end of report interval (*pg_database_size()*)
* *Growth* - database growth during report interval (*pg_database_size()* difference)

#### Session statistics by database
This section contains session-related data from *pg_stat_database* view. Available since Postgres 14

* *Database* - database name
* *Timings (s)* - session timings in seconds
  * *Total* - time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won't be included) (*session_time*)
  * *Active* - time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity) (*active_time*)
  * *Idle(T)* - time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity) (*idle_in_transaction_time*)
* *Sessions* - session counts for databases
  * *Established* - total number of sessions established to this database (*sessions*)
  * *Abandoned* - number of database sessions to this database that were terminated because connection to the client was lost (*sessions_abandoned*)
  * *Fatal* - number of database sessions to this database that were terminated by fatal errors (*sessions_fatal*)
  * *Killed* - number of database sessions to this database that were terminated by operator intervention (*sessions_killed*)

#### Statement statistics by database

Contains per-database aggregated total statistics of *pg_stat_statements* data (if *pg_stat_statements* extension was available during report interval)

* *Database* - database name
* *Calls* - total count of all statements executions (sum of *calls*)
* *Time (s)* - time spent in seconds
  * *Plan* - time spent planning (sum of *total_plan_time*) - available since pg_stat_statements 1.8
  * *Exec* - time spent executing (sum of *total_time* or *total_exec_time*)
  * *Read* - time spent reading blocks (sum of *blk_read_time*)
  * *Write* - time spent writing blocks (sum of *blk_write_time*)
  * *Trg* - time spent executing trigger functions
* *Temp I/O Time* - time spent on temporary file blocks I/O
  * *Read* - time spent reading temporary file blocks, in seconds
  * *Write* - time spent writing temporary file blocks, in seconds
* *Fetched (blk)* - total blocks fetched from disk and buffer cache
  * *Shared* - total fetched shared blocks count (sum of *shared_blks_read + shared_blks_hit*)
  * *Local* - total fetched local blocks count (sum of *local_blks_read + local_blks_hit*)
* *Dirtied (blk)* - total blocks dirtied in database
  * *Shared* - total number of shared blocks dirtied in the database (sum of *shared_blks_dirtied*)
  * *Local* - total number of local blocks dirtied in the database (sum of *local_blks_dirtied*)
* *Temp (blk)* - blocks used for operations (like joins and sorts)
  * *Read* - blocks read (sum of *temp_blks_read*)
  * *Write* - blocks written (sum of *temp_blks_written*)
* *Local (blk)* - blocks used for temporary tables
  * *Read* - blocks read (sum of *local_blks_read*)
  * *Write* - blocks written (sum of *local_blks_written*)
* *Statements* - total count of captured statements
* *WAL size* - total amount of WAL generated by statements (sum of *wal_bytes*)

#### JIT statistics by database

Contains per database total JIT-related statistics provided by *pg_stat_statements* extension. Available since PostgreSQL 15 (*pg_stat_statements* 1.10).

* *Database* - database name
* *Calls* - total count of all statements executions (sum of *calls*)
* *Time (s)* - time spent in seconds
  * *Plan* - time spent planning (sum of *total_plan_time*) - available since pg_stat_statements 1.8
  * *Exec* - time spent executing (sum of *total_time* or *total_exec_time*)
* *Generation*
  * *Count* - Total number of functions JIT-compiled by the statements
  * *Gen. time* - Total time spent by the statements on generating JIT code
* *Inlining*
  * *Count* - Number of times functions have been inlined
  * *Time* - Total time spent by statements on inlining functions (in seconds)
* *Optimization*
  * *Count* - Number of times statements has been optimized
  * *Time* - Total time spent by statements on optimizing (in seconds)
* *Emission*
  * *Count* - Number of times code has been emitted
  * *Time* - Total time spent by statements on emitting code (in seconds)

#### Cluster statistics

This table contains data from *pg_stat_bgwriter* view

* *Scheduled checkpoints* - total number of checkpoints, completed on schedule due to *checkpoint_timeout* parameter (*checkpoints_timed* field)
* *Requested checkpoints* - total number of other checkpoints: due to values of *max_wal_size*, *archive_timeout* and CHECKPOINT commands (*checkpoints_req* field)
* *Checkpoint write time (s)* - total time spent writing checkpoints in seconds (*checkpoint_write_time* field)
* *Checkpoint sync time (s)* - total time spent syncing checkpoints in seconds (*checkpoint_sync_time* field)
* *Checkpoints buffers written* - total number of buffers, written by checkpointer (*buffers_checkpoint* field)
* *Background buffers written* - total number of buffers, written by background writer process (*buffers_clean* field)
* *Backend buffers written* - total number of buffers, written by backends (*buffers_backend* field)
* *Backend fsync count* - total number of backend fsync calls (*buffers_backend_fsync* field)
* *Bgwriter interrupts (too many buffers)* - total count of background writer interrupts due to reaching value of the *bgwriter_lru_maxpages* parameter.
* *Number of buffers allocated* - total count of buffers allocated (*buffers_alloc* field)
* *WAL generated* - total amount of WAL generated (based on *pg_current_wal_lsn()* difference)
* *Start LSN* - WAL LSN at the start of the current report
* *End LSN* - WAL LSN at the end of the current report
* *WAL segments archived* - archived WAL segments count (based on *archived_count* of *pg_stat_archiver* view)
* *WAL segments archive failed*  - WAL segment archive failures count (based on *failed_count* of *pg_stat_archiver* view)

#### WAL statistics

This table contains data from *pg_stat_wal* view. Available since Postgres 14

* *WAL generated* - total amount of WAL generated (*wal_bytes*)
* *WAL per second* - average amount of WAL generated per second
* *WAL records* - total number of WAL records generated (*wal_records*)
* *WAL FPI* - total number of WAL full page images generated (*wal_fpi*)
* *WAL buffers full* - number of times WAL data was written to disk because WAL buffers became full (*wal_buffers_full*)
* *WAL writes* - number of times WAL buffers were written out to disk via XLogWrite request (*wal_write*)
* *WAL writes per second* - average number of times WAL buffers were written out to disk via XLogWrite request per second
* *WAL sync* - Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero) (*wal_sync*)
* *WAL syncs per second* - average number of times WAL files were synced to disk via issue_xlog_fsync request per second
* *WAL write time (s)* - total amount of time spent writing WAL buffers to disk via *XLogWrite* request, in seconds (if *track_wal_io_timing* is enabled, otherwise zero). This includes the sync time when *wal_sync_method* is either *open_datasync* or *open_sync*. (*wal_write_time*)
* *WAL write duty* - WAL write time as a percentage of the report duration time
* *WAL sync time (s)* - Total amount of time spent syncing WAL files to disk via *issue_xlog_fsync* request, in seconds (if *track_wal_io_timing* is enabled, *fsync* is on, and *wal_sync_method* is either *fdatasync*, *fsync* or *fsync_writethrough*, otherwise zero) (*wal_sync_time*)
* *WAL sync duty* - WAL sync time as a percentage of the report duration time

#### Tablespace statistics

This table contains information about tablespaces sizes and growth:

* *Tablespace* - tablespace name
* *Path* - tablespace path
* *Size* - tablespace size as it was at time of last sample in report interval
* *Growth* - tablespace growth during report interval

#### Wait sampling

This section contains data obtained from *pg_wait_sampling* extension if it was available during report interval.

##### Wait events types
This table provides information about summary time waited in events of each wait event type during report interval. Wait statistics is provided for events, detected by *pg_wait_sampling* extension during statements execution and overall waits (including background activity).
* *Wait exent type* - the type of wait event (see *pg_stat_activity*)
* *Statements Waited (s)* - amount of time waited in events of this event type by statements (when _pg_wait_sampling_profile_.queryid_ is not null) in seconds
* *%Total* - time waited in events of this wait event type by statements as a percentage of overall time waited by statements
* *All Waited (s)* - amount of time waited in events of this event type by all backends (including background activity)
* *%Total* - time waited in events of this wait event type by backends as a percentage of overall time waited by backends
##### Top wait events (statements)
This table contains top _pg_profile.topn_ wait events by summary time waited during statements execution (when _pg_wait_sampling_profile_.queryid_ is not null).
* *Wait exent type* - the type of wait event (see *pg_stat_activity*)
* *Wait exent* - wait event name (see *pg_stat_activity*)
* *Waited (s)* - amount of time waited in event by statements in seconds
* *%Total* - time waited in event by statements as a percentage of overall time waited by statements
##### Top wait events (All)
This table contains top _pg_profile.topn_ wait events by summary time waited including background activity.
* *Wait exent type* - the type of wait event (see *pg_stat_activity*)
* *Wait exent* - wait event name (see *pg_stat_activity*)
* *Waited (s)* - amount of time waited in event by all backends in seconds
* *%Total* - time waited in event by all backends as a percentage of overall time waited by backends

### SQL query statistics
This report section contains tables of top statements during report interval sorted by several important statistics. Data is captured from *pg_stat_statements* view if it was available at the time of samples.

#### Top SQL by elapsed time

This table contains top _pg_profile.topn_ statements sorted by elapsed time *total_plan_time* + *total_exec_time* of *pg_stat_statements* view. Available since Postgres 13

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *%Total* - total time of this statement as a percentage of total time of all statements in a cluster
* *Time (s)* - time spent in this statement (in seconds)
  * *Elapsed* - total time, spent in this statement (*total_plan_time* + *total_exec_time*)
  * *Plan* - time spent in planning this statement (*total_plan_time* field)
  * *Exec* - time spent executing this query (*total_exec_time* field)
* *JIT time (s)* - Total time spent by JIT executing this statement, in seconds (*jit_generation_time* + *jit_inlining_time* + *jit_optimization_time* + *jit_emission_time*)
* *I/O time (s)*:
  * *Read* - time spent reading blocks (*blk_read_time* field)
  * *Write* - time spent writing blocks (*blk_write_time* field)
* CPU time (s) - time spent on CPU. Based on data provided by *pg_stat_kcache* extension.
  * *Usr* - CPU time spent in user space
  * *Sys* - CPU time spent in kernel space
* *Plans* - number of times the statement was planned (*plans* field)
* *Executions* - number of times the statement was executed (*calls* field)

#### Top SQL by planning time

Top _pg_profile.topn_ statements sorted by *total_plan_time* field of *pg_stat_statements* view. Available since Postgres 13

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Plan elapsed (s)* - time spent in planning this statement (*total_plan_time* field)
* *%Elapsed* - plan time of this statement as a percentage of statement elapsed time
* *Plan times (ms)* - detailed plan time statistics of this statement (in milliseconds)
  * *Mean* - mean time spent planning this statement (*mean_plan_time* field)
  * *Min* - minimum time spent planning this statement (*min_plan_time* field)
  * *Max* - maximum time spent planning this statement (*max_plan_time* field)
  * *StdErr* - population standard deviation of time spent planning this statement (*stddev_plan_time* field)
* *Plans* - number of times this statement was planned (*plans* field)
* *Executions* - number of times this statement was executed (*calls* field)

#### Top SQL by execution time

Top _pg_profile.topn_ statements sorted by *total_time* (or *total_exec_time*) field of *pg_stat_statements* view

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Exec (s)* - time spent executing this statement (*total_exec_time* field)
* *%Elapsed* - execution time of this statement as a percentage of statement elapsed time
* *%Total* - exec time of this statement as a percentage of total elapsed time of all statements in a cluster
* *JIT time (s)* - Total time spent by JIT executing this statement, in seconds (*jit_generation_time* + *jit_inlining_time* + *jit_optimization_time* + *jit_emission_time*)
* *I/O time (s)*:
  * *Read* - time spent reading blocks (*blk_read_time* field)
  * *Write* - time spent writing blocks (*blk_write_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Execution times (ms)* - detailed execution time statistics of this statement (in milliseconds)
  * *Mean* - mean time spent executing this statement (*mean_exec_time* field)
  * *Min* - minimum time spent executing this statement (*min_exec_time* field)
  * *Max* - maximum time spent executing this statement (*max_exec_time* field)
  * *StdErr* - population standard deviation of time spent executing this statement (*stddev_exec_time* field)
* *Executions* - number of times this statement was executed (*calls* field)

#### Top SQL by executions

Top _pg_profile.topn_ statements sorted by *calls* field of *pg_stat_statements* view

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Executions* - count of statement executions (*calls* field)
* *%Total* - *calls* of this statement as a percentage of total *calls* of all statements in a cluster
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Mean(ms)* - mean time spent in the statement, in milliseconds (*mean_time* or *mean_exec_time* field)
* *Min(ms)* - minimum time spent in the statement, in milliseconds (*min_time* or *min_exec_time* field)
* *Max(ms)* - maximum time spent in the statement, in milliseconds (*max_time* or *max_exec_time* field)
* *StdErr(ms)* - population standard deviation of time spent in the statement, in milliseconds (*stddev_time* or *stddev_exec_time* field)
* *Elapsed(s)* - amount of time spent executing this query, in seconds (*total_time* or *total_exec_time* field)

#### Top SQL by I/O wait time

Top _pg_profile.topn_ statements sorted by read and write time (*blk_read_time* + *blk_write_time*)

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *IO(s)* - amount of time spent on reading and writing (I/O time) by this statement in seconds (*blk_read_time* + *blk_write_time*)
* *R(s)* - amount of time spent on reading by this statement in seconds (*blk_read_time*)
* *W(s)* - amount of time spent on writing by this statement in seconds (*blk_write_time*)
* *%Total* - I/O time of this statement as a percentage of total I/O time for all statements in a cluster
* *Reads* - number of blocks read by this statement divided in three sub-columns:
  * *Shr* - shared reads (*shared_blks_read* field)
  * *Loc* - local reads (*local_blks_read* field)
  * *Tmp* - temp reads (*temp_blks_read* field)
* *Writes* - number of blocks written by this statement divided in three sub-columns:
  * *Shr* - shared writes (*shared_blks_written* field)
  * *Loc* - local writes (*local_blks_written* field)
  * *Tmp* - temp writes (*temp_blks_written* field)
* *Elapsed(s)* - amount of time spent executing this query, in seconds (*total_time* or *total_exec_time* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by shared blocks fetched

Top _pg_profile.topn_ statements sorted by read and hit blocks, helping to detect the most data processing statements.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *blks fetched* - number of fetched blocks (expression: *shared_blks_hit* + *shared_blks_read*)
* *%Total* - blocks fetched for this statement as a percentage of total blocks fetched for all statements in a cluster
* *Hits(%)* - percentage of blocks got from buffers within all blocks got
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by shared blocks read

Top _pg_profile.topn_ statements sorted by shared reads, helping to detect most read intensive statements.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Reads* - number of shared read blocks for this statement (*shared_blks_read* field)
* *%Total* - shared reads for this statement as a percentage of total shared reads for all statements in a cluster
* *Hits(%)* - percentage of blocks got from buffers within all blocks got
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by shared blocks dirtied

Top _pg_profile.topn_ statements sorted by shared dirtied buffer count, helping to detect most data changing statements.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Dirtied* - number of shared blocks dirtied by this statement (*shared_blks_dirtied* field)
* *%Total* - shared blocks dirtied by this statement as a percentage of total shared blocks dirtied by all statements in a cluster
* *Hits(%)* - percentage of blocks got from buffers within all blocks got
* *WAL* - amount of WAL generated by the statement (*wal_bytes* field)
* *%Total* - amount of WAL generated by the statement as a percentage of total WAL generated in cluster (*pg_current_wal_lsn()* increment)
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by shared blocks written

Top _pg_profile.topn_ statements, which had to perform writes sorted by written blocks count.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Written* - number of blocks written by this statement (*shared_blks_written* field)
* *%Total* - number of blocks written by this statement as a percentage of total blocks written by all statements in a cluster
* *%BackendW* - number of blocks written by this statement as a percentage of all blocks written in a cluster by backends (*buffers_backend* field of *pg_stat_bgwriter* view)
* *Hits(%)* - percentage of blocks got from buffers within all blocks got
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by WAL size

Top _pg_profile.topn_ statements, sorted by WAL generated (available since *pg_stat_statements* v1.8)

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *WAL* - amount of WAL generated by the statement (*wal_bytes* field)
* *%Total* - amount of WAL generated by the statement as a percentage of total WAL generated in cluster (*pg_current_wal_lsn()* increment)
* *Dirtied* - number of shared blocks dirtied by this statement (*shared_blks_dirtied* field)
* *WAL FPI* - total number of WAL full page images generated by the statement (*wal_fpi* field)
* *WAL records* - total amount of WAL bytes generated by the statement (*wal_bytes* field)

#### Top SQL by temp usage

Top _pg_profile.topn_ statements sorted by temp I/O, calculated as the sum of *temp_blks_read*, *temp_blks_written*, *local_blks_read* and *local_blks_written* fields

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Local fetched* - number of retrieved local blocks (expression: *local_blks_hit* + *local_blks_read*)
* *Hits(%)* - percentage of local blocks got from temp buffers within all local blocks got
* *Local (blk)* - I/O statistics of blocks used in temporary tables
  * *Write* - number of written local blocks (*local_blks_written*)
  * *%Total* - *local_blks_written* of this statement as a percentage of total *local_blks_written* for all statements in a cluster
  * *Read* - number of read local blocks (*local_blks_read*)
  * *%Total* - *local_blks_read* of this statement as a percentage of total *local_blks_read* for all statements in a cluster
* *Temp (blk)* - I/O statistics of blocks used in operations (like sorts and joins)
  * *Write* - number of written temp blocks (*temp_blks_written*)
  * *%Total* - *temp_blks_written* of this statement as a percentage of total *temp_blks_written* for all statements in a cluster
  * *Read* - number of read local blocks (*temp_blks_read*)
  * *%Total* - *temp_blks_read* of this statement as a percentage of total *temp_blks_read* for all statements in a cluster
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### Top SQL by temp usage

Top _pg_profile.topn_ statements sorted by temp I/O time, calculated as the sum of *temp_blk_read_time* and *temp_blk_write_time* fields

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Temp I/O time (s)* - time spent on temporary file blocks I/O
  * *Read* - time spent reading temporary file blocks, in seconds
  * *Write* - time spent writing temporary file blocks, in seconds
  * *%Total* - temporary I/O time of this statement as a percentage of total temporary I/O time of all statements in a cluster
* *Temp (blk)* - I/O statistics of blocks used in operations (like sorts and joins)
  * *Write* - number of written temp blocks (*temp_blks_written*)
  * *%Total* - *temp_blks_written* of this statement as a percentage of total *temp_blks_written* for all statements in a cluster
  * *Read* - number of read local blocks (*temp_blks_read*)
  * *%Total* - *temp_blks_read* of this statement as a percentage of total *temp_blks_read* for all statements in a cluster
* *Elapsed(s)* - amount of time spent in this statement, in seconds (*total_time* or *total_exec_time+total_plan_time* field)
* *Rows* - number of rows retrieved or affected by the statement (*rows* field)
* *Executions* - number of executions for this statement (*calls* field)

#### rusage statistics

This section contains resource usage statistics provided by *pg_stat_kcache* extension if it was available during report interval.

##### Top SQL by system and user time

Top _pg_profile.topn_ statements sorted by sum of fields *user_time* and *system_time* fields of *pg_stat_kcache*.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *User Time* - User CPU time used
  * *Plan (s)* - User CPU time elapsed during planning in seconds (*plan_user_time* field)
  * *Exec (s)* - User CPU time elapsed during execution in seconds (*exec_user_time* or *user_time* field)
  * *%Total* - User CPU time of this statement as a percentage of a summary user CPU time for all statements
* *System Time* - System CPU time used
  * *Plan (s)* - System CPU time elapsed during planning in seconds (*plan_system_time* field)
  * *Exec (s)* - System CPU time elapsed during execution in seconds (*exec_system_time* or *system_time* field)
  * *%Total* - System CPU time of this statement as a percentage of a summary system CPU time for all statements

##### Top SQL by reads/writes done by filesystem layer

Top _pg_profile.topn_ statements sorted by sum of fields *reads* and *writes* fields of *pg_stat_kcache*.

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *Read Bytes* - Number of bytes read by the filesystem layer
  * *Plan* - bytes read during planning (*plan_reads* field)
  * *Exec* - bytes read during execution (*exec_reads* field)
  * *%Total* - Bytes read of this statement as a percentage of a summary read bytes for all statements
* *Writes* - Number of bytes written by the filesystem layer (*writes* field)
  * *Plan* - bytes written during planning (*plan_writes* field)
  * *Exec* - bytes written during execution (*exec_writes* field)
  * *%Total* - Bytes written of this statement as a percentage of a summary written bytes for all statements

#### Top SQL by JIT elapsed time

Top _pg_profile.topn_ statements sorted by the total JIT-related time (i.e. *jit_generation_time* + *jit_inlining_time* + *jit_optimization_time* + *jit_emission_time*). Available since PostgreSQL 15 (*pg_stat_statements* 1.10).

* *Query ID* - Query identifier as provided by the *pg_stat_statements* extension (*queryid*) in hexadecimal notation. Alternative query identifier as a hash of *dbid*, *userid* and *queryid* is shown in square brackets, this identifier is compatible with *pgcenter* utility.
* *Database* - Statement database name (derived from *dbid* field)
* *User* - User name executed this statement (derived from *userid* field)
* *JIT total (s)* - time spent by JIT in seconds (*jit_generation_time* + *jit_inlining_time* + *jit_optimization_time* + *jit_emission_time*)
* *Generation*
  * *Count* - Total number of functions JIT-compiled by the statement
  * *Gen. time* - Total time spent by the statement on generating JIT code
* *Inlining*
  * *Count* - Number of times functions have been inlined
  * *Time* - Total time spent by statement on inlining functions (in seconds)
* *Optimization*
  * *Count* - Number of times statement has been optimized
  * *Time* - Total time spent by statement on optimizing (in seconds)
* *Emission*
  * *Count* - Number of times code has been emitted
  * *Time* - Total time spent by statement on emitting code (in seconds)
* *Time (s)* - time spent in this statement (in seconds)
  * *Plan* - time spent in planning this statement (*total_plan_time* field)
  * *Exec* - time spent executing this query (*total_exec_time* field)
* *I/O time (s)*:
  * *Read* - time spent reading blocks (*blk_read_time* field)
  * *Write* - time spent writing blocks (*blk_write_time* field)

#### Complete list of SQL texts

Query texts of all statements mentioned in report. You can use *Query ID* link in any statistic table to get there and see query text. Queries in this section are limited to _pg_profile.max_query_length_ (default 20000) characters, but full query text can be obtained from extension tables. One *queryid* entry in this list can have several actual query texts captured in different samples. Only three latest queries texts captured will be listed.

### Schema object statistics

This section of report contains top database objects, using statistics from Statistics Collector views.

#### Top tables by estimated sequentially scanned volume

Top database tables sorted by estimated volume, read by sequential scans. Based on *pg_stat_all_tables* view. Here you can search for tables, possibly lacks some index on it. When there is no relation sizes collected with *pg_relation_size()*, estimation will be calculated based on *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *~SeqBytes* - estimated volume, read by sequential scans. Calculated as a sum of *relation size* multiplied by *seq_scan* for all samples of a report.
* *SeqScan* - number of sequential scans performed on the table (*seq_scan* field)
* *IxScan* - number of index scans initiated on this table (*idx_scan* field)
* *IxFet* - number of live rows fetched by index scans (*idx_tup_fetch* field)
* *Ins* - number of rows inserted (*n_tup_ins* field)
* *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
* *Del* - number of rows deleted (*n_tup_del* field)
* *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)

#### Top tables by blocks fetched

Fetched block is a block being processed from disk (read), or from shared buffers (hit). Tables in this list are sorted by sum of fetched blocks for table relation, its indexes, TOAST of a table (if exists), and TOAST index (if exists). This section can focus your attention on tables with excessive blocks processing. Based on data of *pg_statio_all_tables* view.

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Heap* - statistics for relation blocks fetched (*heap_blks_read* + *heap_blks_hit*)
* *Ix* - statistics for all relation indexes blocks fetched (*idx_blks_read* + *idx_blks_hit*)
* *TOAST* - statistics for TOAST-table blocks fetched (*toast_blks_read* + *toast_blks_hit*)
* *TOAST-Ix* - statistics for TOAST index blocks fetched (*tidx_blks_read* + *tidx_blks_hit*)

Each statistic field in this table is divided in two columns:
* *Blks* - number of blocks fetched for relation heap, index, TOAST or TOAST index
* *%Total* - blocks fetched for relation heap, index, TOAST or TOAST index as a percentage of all blocks fetched in a whole cluster

#### Top tables by blocks read

Top tables sorted by block reads. Tables in this list are sorted by sum of block reads for table, its indexes, TOAST of a table (if exists), and TOAST index (if exists). This section can focus your attention on tables with excessive blocks reading. Based on data of *pg_statio_all_tables* view.

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Heap* - statistics for relation block reads (*heap_blks_read*)
* *Ix* - statistics for all relation indexes blocks reads (*idx_blks_read*)
* *TOAST* - statistics for TOAST-table block reads (*toast_blks_read*)
* *TOAST-Ix* - statistics for TOAST index block reads (*tidx_blks_read*)
* *Hit(%)* - number of heap, indexes, toast and toast index blocks fetched from shared buffers as a percentage of all their blocks fetched from shared buffers and file system

Each read statistic in this table is divided in two columns:
* *Blks* - number of block reads for relation heap, index, TOAST or TOAST index
* *%Total* - block reads for relation heap, index, TOAST or TOAST index as a percentage of all block reads in a whole cluster

#### Top DML tables

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

#### Top tables by updated/deleted tuples

Top tables sorted by amount of operations, causing autovacuum load, i.e. sum of *n_tup_upd* and *n_tup_del* (including TOAST tables). Consider fine-tune of vacuum-related parameters based on provided vacuum and analyze run statistics.

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
* *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)
* *Del* - number of rows deleted (*n_tup_del* field)
* *Vacuum* - number of times this table has been manually vacuumed (not counting VACUUM FULL) (*vacuum_count* field)
* *AutoVacuum* - number of times this table has been vacuumed by the autovacuum daemon (*autovacuum_count* field)
* *Analyze* - number of times this table has been manually analyzed (*analyze_count* field)
* *AutoAnalyze* - number of times this table has been analyzed by the autovacuum daemon (*autoanalyze_count* field)

#### Top growing tables

Top tables sorted by growth. When there is no relation sizes collected with *pg_relation_size()*, table growth will be calculated based on *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Size* - table size, as it was at the moment of last sample in report interval
* *Growth* - table growth
* *Ins* - number of rows inserted (*n_tup_ins* field)
* *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
* *Del* - number of rows deleted (*n_tup_del* field)
* *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)

#### Top indexes by blocks fetched

Fetched block is a block being processed from disk (read), or from shared buffers (hit). Based on data of *pg_statio_all_indexes* view.

* *DB* - database name of the index
* *Tablespace* - tablespace name, where the index is located
* *Schema* - schema name of the index
* *Table* - table name
* *Index* - index name
* *Scans* - number of scans, performed on index (*idx_scan* field)
* *Blks* - blocks fetched from this index (*idx_blks_read* + *idx_blks_hit*)
* *%Total* - blocks fetched for this index as a percentage of all blocks fetched in a whole cluster

#### Top indexes by blocks read

Top indexes sorted by block reads. Based on data of *pg_statio_all_indexes* view.

* *DB* - database name of the index
* *Tablespace* - tablespace name, where the index is located
* *Schema* - schema name of the index
* *Table* - table name
* *Index* - index name
* *Scans* - number of scans, performed on index (*idx_scan* field)
* *Blk Reads* - number of disk blocks read from this index (*idx_blks_read*)
* *%Total* - block reads from this index as a percentage of all block reads in a whole cluster
* *Hits(%)* - percentage of index blocks got from buffer cache within all index blocks fetched for this index

#### Top growing indexes

Top indexes sorted by growth. When there is no relation sizes collected with *pg_relation_size()*, estimation will be calculated based on *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

* *DB* - database name of the index
* *Tablespace* - tablespace name, where the index is located
* *Schema* - schema name of the index
* *Table* - table name
* *Index* - index name
* *Index* - index statistics
  * *Size* - index size, as it was at the moment of last sample in report interval
  * *Growth* - index growth during report interval
* *Table* - underlying table statistics
  * *Ins* - number of rows inserted into underlying table (*n_tup_ins* field)
  * *Upd* - number of rows updated in underlying table (without HOT) (*n_tup_upd* - *n_tup_hot_upd*)
  * *Del* - number of rows deleted from underlying table (*n_tup_del* field)

#### Unused indexes

Non-scanned indexes during report interval sorted by DML operations on underlying tables, causing index support. Constraint indexes are excluded.

* *DB* - database name of the index
* *Tablespace* - tablespace name, where the index is located
* *Schema* - schema name of the index
* *Table* - table name
* *Index* - index name
* *Index* - index statistics
  * *Size* - index size, as it was at the moment of last sample in report interval
  * *Growth* - index growth during report interval
* *Table* - underlying table statistics
  * *Ins* - number of rows inserted into underlying table (*n_tup_ins* field)
  * *Upd* - number of rows updated in underlying table (without HOT) (*n_tup_upd* - *n_tup_hot_upd*)
  * *Del* - number of rows deleted from underlying table (*n_tup_del* field)

### User function statistics

This report section contains top functions in cluster, based on *pg_stat_user_functions* view.

#### Top functions by total time

Top functions sorted by time elapsed.

* *DB* - database name of the function
* *Schema* - schema name of the index
* *Function* - function name
* *Executions* - number of times this function has been called (*calls* field)
* *Time (s)* - function timing statistics in seconds
  * *Total* - total time spent in this function and all other functions called by it (*total_time* field)
  * *Self* - total time spent in this function itself, not including other functions called by it (*self_time* field)
  * *Mean* - mean time of single function execution
  * *Mean self* - mean self time of single function execution

#### Top functions by executions

Top functions sorted by executions count.

* *DB* - database name of the function
* *Schema* - schema name of the index
* *Function* - function name
* *Executions* - number of times this function has been called (*calls* field)
* *Time (s)* - function timing statistics in seconds
  * *Total* - total time spent in this function and all other functions called by it (*total_time* field)
  * *Self* - total time spent in this function itself, not including other functions called by it (*self_time* field)
  * *Mean* - mean time of single function execution
  * *Mean self* - mean self time of single function execution

#### Top trigger functions by total time

Top trigger functions sorted by time elapsed.

* *DB* - database name of the function
* *Schema* - schema name of the index
* *Function* - function name
* *Executions* - number of times this function has been called (*calls* field)
* *Time (s)* - function timing statistics in seconds
  * *Total* - total time spent in this function and all other functions called by it (*total_time* field)
  * *Self* - total time spent in this function itself, not including other functions called by it (*self_time* field)
  * *Mean* - mean time of single function execution
  * *Mean self* - mean self time of single function execution

### Vacuum-related stats

#### Top tables by vacuum operations

Top tables sorted by vacuums (manual and automatic) processed

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Vacuum count* - number of times this table has been manually vacuumed (not counting VACUUM FULL) (*vacuum_count* field)
* *Autovacuum count* - number of times this table has been vacuumed by the autovacuum daemon (*autovacuum_count* field)
* *Ins* - number of rows inserted (*n_tup_ins* field)
* *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
* *Del* - number of rows deleted (*n_tup_del* field)
* *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)

#### Top tables by analyze operations

Top tables sorted by analyze run (manual and automatic) count

* *DB* - database name of the table
* *Tablespace* - tablespace name, where the table is located
* *Schema* - schema name of the table
* *Table* - table name
* *Analyze count* - number of times this table has been manually analyzed (*analyze_count* field)
* *Autoanalyze count* - number of times this table has been analyzed by the autovacuum daemon (*autoanalyze_count* field)
* *Ins* - number of rows inserted (*n_tup_ins* field)
* *Upd* - number of rows updated (including HOT) (*n_tup_upd* field)
* *Del* - number of rows deleted (*n_tup_del* field)
* *Upd(HOT)* - number of rows HOT updated (*n_tup_hot_upd* field)

#### Top indexes by estimated vacuum load

This table provides estimation of implicit vacuum load caused by table indexes. Here is top indexes sorted by count of vacuums performed on underlying table multiplied by index size. When there is no relation sizes collected with *pg_relation_size()*, estimation will be calculated based on *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

* *DB* - database name of the index
* *Tablespace* - tablespace name, where the index is located
* *Schema* - schema name of the index
* *Table* - table name
* *Index* - index name
* *~Vacuum bytes* - vacuum load estimation calculated as (*vacuum_count* + *autovacuum_count*) * *index_size*
* *Vacuum cnt* - number of times this table has been manually vacuumed (not counting VACUUM FULL) (*vacuum_count* field)
* *Autovacuum cnt* - number of times this table has been vacuumed by the autovacuum daemon (*autovacuum_count* field)
* *IX size* - average index size during report interval
* *Relsize* - average relation size during report interval

#### Top tables by dead tuples ratio
This section contains modified tables with last vacuum run. Statistics is valid for last sample in report interval. Based on *pg_stat_all_tables* view. When there is no relation sizes collected with *pg_relation_size()*, size will be provided from *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

Top tables, sized 5 MB and more, sorted by dead tuples ratio.

* *DB* - database name of the table
* *Schema* - schema name of the table
* *Table* - table name
* *Live* - estimated number of live rows (*n_live_tup*)
* *Dead* - estimated number of dead rows (*n_dead_tup*)
* *%Dead* - dead rows of the table as a percentage of all rows in the table
* *Last AV* - last time when this table was vacuumed by the autovacuum daemon (*last_autovacuum*)
* *Size* - table size, as it was at the moment of last report sample.

#### Top tables by modified tuples ratio
This section contains modified tables with last vacuum run. Statistics is valid for last sample in report interval. Based on *pg_stat_all_tables* view. When there is no relation sizes collected with *pg_relation_size()*, size will be provided from *pg_class.relpages* field. This will be less accurate, so such values will be provided in square brackets.

Top tables, sized 5 MB and more, sorted by modified tuples ratio.

* *DB* - database name of the table
* *Schema* - schema name of the table
* *Table* - table name
* *Live* - estimated number of live rows (*n_live_tup*)
* *Dead* - estimated number of dead rows (*n_dead_tup*)
* *Mod* - estimated number of rows modified since this table was last analyzed (*n_mod_since_analyze*)
* *%Mod* - modified rows of the table as a percentage of all rows in the table
* *Last AA* - last time when this table was analyzed by the autovacuum daemon
* *Size* - table size, as it was at the moment of last report sample.

### Cluster settings during the report interval

This section of a report contains PostgreSQL GUC parameters, and values of functions *version()*, *pg_postmaster_start_time()*, *pg_conf_load_time()* and field *system_identifier* of *pg_control_system()* function during report interval.

* *Setting* - name of a parameter
* *reset_val* - reset_val field of *pg_settings* view. Bold font is used to show settings, changed during report interval.
* *Unit* - setting unit
* *Source*  - configuration file, where this setting was defined, line number after semicolon.
* *Notes* - This field will contain timestamp of a sample, when this value was observed first time during report interval.

### What you need to remember...
1. PostgreSQL collects execution statistics __after__ execution is complete. If single execution of a statement lasts for several samples, it will affect statistics of only last sample (when it was completed). And you can't get statistics on still running statements. Also, maintenance processes like vacuum and checkpointer will update statistics only on completion.
1. Resetting any PostgreSQL statistics may affect accuracy of a next sample.
1. Exclusive locks on relations conflicts with calculating relation size. Sample won't collect relation sizes of relations with AccessExclusiveLock held by any session. However, a session can aquire AccessExclusiveLock on relation during sample processing. To workaround this problem, _lock_timeout_ is set to 3s, so if _take_sample()_ function will be unable to acquire a lock for 3 seconds, it will fail, and no sample will be generated.
