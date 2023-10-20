# pg_profile
This extension for PostgreSQL helps you to find out most resource intensive activities in your PostgreSQL databases.
## Concepts
This extension is based on statistics views of PostgreSQL and contrib extensions *pg_stat_statements* and *pg_stat_kcache*. It is written in pure pl/pgsql and doesn't need any external libraries or software, but PostgreSQL database itself and a cron-like tool performing periodic tasks. Initially developed and tested on PostgreSQL 9.6 extension may be incompatible with earlier releases.

Historic repository will be created in your database by this extension. This repository will hold statistics "samples" for your postgres clusters. Sample is taken by calling _take_sample()_ function. PostgreSQL doesn't have any job-like engine, so you'll need to use *cron*.

Periodic samples can help you finding most resource intensive activities in the past. Suppose, you were reported performance degradation several hours ago. Resolving such issue, you can build a report between two samples bounding performance issue period to see load profile of your database. It's worth using a monitoring tool such as Zabbix to know exact time when performance issues was happening.

You can take an explicit samples before running any batch processing, and after it will be done.

Any time you take a sample, _pg_stat_statements_reset()_ will be called, ensuring you will not loose statements due to reaching *pg_stat_statements.max*. Also, report will contain section, informing you if captured statements count in any sample reaches 90% of _pg_stat_statements.max_. Reset performed by _pg_profile_ extention will affect statistics collected by other monitoring tools from _pg_stat_statements_ view.

*pg_profile*, installed in one cluster is able to collect statistics from other clusters, called *servers*. You just need to define some servers, providing names and connection strings and make sure connection can be established to all databases of all defined servers. Now you can track statistics on your standbys from master, or from any other server. Once extension is installed, a *local* server is automatically created - this is a *server* for cluster where *pg_profile* resides.

Report examples:
* [Regular report from Postgres 16 database](https://zubkov-andrei.github.io/pg_profile/report_examples/pg16.html)
* [Differential report from Postgres 16 database](https://zubkov-andrei.github.io/pg_profile/report_examples/pg16_diff.html)

## Grafana dashboard ##
There are some grafana dashboards provided in the grafana folder of the project. They have separate [documentation](grafana/README.md).

## Prerequisites

Although *pg_profile* is usually installed in the target cluster, it also can collect performance data from other clusters. Hence, we have prerequisites for *pg_profile* database, and for *servers*.

### pg_profile database prerequisites

_pg_profile_ extension depends on extensions _plpgsql_ and _dblink_.

### Servers prerequisites

The only mandatory requirement for server cluster is the ability to connect from pg_profile database using provided server connection string. All other requirements are optional, but they can improve completeness of gathered statistics.

Consider setting following Statistics Collector parameters:

```
track_activities = on
track_counts = on
track_io_timing = on
track_wal_io_timing = on      # Since Postgres 14
track_functions = all/pl
```

If you need statement statistics in reports, then database, mentioned in server connection string must have _pg_stat_statements_ extension installed and configured. Set *pg_stat_statements* parameters to meet your needs (see PostgreSQL documentation):

* _pg_stat_statements.max_ - low setting for this parameter may cause some statements statistics to be wiped out before sample is taken. Report will warn you if your _pg_stat_statements.max_ is seems to be undersized.
* _pg_stat_statements.track = 'top'_ - _all_ value will affect accuracy of _%Total_ fields for statements-related sections of report.

If CPU and filesystem statistics is needed, consider installing *pg_stat_kcache* extension.

## Supported versions
## PostgreSQL
* **16** supported since version 4.3
* **15** supported since version 4.1
* **14** supported since version 0.3.4
* **13** supported since version 0.1.3
* **12** supported since version 0.1.0
## _pg_stat_statements_ extension
* **1.10** supported since version 4.1
* **1.9** supported since version 4.0
* **1.8** supported since version 0.1.2
## _pg_stat_kcache_ extension
* **2.2.2** supported since version 4.3
* **2.2.1** supported since version 4.0
* **2.2.0** supported since version 0.3.1
* **2.1.3** supported since version 0.2.1


## Installation

### Step 1 Installation of extension files

Extract extension files (see [Releases](https://github.com/zubkov-andrei/pg_profile/releases) page) to PostgreSQL extensions location, which is

```
# tar xzf pg_profile-<version>.tar.gz --directory $(pg_config --sharedir)/extension
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

## Building and installing pg_profile

You will need postgresql development packages to build pg_profile.

```
sudo make USE_PGXS=y install && make USE_PGXS=y installcheck
```

If you only need to get sql-script for manual creation of *pg_profile* objects - it may be useful in case of RDS installation, do

```
make USE_PGXS=y sqlfile
```

Now you can use pg_profile--{version}.sql as sql script to create pg_profile objects. Such installation will lack extension benefits of PostgreSQL, but you can install it without server file system access.

------

Please, read full documentation in doc/pg_profile.md
