# pg_profile
This extension for PostgreSQL helps you to find out most resource intensive activities in your PostgreSQL databases.
## Concepts
This extension is based on statistics views of PostgreSQL and contrib extension *pg_stat_statements*. It is written in pure pl/pgsql and doesn't need any external libraries or software, but PostgreSQL database itself, and a cron-like tool performing periodic tasks. Initially developed and tested on PostgreSQL 9.6, and may be incompatible with earlier releases.

Historic repository will be created in your database by this extension. This repository will hold statistics "snapshots" for your postgres clusters. Snapshot is taken by calling _snapshot()_ function. PostgreSQL doesn't have any job-like engine, so you'll need to use *cron*.

Periodic snapshots can help you finding most resource intensive activities in the past. Suppose, you were reported performance degradation several hours ago. Resolving such issue, you can build a report between two snapshots bounding performance issue period to see load profile of your database. It's worse using a monitoring tool such as Zabbix to know exact time when performance issues was happening.

You can take an explicit snapshot before running any batch processing, and after it will be done.

Any time you make a snapshot, _pg_stat_statements_reset()_ will be called, ensuring you will not loose statements due to reaching *pg_stat_statements.max*. Also, report will contain section, informing you if captured statements count in any snapshot reaches 90% of _pg_stat_statements.max_.

*pg_profile*, installed in one cluster is able to collect statistics from other clusters, called *nodes*. You just need to define some nodes, providing names and connection strings and make sure connection can be established to all databases of all nodes. Now you can track statistics on your standbys from master, or from any other node. Once extension is installed, a *local* node is automatically created - this is a node for cluster where *pg_profile* resides.

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