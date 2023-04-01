# Grafana dashboards for *pg_profile* #
You can use provided grafana dashboards to visualize summary database load over time. Those dashboards are using *pg_profile* repository as the data source. Visualization of *pg_profile* samples should help you to detect time intervals with the specific load profile you want to see in a report.

The header of a dashboard will provide you with the *get_report()* function call to build a *pg_profile* report on exact time interval you see in the grafana.

There are two dashboards provided:
* **pg_profile_visualization.json** - this is the main dashboard to use with pg_profile repository. It provides summary information for each database
* **pg_profile_summary.json** - this dashboard provides summary information for the whole cluster. Use it if you have a lot of databases in your cluster and visualization in per-database manner seems overloaded

To use those dashboards you will need a PostgreSQL data source in your grafana installation. This data source should be pointing to the database with the *pg_profile* extension installed. If your *pg_profile* extension is installed in its own schema make sure that database user used in grafana data source has this schema in *search_path* setting.
