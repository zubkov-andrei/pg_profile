# Grafana dashboards for *pg_profile* #
You can use provided grafana dashboards to visualize summary database load over time. Those dashboards are using *pg_profile* repository as the data source. Visualization of *pg_profile* samples should help you to detect time intervals with the specific load profile you want to see in a report.

The header of a dashboard will provide you with the *get_report()* function call to build a *pg_profile* report on exact time interval you see in the grafana.

Dashboards provided:
* **pg_profile_visualization.json** - this is the main dashboard to use with pg_profile repository. It provides summary information for each database
* **pg_profile_summary.json** - this dashboard provides summary information for the whole cluster. Use it if you have a lot of databases in your cluster and visualization in per-database manner seems overloaded

To use those dashboards you will need a PostgreSQL data source in your grafana installation. This data source should be pointing to the database with the *pg_profile* extension installed. If your *pg_profile* extension is installed in its own schema make sure that the database user used in grafana data source has this schema in *search_path* setting.

## Grafana dashboard cotrols ##
Provided dashboards have some useful controls:
* **Database with pg_profile extension** can be used to change the current grafana data source if you have several pg_profile instances
* **Server** - the current server in pg_profile instance if your pg_profile instance have several servers defined
* **Server starts** - toggles grafana annotations showing PostgreSQL database restarts captured by pg_profile
* **Configuration loads** - toggles grafana annotations showing PostgreSQL configuration reloads captured by pg_profile
* **Interval** button is used to set dashboard time range that will cover all samples captured by pg_profile for the selected server server. Sometimes it should be clicked twice.
