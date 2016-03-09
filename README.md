# db-replicator
A simple system for data replication for PostgreSQL. Created for the [NWP project](http://www.roadstosafediscovery.com). Intended to be used with a single master db and read-only slaves.

### Usage
  1. Edit setup.ini file. Include your database connection info. Include the tables to be replicated as a space separated list. 
  2. Run setup.pl. Warning: This may overwrite existing tables, triggers, and trigger functions in your database. I would suggest reviewing the code before using.
  3. Edit replicator.ini.
  4. Run replicator.pl whenever fresh data is to be fetched for the slave. Can be triggered with Cron, or anything really.