#!/usr/bin/perl
use Config::Simple;
use strict;
use DBI;
use warnings;

# read config file
my %config;
Config::Simple->import_from('setup.ini', \%config);

# connect to master database
my $master_data_source = 
    "DBI:Pg:dbname=".$config{"master.dbname"}.
    ";host=".$config{"master.host"}.
    ";port=".$config{"master.port"};
my $master_db = DBI->connect(
    $master_data_source,
    $config{"master.user"},
    $config{"master.password"}
) or die "Connection Error: $DBI::errstr\n";
$master_db->{AutoCommit} = 0;
$master_db->{RaiseError} = 1;
#print a line if DB connection is established.
my $ping=$master_db->ping;
print "master db ping = $ping\n";


my $schema = $config{"master.schema"};

# create master_table_update table
$master_db->do(
    "CREATE TABLE IF NOT EXISTS $schema.master_table_update
    (
      master_table character varying(48) NOT NULL,
      updated_at timestamp with time zone,
      CONSTRAINT table_update_pkey PRIMARY KEY (master_table)
    )"
) or die $master_db->errstr;
# truncate the table in case it already exists and is populated to avoid collisions
$master_db->do("TRUNCATE $schema.master_table_update");
$master_db->commit();

my @tables = split(" ", $config{"master.tables"});

my $insert_query = $master_db->prepare(
    "INSERT INTO $schema.master_table_update
        (master_table,updated_at) VALUES (?,current_timestamp)"
);

for my $table (@tables) {

    print "adding table $table\n";
    # add table to master_table_update
    $insert_query->execute($schema.".".$table) or die $master_db->errstr;

    # create trigger function
    my $trigger_function = $schema.".f_".$table."_update";
    $master_db->do(
        "CREATE OR REPLACE FUNCTION $trigger_function()
        RETURNS trigger AS
        \$BODY\$
            BEGIN
                UPDATE $schema.master_table_update 
                SET updated_at = current_timestamp
                WHERE master_table = '$schema.$table';
                RETURN NULL;
            END;
        \$BODY\$
        LANGUAGE plpgsql VOLATILE
        COST 100;"
    ) or die $master_db->errstr;

    # create trigger
    my $trigger_name = "tr_".$table."_update";
    $master_db->do(
        "DROP TRIGGER IF EXISTS $trigger_name ON $schema.$table;"
    );
    $master_db->do(
        "CREATE TRIGGER $trigger_name
        AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
        ON $schema.$table
        FOR EACH STATEMENT
        EXECUTE PROCEDURE $trigger_function();"
    ) or die $master_db->errstr;

    
}

$master_db->commit();
$master_db->disconnect();
print "disconnected from databases, done.\n";