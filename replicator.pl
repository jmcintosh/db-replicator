#!/usr/bin/perl
use Config::Simple;
use DBI;
use strict;
use warnings;

###### read config file ######
my %config;
Config::Simple->import_from('replicator.ini', \%config);

###### connect to local slave database #######
my $slave_data_source = 
    "DBI:Pg:dbname=".$config{"slave.dbname"}.
    ";host=".$config{"slave.host"}.
    ";port=".$config{"slave.port"};

my $slave_db = DBI->connect(
    $slave_data_source,
    $config{"slave.user"},
    $config{"slave.password"}
) or die "Connection Error: $DBI::errstr\n";
$slave_db->{AutoCommit} = 0;
$slave_db->{RaiseError} = 1;
#print a line if DB connection is established.
my $ping=$slave_db->ping;
print "slave db ping = $ping\n";

###### connect to master database #######
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
$ping=$master_db->ping;
print "master db ping = $ping\n";

my %slave_table_updates;
my %master_table_updates;


my $query = $slave_db->prepare("SELECT * FROM master_table_update");
$query->execute();
my @row = $query->fetchrow_array();
if(!@row){
    die "no tables found in slave DB\n";
}
while(@row){
    my $table = $row[0];
    my $slave_timestamp = $row[1];
    
    my $query_is_updated = $master_db->prepare(
        "SELECT updated_at 
        FROM master_table_update 
        WHERE master_table = ? LIMIT 1"
    );

    $query_is_updated->execute($table);
    my @response = $query_is_updated->fetchrow_array();
    if(@response){
        my $master_timestamp = $response[0];
        if($slave_timestamp ne $master_timestamp){
            print "timestamps are not the same, update table $table\n";
            # update the event table
            updateTable($table);
            
            # update the slave update table
            my $query_update = $slave_db->prepare(
                "UPDATE master_table_update 
                SET updated_at = ? 
                WHERE master_table = ?"
            );

            $query_update->execute($master_timestamp, $table);
            
            $slave_db->commit();
        }else{
            print "timestamps are the same, do not update table $table\n";
        }
        
    }else{
        print "record for table $table not found in master DB\n";
    }
    $query_is_updated->finish();
    
    @row = $query->fetchrow_array();
}
$query->finish;

# disconnect from databases
$master_db->disconnect();
$slave_db->disconnect();
print "disconnected from databases, done.\n";

sub updateTable{
    my $table = $_[0];
    # clear data from the slave table
    my $query_clear_table = $slave_db->prepare("TRUNCATE $table;");
    $query_clear_table->execute();
    $query_clear_table->finish();
    
    # get data from the master db
    my $query_get_data = $master_db->prepare("SELECT * FROM $table;");
    $query_get_data->execute();
    my @values = $query_get_data->fetchrow_array;
    if(!@values){
        print "no data in table $table\n";
    }
    my $placeholders = join ',' , ('?') x @values;
    my $query_put_data = $slave_db->prepare(
        "INSERT INTO $table VALUES ($placeholders);"
    );
    while(@values){
        # insert data into slave db
        $query_put_data->execute(@values);
        
        @values = $query_get_data->fetchrow_array;
    }
    $query_put_data->finish();
    $query_get_data->finish();
    
}
