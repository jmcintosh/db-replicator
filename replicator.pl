#!/usr/bin/perl
use Config::Simple;
use DBI;
use strict;
use warnings;

# read config file
my %config;
Config::Simple->import_from('replicator.ini', \%config);

# connect to local slave database
my $slave_db = connectToDatabase(
    "DBI:Pg:dbname=".$config{"slave.dbname"}.
    ";host=".$config{"slave.host"}.
    ";port=".$config{"slave.port"},
    $config{"slave.user"},
    $config{"slave.password"}
);
#print a line if DB connection is established.
my $ping = $slave_db->ping;
print "slave db ping = $ping\n";

# connect to master database
my $master_db = connectToDatabase(
    "DBI:Pg:dbname=".$config{"master.dbname"}.
    ";host=".$config{"master.host"}.
    ";port=".$config{"master.port"},
    $config{"master.user"},
    $config{"master.password"}
);
#print a line if DB connection is established.
$ping = $master_db->ping;
print "master db ping = $ping\n";
my $master_schema = $config{"master.schema"};

my $initialized = $config{"slave.initialized"};
if(!(defined $initialized)){
    $initialized = 0;
}
if($initialized != 1){
    # create master_table_update table
    print "initialize slave db";
    $slave_db->do(
        "CREATE TABLE IF NOT EXISTS $master_schema.master_table_update
        (
            master_table character varying(48) NOT NULL,
            updated_at timestamp with time zone,
            CONSTRAINT table_update_pkey PRIMARY KEY (master_table)
        )"
    ) or die $slave_db->errstr;

    # populate table
    my $query = $master_db->prepare(
        "SELECT master_table FROM $master_schema.master_table_update"
    );
    $query->execute() or die $slave_db->errstr;
    my @row = $query->fetchrow_array();
    if(!@row){
        die "no tables found in master DB\n";
    }
    my $insert = $slave_db->prepare(
        "INSERT INTO $master_schema.master_table_update
        (master_table) VALUES (?)"
    );
    while(@row){
        my $table = $row[0];
        $insert->execute($table);
        @row = $query->fetchrow_array();
    }

    $slave_db->commit();
    my $ini = new Config::Simple('replicator.ini') or die Config::Simple->error();
    $ini->param("slave.initialized","1");
    $ini->save();
}


my $query = $slave_db->prepare(
    "SELECT * FROM $master_schema.master_table_update"
);
$query->execute();
my @row = $query->fetchrow_array();
if(!@row){
    die "no tables found in slave DB\n";
}
while(@row){
    my $table = $row[0];
    my $slave_timestamp = $row[1];
    if(!(defined $slave_timestamp)) {
        $slave_timestamp = "";
    }
    
    my $query_is_updated = $master_db->prepare(
        "SELECT updated_at 
        FROM $master_schema.master_table_update 
        WHERE master_table = ? LIMIT 1"
    );

    $query_is_updated->execute($table);
    my @response = $query_is_updated->fetchrow_array();
    if(@response){
        my $master_timestamp = $response[0];
        if(!(defined $master_timestamp)) {
            $master_timestamp = "";
        }
        if($slave_timestamp ne $master_timestamp){
            print "timestamps are not the same, update table $table\n";
            # update the event table
            updateTable($table);
            
            # update the slave update table
            my $query_update = $slave_db->prepare(
                "UPDATE $master_schema.master_table_update 
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
    
    @row = $query->fetchrow_array();
}

# disconnect from databases
$master_db->disconnect();
$slave_db->disconnect();
print "disconnected from databases, done.\n";

sub updateTable{
    my $table = $_[0];
    # clear data from the slave table
    my $query_clear_table = $slave_db->do("TRUNCATE $table;");
    
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
}

sub connectToDatabase{
    my $data_source = $_[0];
    my $user = $_[1];
    my $password = $_[2];

    my $dbh = DBI->connect(
        $data_source,
        $user,
        $password
    ) or die "Connection Error: $DBI::errstr\n";
    $dbh->{AutoCommit} = 0;
    $dbh->{RaiseError} = 1;
    return $dbh;
}