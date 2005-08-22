#!/usr/local/bin/perl

# cjm@fruitfly.org

# currently assumes Pg

use strict;
use Carp;
use DBIx::DBStag;
use Data::Stag qw(:all);
use Data::Dumper;
use Getopt::Long;

my $h = {};

my $dbname = '';
my $connect;
my $term;
my @hist = ();

GetOptions(
           "dbname|d=s"=>\$dbname,
           "connect|c"=>\$connect,
          );

my $db = shift;
# parent dbh
my $sdbh = 
  DBIx::DBStag->new;

my $resource = $sdbh->resources_hash->{$db};
my $pstr = '';
if ($resource) {
    my $loc = $resource->{loc};
    if ($loc =~ /(\w+):(\S+)\@(\S+)/) {
        $pstr = "-h $3 $2";
    }
    if (!$pstr) {
        print STDERR "Could not resolve: $db [from $loc]\n";
        exit 1;
    }
}
else {
    print STDERR "No such resource: $db\n";
    exit 1;
}

print $pstr;
exit 0;
