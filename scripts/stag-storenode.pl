#!/usr/local/bin/perl -w

=head1 NAME 

stag-storenode.pl

=head1 SYNOPSIS

  stag-storenode.pl -d "dbi:Pg:dbname=mydb;host=localhost" myfile.xml

=head1 DESCRIPTION


=head1 ARGUMENTS

=cut



use strict;

use Carp;
use Data::Stag;
use DBIx::DBStag;
use Getopt::Long;

my $debug;
my $help;
my $db;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

my $dbh = 
  DBIx::DBStag->connect($db);
foreach my $fn (@ARGV) {
    my $stag = Data::Stag->parse($fn);
    $dbh->storenode($stag);
#    my @kids = $stag->kids;
#    foreach (@kids) {
#        $dbh->storenode($_);
#    }
}
