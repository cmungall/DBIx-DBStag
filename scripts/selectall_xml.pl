#!/usr/local/bin/perl -w

=head1 NAME 

selectall_xml.pl

=head1 SYNOPSIS

  selectall_xml.pl -d "dbi:Pg:dbname=mydb;host=localhost" "SELECT * FROM a NATURAL JOIN b"

=head1 DESCRIPTION


=head1 ARGUMENTS

=cut



use strict;

use Carp;
use DBIx::DBStag;
use Getopt::Long;

my $debug;
my $help;
my $db;
my $nesting;
my $show;
my $file;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
           "show"=>\$show,
	   "nesting|n=s"=>\$nesting,
	   "file|f=s"=>\$file,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

my $dbh = 
  DBIx::DBStag->connect($db);
my $sql;
if ($file) {
    open(F, $file) || die $file;
    $sql = join('', <F>);
    close(F);
}
else {
    $sql = shift @ARGV;
}
if (!$sql) {
    print STDERR "Reading SQL from STDIN...\n";
    $sql = <STDIN>;
}
my $xml = $dbh->selectall_xml($sql, $nesting);
$dbh->disconnect;
if ($show) {
    print $dbh->last_stmt->xml;
}
print $xml;
