#!/usr/local/bin/perl -w

=head1 NAME 

selectall_xml.pl

=head1 SYNOPSIS

  selectall_xml.pl [-d <dbi>] [-f file of sql] [-nesting|n <nesting>] SQL

=head1 DESCRIPTION

Example:
  selectall_xml.pl -d "dbi:Pg:dbname=mydb;host=localhost"\
        "SELECT * FROM a NATURAL JOIN b"


=head1 ARGUMENTS

=cut



use strict;

use Carp;
use DBIx::DBStag;
use Data::Dumper;
use Getopt::Long;

my $debug;
my $help;
my $db;
my $nesting;
my $show;
my $file;
my $user;
my $pass;
my $template;
my $where;
my $select;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
           "show"=>\$show,
	   "nesting|n=s"=>\$nesting,
	   "file|f=s"=>\$file,
	   "user|u=s"=>\$user,
	   "pass|p=s"=>\$pass,
	   "template|t=s"=>\$template,
	   "where|w=s"=>\$where,
	   "select|s=s"=>\$select,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

my $dbh = 
  DBIx::DBStag->connect($db, $user, $pass);
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
if ($sql =~ /^\/(.*)/) {
    # shorthand for a template
    $template = $1;
    $sql = '';
}
my $xml;
my @sel_args = ($sql, $nesting);
if ($template) {
    $template =
      DBIx::DBStag->new->find_template($template);
    if ($where) {
	$template->set_clause(where => $where);
    }
    if ($select) {
	$template->set_clause(select => $select);
    }

    my @args = ();
    my %argh = ();
    while (my $arg = shift @ARGV) {
	if ($arg =~ /(.*)=(.*)/) {
	    $argh{$1} = $2;
	}
	else {
	    push(@args, $arg);
	}
    }
    my $bind = \@args;
    if (%argh) {
	$bind = \%argh;
	if (@args) {
	    die("can't used mixed argument passing");
	}
    }
    @sel_args =
      ($template, $nesting, $bind);
}
eval {
    $xml = $dbh->selectall_xml(@sel_args);
};
if ($@) {
    print "FAILED\n$@";
}

$dbh->disconnect;
if ($show) {
    print $dbh->last_stmt->xml;
}
print $xml;
