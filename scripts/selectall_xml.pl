#!/usr/local/bin/perl -w

=head1 NAME 

selectall_xml.pl

=head1 SYNOPSIS

  selectall_xml.pl [-d <dbi>] [-f file of sql] [-nesting|n <nesting>] SQL

=head1 DESCRIPTION

This script will query a database using either SQL provided by the
script user, or using an SQL templates; the query results will be
turned into XML using the L<DBIx::DBStag> module. The nesting of the
XML can be controlled by the DBStag SQL extension "USE NESTING..."

=head2 EXAMPLES

  selectall_xml.pl -d "dbi:Pg:dbname=mydb;host=localhost"\
        "SELECT * FROM a NATURAL JOIN b"


=head2 TEMPLATES

A parameterized SQL template (canned query) can be used instead of
specifying the full SQL

For example:

  selectall_xml.pl -d genedb /genedb-gene gene_symbol=Adh

Or:

  selectall_xml.pl -d genedb /genedb-gene Adh 

A template is indicated by the syntactic shorthand of using a slash to
precede the template name; in this case the template is called
B<genedb-gene>. the -t option can also be used.

All the remaining arguments are passed in as SQL template
parameters. They can be passed in as either name=value pairs, or as a
simple list of arguments which get passed into the template in order

To use templates, you should have the environment variable
B<DBSTAG_TEMPLATE_DIRS> set. See B<DBIx::DBStag> for details.

=head1 ENVIRONMENT VARIABLES

=over

=item DBSTAG_DBIMAP_FILE

A file containing configuration details for local databases

=item DBSTAG_TEMPLATE_DIRS

list of directories (seperated by B<:>s) to be searched when templates
are requested

=back

=head1 COMMAND LINE ARGUMENTS

=over

=item -h|help

shows this page

=item -d|dbname DBNAME

this is either a full DBI locator string (eg
B<dbi:Pg:dbname=mydb;host=localhost>) or it can also be a shortened
"nickname", which is then looked up in the file pointed at by the
environment variable B<DBSTAG_DBIMAP_FILE>

=item -u|user USER

database user identity

=item -p|password PASS

database password

=item -f|file SQLFILE

this is a path to a file containing SQL that will be executed, as an
alternative to writing the SQL on the command line

=item -n|nesting NESTING-EXPRESSIONS

a bracketed expression indicating how to the resulting objects/XML
should be nested. See L<DBIx::DBStag> for details.

=item -t|template TEMPLATE-NAME

the name of a template; see above

=item -w|where WHERE-CLAUSE

used to override the WHERE clause of the query; useful for combining
with templates

=item -s|select SELECT-COLS

used to override the SELECT clause of the query; useful for combining
with templates

=item -rows

sometimes it is preferable to return the results as a table rather
than xml or a similar nested structure. specifying -rows will fetch a
table, one line per row, and columns seperated by tabs

=item -show

will show the parse of the SQL statement

=back


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
my $rows;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
	   "rows"=>\$rows,
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
    if ($rows) {
	my $ar =
	  $dbh->selectall_rows(@sel_args);
	foreach my $r (@$ar) {
	    printf "%s\n", join("\t", @$r);
	}
	$dbh->disconnect;	
	exit 0;
    }
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
