#!/usr/local/bin/perl -w

# POD docs at end

use strict;

use Carp;
use Data::Stag;
use DBIx::DBStag;
use Getopt::Long;

my $debug;
my $help;
my $db;
my @units;
my $parser;
my @mappings;
my $mapconf;
my @noupdate = ();
my $tracenode;
my $transform;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
	   "unit|u=s@"=>\@units,
           "parser|p=s"=>\$parser,
	   "mapping|m=s@"=>\@mappings,
	   "conf|c=s"=>\$mapconf,
           "noupdate=s@"=>\@noupdate,
           "tracenode=s"=>\$tracenode,
           "transform|t=s"=>\$transform,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

#print STDERR "Connecting to $db\n";
my $dbh = DBIx::DBStag->connect($db);
$dbh->dbh->{AutoCommit} = 0;

if ($mapconf) {
    $dbh->mapconf($mapconf);
}
if (@mappings) {
    $dbh->mapping(\@mappings);
}
@noupdate = map {split(/\,/,$_)} @noupdate;
$dbh->noupdate_h({map {$_=>1} @noupdate});
$dbh->tracenode($tracenode) if $tracenode;

sub store {
    my $self = shift;
    my $stag = shift;
    #$dbh->begin_work;
    $dbh->storenode($stag);
    $dbh->commit;
    return;
}

my $thandler;
if ($transform) {
    $thandler = Data::Stag->makehandler($transform);
}

foreach my $fn (@ARGV) {
    if ($fn eq '-' && !$parser) {
	$parser = 'xml';
    }
    if (@units) {
        my $H;
	my $storehandler = Data::Stag->makehandler(
                                                   map {
                                                       $_ =>sub{store(@_)};
                                                   } @units
                                                  );
        if ($thandler) {
            $H = Data::Stag->chainhandlers([@units],
                                           $thandler,
                                           $storehandler);
        }
        else {
            $H = $storehandler;
        }
        Data::Stag->parse(-format=>$parser,-file=>$fn, -handler=>$H);
    }
    else {
        print STDERR "WARNING! Slurping whole file into memory may be inefficient; consider -u\n";
        my $stag;
        my @pargs = (-format=>$parser,-file=>$fn);
        push(@pargs, -handler=>$thandler) if $thandler;
	$stag = Data::Stag->parse(@pargs);
	$dbh->storenode($stag);
	$dbh->commit;
    }
#    my @kids = $stag->kids;
#    foreach (@kids) {
#        $dbh->storenode($_);
#    }
}
$dbh->disconnect;
exit 0;

__END__

=head1 NAME 

stag-storenode.pl

=head1 SYNOPSIS

  stag-storenode.pl -d "dbi:Pg:dbname=mydb;host=localhost" myfile.xml

=head1 DESCRIPTION

This script is for storing data (specified in a nested file format
such as XML or S-Expressions) in a database. It assumes a database
schema corresponding to the tags in the input data already exists.

=head2 ARGUMENTS

=head3 -d B<DBNAME>

This is either a DBI locator or the logical name of a database in the
DBSTAG_DBIMAP_FILE config file

=head3 -u B<UNIT>

This is the node/element name on which to load; a database loading
event will be fired every time one of these elements is parsed; this
also constitutes a whole transaction

=head3 -c B<STAGMAPFILE>

This is a stag mapping file, indicating which elements are aliases

=head3 -p B<PARSER>

Default is xml; can be any stag compatible parser, OR a perl module
which will parse the input file and fire stag events (see
L<Data::Stag::BaseGenerator>)

=head3 -t B<TRANSFORMER>

This is the name of a perl module that will perform a transformation
on the stag events/XML. See also L<stag-handle.pl>

=head3 -noupdate B<NODELIST>

A comma-seperated (no spaces) list of nodes/elements on which no
update should be performed if a unique key is found to be present in
the DB

=head1 MAKING DATABASE FROM XML FILES

It is possible to automatically generate a database schema and
populate it directly from XML files (or from Stag objects or other
Stag compatible files). Of course, this is no substitute for proper
relational design, but often it can be necessary to quickly generate
databases from heterogeneous XML data sources, for the purposes of
data mining.

There are 3 steps involved:

1. Prepare the input XML (for instance, modifying db reserved words).
2. Autogenerate the CREATE TABLE statements, and make a db from these.
3. Store the XML data in the database.

=head2 Step 1: Prepare input file

You may need to make modifications to your XML before it can be used
to make a schema. If your XML elements contain any words that are
reserved by your DB you should change these.

Any XML processing tool (eg XSLT) can be used. Alternatively you can
use the script 'stag-mogrify'

e.g. to get rid of '-' characters (this is how Stag treates
attributes) and to change the element with postgresql reserved word
'date', do this:

  stag-mogrify.pl -xml -r 's/^date$/moddate/' -r 's/\-//g' data.xml > data.mog.xml

You may also need to explicitly make elements where you will need
linking tables. For instance, if the relationship between 'movie' and
'star' is many-to-many, and your input data looks like this:

  (movie
   (name "star wars")
   (star
    (name "mark hamill")))

You will need to *interpose* an element between these two, like this:

  (movie
   (name "star wars")
   (movie2star
    (star
     (name "mark hamill"))))

you can do this with the -i switch:

  stag-mogrify.pl -xml -i movie,star,movie2star data.xml > data.mog.xml

or if you simply do:

  stag-mogrify.pl -xml -i star data.xml > data.mog.xml

the mogrifier will simply interpose an element above every time it
sees 'star'; the naming rule is to use the two elements with an
underscore between (in this case, 'movie_star').

=head2 Step 2: Generating CREATE TABLE statements

Use the stag-autoddl.pl script;

  stag-autoddl.pl data.mog.xml > table.sql

The default rule is to create foreign keys from the nested element to
the outer element; you will want linking tables tobe treated
differently (a linking table will point to parent and child elements).

  stag-autoddl.pl -l movie2star -l star2character data.mog.xml > table.sql

Once you have done this, load the statements into your db; eg for postgresql
(for other databases, use L<SQL::Translator>)

  psql -a mydb < table.sql

If something goes wrong, go back to step 1 and sort it out!

Note that certain rules are followed: ever table generated gets a
surrogate primary key of type 'serial'; this is used to generate
foreign key relationships. The rule used is primary and foreign key
names are the name of the table with the '_id' suffix.

Feel free to modify the autogenerated schema at this stage (eg add
uniqueness constraints)

=head2 Step 3: Store the data in the db

  stag-storenode.pl -u movie -d 'dbi:Pg:mydb' data.mog.xml

You generally dont need extra metadata here; everything can be
infered by introspecting the database.

The -u|unit switch controls when transactions are committed

If this works, you should now be able to retreive XML from the database, eg

  selectall_xml.pl -d 'dbi:Pg:mydb' 'SELECT * FROM x NATURAL JOIN y'

=cut


