#!/usr/local/bin/perl -w

=head1 NAME 

stag-storenode.pl

=head1 SYNOPSIS

  stag-storenode.pl -d "dbi:Pg:dbname=mydb;host=localhost" myfile.xml

=head1 DESCRIPTION

This script is for storing data (specified in a nested file format
such as XML or S-Expressions) in a database. It assumes a database
schema corresponding to the tags in the input data already exists.

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



use strict;

use Carp;
use Data::Stag;
use DBIx::DBStag;
use Getopt::Long;

my $debug;
my $help;
my $db;
my $unit;
GetOptions(
           "help|h"=>\$help,
	   "db|d=s"=>\$db,
	   "unit|u=s"=>\$unit,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

#print STDERR "Connecting to $db\n";
my $dbh = DBIx::DBStag->connect($db);
$dbh->dbh->{AutoCommit} = 0;

foreach my $fn (@ARGV) {
    if ($unit) {
	my $H = Data::Stag->makehandler($unit=>sub {
					    my $self = shift;
					    my $stag = shift;
					    $dbh->begin_work;
					    $dbh->storenode($stag);
					    $dbh->commit;
					});
	Data::Stag->parse(-file=>$fn, -handler=>$H);
    }
    else {
	my $stag = Data::Stag->parse($fn);
	$dbh->storenode($stag);
	$dbh->commit;
    }
#    my @kids = $stag->kids;
#    foreach (@kids) {
#        $dbh->storenode($_);
#    }
}
