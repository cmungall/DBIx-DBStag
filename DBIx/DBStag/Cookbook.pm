
=head1 NAME

  DBIx::DBStag::Cookbook - building and querying databases from XML

=head1 SYNOPSIS

  stag-autoddl.pl
  stag-storenode.pl
  selectall_xml.pl  

=head1 DESCRIPTION

This will give an outline of how to build a relational database from
XML, set up stag templates, issue relational queries that return
hierarchical results as XML or objects, and autogenerate a web query
front end for this data.

The dataset we will use is the CIA World Factbook.

The web interface should end up looking something like this -
L<http://www.godatabase.org/cgi-bin/wfb/ubiq.cgi>

=head2 AUTOGENERATING A RELATIONAL DATABASE

Download CIA world factbook in XML format; see
L<http://www.informatik.uni-freiburg.de/~may/Mondial/>

Download the file B<cia.xml>

process XML:

  stag-mogrify.pl -w xml -r 's/text$/quant/' -r 's/id$/ciaid/' -r 's/(.*)\-//' cia.xml > cia-pp.xml

Generate the SQL B<CREATE TABLE> statements

  stag-autoddl.pl -t cia-pp2.xml cia-pp.xml > cia-schema.sql

This does further post-processing of the XML, to make it suitable for
relational storage; see B<cia-pp2.xml>

Load the database (the following instructions assume you have
postgresql on your localhost; please consult your DBMS manual if this
is not the case)

  createdb cia
  psql -a -e cia < cia-schema.sql >& create.log

Load the data

  stag-storenode.pl -d dbi:Pg:dbname=cia cia-pp2.xml >& load.log

=head2 FETCHING TREE DATA USING SQL

Query the data using the stag query shell (qsh). The following can be
cut and pasted directly:

  stag-qsh -d cia
  \l
  # get a tree rooted at 'country' object
  select * from country inner join country_coasts using (country_id)
  where country.name = 'France';
  
  # find countries straggling borders
  select c1.*, c2.*
  from country AS c1 
           inner join borders on (c1.country_id = borders.country_id)
           inner join country AS c2 on (borders.country=c2.ciaid)
  where c1.continent != c2.continent
  order by c1.name, c2.name
  use nesting (set(c1(c2)));
  \q

See L<DBIx::DBStag> for more details on fetching hierarchical data
from relational database

=head2 USING TEMPLATES

First create a place for your templates:

  mkdir ./templates
  setenv DBSTAG_TEMPLATE_DIRS ".:templates:/usr/local/share/sql/templates"

Auto-generate templates (you can customize these later):

  stag-autoschema.pl -w sxpr cia-pp2.xml > cia-stagschema.sxpr
  stag-autotemplate.pl -no_pp -s cia -dir ./templates  cia-stagschema.sxpr

You may wish to examine a template:

  more templates/cia-country.stg

now execute a template from the command line:

  selectall_xml.pl -d cia /cia-country country_name=Austria

You should see something similar to:

  <set>
    <country>
      <country_id>3</country_id>
      <government>federal republic</government>
      <population>8023244</population>
      <total_area>83850</total_area>
      <name>Austria</name>
      <inflation>2.3</inflation>
      ...
      <languages>
        <languages_id>1</languages_id>
        <name>German</name>
        <num>100</num>
        <country_id>3</country_id>
      </languages>
      ...

We can do this interactively using qsh.

First, we need to inform stag-qsh what the schema is. The schema is
used to determine which templates are appropriate. Later we will
discover how to set up a resources file, which will allow stag to
infer the schema.

Call qsh from command line:

  stag-qsh -d cia -s cia

Interactive perl/qsh:
  
  \l
  t cia-country
  /borders_country=cid-cia-Germany

The above should fetch all countries bordering Germany

If we prefer objects to hierarchical syntax such as XML, we can do
this in perl. Still in qsh, type the following:

  $dataset =
    $dbh->selectall_stag(-template=>$template,-bind=>{languages_name=>'Spanish'});
  @lcountry = $dataset->get_country;
  foreach $country (@lcountry) { 
    printf("Country: %s\n  Religions:%s\n",
           $country->sget_name,
           join(', ', 
                map {
                     $_->get_name.' '.$_->get_num
                } $country->get_religions))
  }
  ;
  \q

See L<Data::Stag> for more details on using Stag objects

=head2 BUILDING A CGI/WEB INTERFACE

We can construct a generic but powerful default cgi interface for our
data, using ubiq.cgi, which should come with your distribution. You
may have to modify some of the directories below.

We want to create the CGI, and give it access to our templates:

  mkdir /usr/local/httpd/cgi-bin/cia
  cp templates/*.stg /usr/local/httpd/cgi-bin/cia
  cp $HOME/DBIx-DBStag/cgi-bin/ubiq.cgi /usr/local/httpd/cgi-bin/cia
  chmod +x /usr/local/httpd/cgi-bin/cia/ubiq.cgi

Set up the environment for the CGI script. It must be able to see the
templates and the necessary perl libraries (if not installed
system-wide)

  cat > /usr/local/httpd/cgi-bin/cia/dbenv.pl
  $ENV{DBSTAG_DBIMAP_FILE} = "./resources.conf";
  $ENV{DBSTAG_TEMPLATE_DIRS} = ".:./templates:/data/bioconf/templates";
  $ENV{STAGLIB} = "/users/me/lib/DBIx-DBStag:/users/me/lib/stag";

We must create a basic resources file, currently containing one db:

  cat > /usr/local/httpd/cgi-bin/cia/resources.conf
  cia              rdb               Pg:cia        schema=cia

You should be able to use the interface via http://localhost/cgi-bin/cia/ubiq.cgi

You can customize this by overriding some of the existing display functions;

  cat > /usr/local/httpd/cgi-bin/cia/ubiq-customize.pl
  # --- CUSTOM SETTINGS
  {
   no warnings 'redefine';
   
   *g_title = sub {
       "U * B * I * Q - CIA World Factbook";
   };
   *short_intro = sub {
       "Demo interface to CIA World Factbook"
   };
   add_initfunc(sub {
  		   $dbname = 'cia';
  		   $schema = 'cia';
  	       });
  }


From here on you can customise the web interface, create new
templates, integrate this with other data. Consult L<DBIx::DBStag> and
the script B<ubiq.cgi> for further details.

=head1 WEBSITE

L<http://stag.sourceforge.net>

=head1 AUTHOR

Chris Mungall 

  cjm at fruitfly dot org

=head1 COPYRIGHT

Copyright (c) 2002 Chris Mungall

This module is free software.
You may distribute this module under the same terms as perl itself

=cut
