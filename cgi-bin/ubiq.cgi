#!/usr/local/bin/perl -w

BEGIN{
    eval{do "dbenv.pl"};
    die $@ if $@;
}

use strict;
use lib split(/:/, $ENV{STAGLIB} || '');

use IO::String;
use DBIx::DBStag;
use CGI qw/:standard/;

my $cscheme =
  {
   'keyword'=>'cyan',
   'variable'=>'magenta',
   'text' => 'reset',
   'comment' => 'red',
   'block' => 'blue',
   'property' => 'green',
  };

my $cgi = CGI->new;

my $sdbh = 
  DBIx::DBStag->new;

# child dbh
my $dbh;

my $stag;
my $res;
my $schema;
my $loc;
my $templates = [];
my $varnames = [];
my $options = {};
my $nesting;
my $rows = [];
my $template;
my $template_name = '';
my %exec_argh = ();
my $resources = $sdbh->resources_list;
my $resources_hash = $sdbh->resources_hash;
my @dbresl = grep {$_->{type} eq 'rdb'} @$resources;
my @dbnames = (map {$_->{name}} @dbresl);
my $W = Data::Stag->getformathandler('sxpr');
my $ofh = \*STDOUT;

do "dbconf.pl";
die $@ if $@;

my $dbname = $cgi->param('dbname');
if (@dbnames == 1) {
    $dbname = $dbnames[0];
}
if ($dbname) {
    setdb($dbname);
}
if ($cgi->param('submit') eq 'selecttemplate' &&
    $cgi->param('template')) {
    settemplate($cgi->param('template'));
}
foreach (@$varnames) {
    my $v = $cgi->param("attr_$_");
    if ($v) {
	$exec_argh{$_} = $v;
    }
}
if ($template && $cgi->param('submit') eq 'exectemplate') {
    $dbh = DBIx::DBStag->connect($dbname);
    $stag =
      $dbh->selectall_stag(
			   -template=>$template,
			   -bind=>\%exec_argh,
			   -nesting=>$nesting,
			  );
}

www($cgi);

sub g_title {
    "U B I Q"
}

sub www {
    my $cgi = shift;

    print(
	  header, 
	  start_html(g_title), 
	  h1(g_title), 
	  start_form(-action=>'ubiq.cgi'),
	  "Database",
	  popup_menu(-name=>'dbname',
		      -values=>[sort {$a cmp $b} @dbnames],
		      -onChange=>"submit()",
		    ),
	  submit(-name=>'submit',
		 -value=>"selectdb"),
	  hr,
	  attr_settings(),
	  ($stag ? stag_detail($stag) : ''),
	  ($template ? template_detail([$template]) : ''),
	  hr,
	  h3("Choose a template:"),
	  template_summary($templates),
	  submit(-name=>'submit',
		 -value=>"selecttemplate"),
	  hr,
	  ($template ? '' : template_detail($templates)),
	  );
	   
}
sub template_summary {
    my $templates = shift;
    my %labels =
      map {
	  ($_->name => $_->name)
      } @$templates;
    return
      radio_group(-name=>'template',
		  -values=>[values %labels],
		  -labels=>\%labels,
		  -linebreak=>'true',
		  -columns=>4,
		  -onChange=>"submit(selecttemplate)",
		 );
}

sub setdb {
    $dbname = shift;
    msg("Set dbname to $dbname");
    $res = $resources_hash->{$dbname};
    if ($res) {
	$schema = $res->{schema} || '';
	$loc = $res->{loc} || '';
	msg("loc: $loc") if $loc;
	if ($schema) {
	    $templates = $sdbh->find_templates_by_schema($schema);
	    msg("schema: $schema");
	}
	else {
	    msg("schema not known; templates unrestricted");
	    $templates = $sdbh->template_list;
	}
	msg("Templates available: " . scalar(@$templates));
    }
    else {
	warnmsg("Unknown $dbname");
    }
    $res;
}
sub settemplate {
    my $n = shift;
    my @matches = grep {$_->name eq $n} @$templates;
    die unless @matches == 1;
    $template = shift @matches;
    $varnames = $template->get_varnames;
    $template_name = $n;
}
sub attr_settings {
    return unless $template;
    my @vals = ();
    my @popups = ();
    my $t =
      table(Tr({},
	       [
		map {td([$_, textfield("attr_$_")])} @$varnames
	       ]));
    
    return $t .
      submit(-name=>'submit',
	     -value=>'exectemplate');
}

sub template_detail {
    my $templates = shift;
    my @tbls =
      map {
	  my $io = IO::String->new;
	  $_->show($io, $cscheme, \&htmlcolor);
	  my $sr = $io->string_ref;
	  table({-border=>1},
		Tr(
		   [td(["<pre>$$sr</pre>"])]))
      } @$templates;
    return join("\n", @tbls);
}

sub stag_detail {
    my $stag = shift;
    my $out = $stag->sxpr;
    table({-border=>1},
	  Tr({},
	     td(["<pre>$out</pre>"])));
}

sub msg {
}


sub htmlcolor {
    my $c = shift;
    if ($c eq 'reset') {
	'</font>';
    }
    else {
	"<font color=\"$c\">";
    }
}
