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
use vars qw(%IS_FORMAT_FLAT $cscheme);

%IS_FORMAT_FLAT =
  map {$_=>1} qw(flat-CSV flat-TSV flat-HTML-table);
$cscheme =
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
my $example_input = {};
my $options = {};
my $nesting = '';
my $rows;
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

my $format = $cgi->param('format') || 'sxpr';
my $dbname = $cgi->param('dbname');
if (@dbnames == 1) {
    $dbname = $dbnames[0];
}
if ($dbname) {
    setdb($dbname);
}
if ($cgi->param('template')) {
    settemplate($cgi->param('template'));
}
foreach (@$varnames) {
    my $v = $cgi->param("attr_$_");
    if ($v) {
	$v =~ s/\*/\%/g;
	$exec_argh{$_} = $v;
    }
}
if ($template && $cgi->param('submit') eq 'exectemplate') {
    conn();
    if ($cgi->param('where')) {
	$template->set_clause(where=>$cgi->param('where'));
    }
    if ($cgi->param('select')) {
	$template->set_clause(where=>$cgi->param('select'));
    }
    if (is_format_flat($format)) {
	$rows =
	  $dbh->selectall_rows(
			       -template=>$template,
			       -bind=>\%exec_argh,
			      );
    }
    else {
	$stag =
	  $dbh->selectall_stag(
			       -template=>$template,
			       -bind=>\%exec_argh,
			       -nesting=>$nesting,
			      );
    }
}

www($cgi);

sub g_title {
    "U * B * I * Q";
}

sub href {
    my $url = shift;
    my $n = shift || $url;
    "<a href=\"$url\">$n</a>";
}


sub hdr {
    (h1(g_title), 
     href("ubiq.cgi", "Ubiq"),
     ' | ',
     href("ubiq.cgi?help=1", "Help"),
     br,
     href('#templates', '>>Templates'),
    );

}

sub conn {
    $dbh = DBIx::DBStag->connect($dbname) unless $dbh;
}

sub query_results {
    (
     ($stag ? stag_detail($stag) : ''),
     ($rows ? rows_detail($rows) : ''),
    );
}

sub is_format_flat {
    my $f = shift;
    $IS_FORMAT_FLAT{$f};
}
sub keep {
    join('&',
	 map {"$_=".param(escapeHTML($_))} grep {param($_)} qw(dbname template format save mode));
}

sub template_chooser {
    my $templates = shift;
    my $KEEP = keep;
    return 
      table(Tr({-valign=>"TOP"},
	       [
		map {
		    my $is_selected = $_->name eq $template_name;
		    my $h = {};
		    if ($is_selected) {
			$h = {bgcolor=>'red'}
		    }
		    my $desc = $_->desc;
		    my $name = $_->name;
		    my $nl = "\n";
		    $desc =~ s/\n/\<br\>/gs;
		    td($h,
		       [
			href("#$name", '[scroll]'),
#			href("#$name", '[view]'),
			href(sprintf('ubiq.cgi?%s&template=%s', $KEEP, $name),
			     strong($name)),
			em($desc),
		       ])
		} @$templates,
		
	       ]));
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
    conn;
    $example_input = $template->get_example_input($dbh,
						  "./cache/cache-$dbname-$n",
						  1);
    $template_name = $n;
}
sub attr_settings {
    return unless $template;
    my @vals = ();
    my @popups = ();
    my @extra = ();

    my $basic_tbl = 
      table(Tr({},
	       [
		map {
		    my $examples = '';
		    my $ei = $example_input->{$_} || [];
		    while (length("@$ei") > 100) {
			pop @$ei;
		    }
		    if (@$ei) {
			$examples = "  Examples: ".em(join(', ', @$ei));
		    }
		    td([$_, textfield("attr_$_").$examples])
		} @$varnames
	       ]));
    my $adv_tbl =
      table(Tr({},
	       [td([
		    join(br,
			 "Override SQL SELECT:",
			 textarea(-name=>'select',
				  -cols=>80,
				 ),
			 "Override SQL WHERE:",
			 textarea(-name=>'where',
				  -cols=>80,
				 ),
			 "Override Full SQL Query:",
			 textarea(-name=>'sql',
				  -cols=>80,
				 ),
			 "Use nesting hierarchy:",
			 textarea(-name=>'nesting',
				  -cols=>80,
				 ),
			)
					
		   ])]));
      

    return 
      (
       hr,
       "Selected Template: ",
       strong($template_name),
       br,
       submit(-name=>'submit',
	      -value=>'exectemplate'),
       $basic_tbl,
       $adv_tbl,
#       table({-border=>1},
#	     Tr({-valign=>"TOP"},
#		[td([
		     
#		    ])])),

       ("Tree/Flat format: ",
	popup_menu(-name=>'format',
		   -values=>[qw(sxpr itext XML nested-HTML flat-TSV flat-CSV flat-HTML-table)]),
	checkbox(-name=>'save',
		 -value=>1,
		 -label=>'Save Results to Disk'),
       ),

       br,
       submit(-name=>'submit',
	      -value=>'exectemplate'),
       hr);
}

sub template_detail {
    my $templates = shift;
    my @tbls =
      map {
	  my $io = IO::String->new;
	  $_->show($io, $cscheme, \&htmlcolor);
	  my $sr = $io->string_ref;
	  ('<a name="'.$_->name.'"',
	   'template: ',
	   em($_->name),
	   table({-border=>1},
		 Tr(
		    [td(["<pre>$$sr</pre>"])])))
	} @$templates;
    return '<a name="templates">'.join("\n", @tbls);
}

sub stag_detail {
    my $stag = shift;
#    my $W = Data::Stag->getformathandler($format || 'sxpr');
#    $stag->events($W);
#    my $out = $W->popbuffer;
    my $out = $stag->generate(-fmt=>$format);
    return resultbox($out);
}

sub cell {
    my $cell = shift;
    if (!defined($cell)) {
	return '<font color="red">NULL</font>';
    }
    $cell;
}

sub rows_detail {
    my $rows = shift;
    if ($format eq 'flat-HTML-table') {
	my $hdr = shift @$rows;
	h2('Results').
	  table({-border=>1, -bgcolor=>'yellow'},
		Tr({},
		   [th([@$hdr]),
		    map {td([map {cell($_)} @$_])} @$rows]));
    }
    else {
	my $j = "\t";
	if ($format eq 'flat-CSV') {
	    $j = ',';
	}	
	my $out = join("\n",
		       map {
			   join($j,
				map {escape($_, ("\n"=>'\n', $j=>"\\$j"))} @$_)
		       } @$rows);
	resultbox($out);
    }
}
sub escape {
    my $s = shift;
    my %cmap = @_;
    $cmap{'\\'} = '\\\\';
    my @from = keys %cmap;
    my @to = map{$cmap{$_}} @from;
    my $f = join('', @from);
    my $t = join('', @to);
    $s =~ tr/$f/$t/;
    $s;
}

sub resultbox {
    my $out = shift;
    if (param('save')) {
	return $out;
    }
    h2('Results').
      table({-border=>1},
	    Tr({},
	       td({bgcolor=>"yellow"},["<pre>$out</pre>"])));
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


# ++++++++++++++++++++++++++++++++++++++++++++++++
#
# MAIN PAGE
#
# ++++++++++++++++++++++++++++++++++++++++++++++++

sub www {
    my $cgi = shift;

    if (param('save')) {
	print(header({-type=>"text/text"}),
	      query_results);
	return;
    }

    print(
	  header, 
	  start_html(g_title), 
	  hdr,
	  start_form(-action=>'ubiq.cgi', -method=>'GET'),
	  "Database",
	  popup_menu(-name=>'dbname',
		      -values=>[sort {$a cmp $b} @dbnames],
		      -onChange=>"submit()",
		    ),
	  submit(-name=>'submit',
		 -value=>"selectdb"),
	  " Query Constraint Mode: ",
	  popup_menu(-name=>'mode',
		     -values=>[qw(Basic Advanced Custom-SQL)],
		     -onChange=>'submit()'),
	  query_results,
	  attr_settings(),
	  ($template ? template_detail([$template]) : ''),
	  hr,
	  h3("Choose a template:"),
	  template_chooser($templates),
	  hr,
	  ($template ? '' : template_detail($templates)),
	  hidden('template', param('template')),
	  end_form,
	  );
	   
}
