#!/usr/local/bin/perl -w

BEGIN{
    eval{do "dbenv.pl"};
    die $@ if $@;
};   # end of sub: 

use strict;
use lib split(/:/, $ENV{STAGLIB} || '');

use IO::String;
use DBIx::DBStag;
use CGI qw/:standard/;
use vars qw(%IS_FORMAT_FLAT $cscheme);

# --------------------------
# MAIN
ubiq();
exit 0;
# --------------------------


# ++++++++++++++++++++++++++++++++++++++++++++++++++
# ubiq
#
# This is the core function. It does everything
# ++++++++++++++++++++++++++++++++++++++++++++++++++
sub ubiq {

    # =============================================
    # DECLARE VARIABLES
    # note: the functions below are lexically closed
    #       and can thus access these variables.
    #
    # if you're not familiar with closures you might
    # find this a bit confusing...
    # =============================================
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
    my $format;
    my $dbname;


    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # keep
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub keep;			#
    *keep = sub {
	join('&',
	     map {"$_=".param(escapeHTML($_))} grep {param($_)} qw(dbname template format save mode));
    };				# end of sub: keep


    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # conn
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub conn;			#
    *conn = sub {
	$dbh = DBIx::DBStag->connect($dbname) unless $dbh;
    };				# end of sub: conn

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # is_format_flat
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub is_format_flat;		#
    *is_format_flat = sub {
	#	my $f = shift;
	$IS_FORMAT_FLAT{$format};
    };				# end of sub: is_format_flat



    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    #
    # BASIC LAYOUT
    #
    # headers, footers, help, etc
    #
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # g_title
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub g_title;		#
    *g_title = sub {
	"U * B * I * Q";
    };				# end of sub: g_title

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # short_intro
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub short_intro;		#
    *short_intro = sub {
	"This is the generic UBIQ interface";
    };				# end of sub: short_intro

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # top_of_page
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub top_of_page;			#
    *top_of_page = sub {
	(h1(g_title), 
	 href("ubiq.cgi", "Ubiq"),
	 ' | ',
	 href("ubiq.cgi?help=1", "Help"),
	 br,
	 href('#templates', '>>Templates'),
	 br,
	 short_intro,
	 hr,
	);

    };				# end of sub: top_of_page

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # footer
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub footer;			#
    *footer = sub {
	(hr,
	 href('http://stag.sourceforge.net'),
	 br,
	 myfont('$Id: ubiq.cgi,v 1.5 2003/08/04 02:22:22 cmungall Exp $', (size=>-2)),
	);
    };				# end of sub: footer

    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    #
    # VIEW WIDGETS
    #
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # template_detail
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub template_detail;	#
    *template_detail = sub {
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
    };				# end of sub: template_detail

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # stag_detail
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub stag_detail;		#
    *stag_detail = sub {
	#    my $W = Data::Stag->getformathandler($format || 'sxpr');
	#    $stag->events($W);
	#    my $out = $W->popbuffer;
	my $out = $stag->generate(-fmt=>$format);
	return resultbox($out);
    };				# end of sub: stag_detail

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # rows_detail
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub rows_detail;		#
    *rows_detail = sub {
	if ($format eq 'flat-HTML-table') {
	    my $hdr = shift @$rows;
	    h2('Results').
	      table({-border=>1, -bgcolor=>'yellow'},
		    Tr({},
		       [th([@$hdr]),
			map {td([map {colval2cell($_)} @$_])} @$rows]));
	} else {
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
    };				# end of sub: rows_detail

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # query_results
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub query_results;		#
    *query_results = sub {
	(
	 ($stag ? stag_detail() : ''),
	 ($rows ? rows_detail() : ''),
	);
    };				# end of sub: query_results

    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    #
    # CHOOSERS
    #
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # template_chooser
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub template_chooser;	#
    *template_chooser = sub {
	#my $templates = shift;
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
    };				# end of sub: template_chooser

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # attr_settings
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub attr_settings;		#
    *attr_settings = sub {
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
    };				# end of sub: attr_settings

    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    #
    # SETTERS
    #
    #  these set variables depending on users selections
    #
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # setdb
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub setdb;			#
    *setdb = sub {
	#$dbname = shift;
	return unless $dbname;
	msg("Set dbname to $dbname");
	$res = $resources_hash->{$dbname};
	if ($res) {
	    $schema = $res->{schema} || '';
	    $loc = $res->{loc} || '';
	    msg("loc: $loc") if $loc;
	    if ($schema) {
		$templates = $sdbh->find_templates_by_schema($schema);
		msg("schema: $schema");
	    } else {
		msg("schema not known; templates unrestricted");
		$templates = $sdbh->template_list;
	    }
	    msg("Templates available: " . scalar(@$templates));
	} else {
	    warnmsg("Unknown $dbname");
	}
	$res;
    };				# end of sub: setdb
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # settemplate
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub settemplate;		#
    *settemplate = sub ($) {
	my $n = shift;
	my @matches = grep {$_->name eq $n} @$templates;
	die "looking for $n, got @matches" unless @matches == 1;
	$template = shift @matches;
	$varnames = $template->get_varnames;
	conn;
	$example_input = $template->get_example_input($dbh,
						      "./cache/cache-$dbname-$n",
						      1);
	$template_name = $n;
    };				# end of sub: settemplate

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # resultbox
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub resultbox;		#
    *resultbox = sub {
	my $out = shift;
	if (param('save')) {
	    return $out;
	}
	h2('Results').
	  table({-border=>1},
		Tr({},
		   td({bgcolor=>"yellow"},["<pre>$out</pre>"])));
    };				# end of sub: resultbox

    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # msg
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub msg;			#
    *msg = sub {
    };				# end of sub: msg


    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # htmlcolor
    #
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub htmlcolor;		#
    *htmlcolor = sub {
	my $c = shift;
	if ($c eq 'reset') {
	    '</font>';
	} else {
	    "<font color=\"$c\">";
	}
    };				# end of sub: htmlcolor


    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    # display_htmlpage
    #
    # MAIN PAGE
    #
    # ++++++++++++++++++++++++++++++++++++++++++++++++++
    sub display_htmlpage;			#
    *display_htmlpage = sub {
	print(
	      header, 
	      start_html(g_title), 
	      top_of_page,
	      start_form(-action=>'ubiq.cgi', -method=>'GET'),

	      # DATABASE SELECTION
	      ("Database",
	       popup_menu(-name=>'dbname',
			  -values=>[sort {$a cmp $b} @dbnames],
			  -onChange=>"submit()",
			 ),
	       submit(-name=>'submit',
		      -value=>"selectdb")),

	      # QUERY RESULTS - if present
	      (query_results),

	      # ATTRIBUTE CHOOSER - if template is set
	      (attr_settings(),
	       ($template ? template_detail([$template]) : ''),
	       hr),

	      # TEMPLATE CHOOSER
	      (h3("Choose a template:"),
	       template_chooser,
	       hr),

	      # TEMPLATES - all or just selected
	      ($template ? '' : template_detail($templates)),

	      # PERSISTENT VARS
	      hidden('template', param('template')),

	      end_form,
	      footer,
	     );
    };				# end of sub: display_htmlpage

    # ================================
    #
    # SETTING THINGS UP
    #
    # ================================

    my @initfuncs = ();

    *add_initfunc = sub {
	push(@initfuncs, shift);
    };

    add_initfunc(sub {
		     $format = param('format') || 'sxpr';
		     $dbname = param('dbname');
		     if (@dbnames == 1) {
			 # only one to choose from; autoselect
			 $dbname = $dbnames[0];
		     }
		     
		     setdb;                # sets $dbh

		     # sets $template $varnames
		     settemplate(param('template'))
		       if param('template');
		     
		     # set variable bindings
		     foreach (@$varnames) {
			 my $v = param("attr_$_");
			 if ($v) {
			     $v =~ s/\*/\%/g;
			     $exec_argh{$_} = $v;
			 }
		     }
		 });

    if (-f 'ubiq-customize.pl') {
	eval `cat ubiq-customize.pl`;
	die $@ if $@;
    }


    $_->() foreach @initfuncs;

    # execute query
    if ($template && param('submit') eq 'exectemplate') {
	conn();
	if (param('where')) {
	    $template->set_clause(where=>param('where'));
	}
	if (param('select')) {
	    $template->set_clause(where=>param('select'));
	}
	if (is_format_flat) {
	    $rows =
	      $dbh->selectall_rows(
				   -template=>$template,
				   -bind=>\%exec_argh,
				  );
	} else {
	    $stag =
	      $dbh->selectall_stag(
				   -template=>$template,
				   -bind=>\%exec_argh,
				   -nesting=>$nesting,
				  );
	}
    }

    # WRITE HTML TO BROWSER
    if (param('save')) {
	# WRITE TO FILE
	print(header({-type=>"text/text"}),
	      query_results);
    }
    else {
	# WRITE HTML
	display_htmlpage;

    }

}

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#
# CGI UTILITY FUNCTIONS
#
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# ++++++++++++++++++++++++++++++++++++++++++++++++++
# href
#
#
# ++++++++++++++++++++++++++++++++++++++++++++++++++
sub href ($) {
    my $url = shift;
    my $n = shift || $url;
    "<a href=\"$url\">$n</a>";
}				# end of sub: href

# ++++++++++++++++++++++++++++++++++++++++++++++++++
# myfont
#
#
# ++++++++++++++++++++++++++++++++++++++++++++++++++
sub myfont ($%) {
    my $str = shift;
    my %h = @_;
    sprintf("<font %s>$str</font>",
	    join(' ',
		 map {sprintf('%s="%s"',
			      $_, $h{$_})} keys %h));
}				# end of sub: myfont

# ++++++++++++++++++++++++++++++++++++++++++++++++++
# escape
#
#   escapes characters using a map
# ++++++++++++++++++++++++++++++++++++++++++++++++++
sub escape ($@) {
    my $s = shift || '';
    my %cmap = @_;
    $cmap{'\\'} = '\\\\';
    my @from = keys %cmap;
    my @to = map{$cmap{$_}} @from;
    my $f = join('', @from);
    my $t = join('', @to);
    $s =~ tr/$f/$t/;
    $s;
}				# end of sub: escape


# ++++++++++++++++++++++++++++++++++++++++++++++++++
# colval2cell
#
#
# ++++++++++++++++++++++++++++++++++++++++++++++++++
sub colval2cell ($) {
    my $cell = shift;
    if (!defined($cell)) {
	return '<font color="red">NULL</font>';
    }
    $cell;
}				# end of sub: colval2cell

