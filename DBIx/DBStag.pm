# $Id: DBStag.pm,v 1.36 2004/09/30 19:20:58 cmungall Exp $
# -------------------------------------------------------
#
# Copyright (C) 2002 Chris Mungall <cjm@fruitfly.org>
#
# This module is free software.
# You may distribute this module under the same terms as perl itself

#---
# POD docs at end of file
#---

package DBIx::DBStag;


use strict;
use vars qw($VERSION @ISA @EXPORT_OK %EXPORT_TAGS $DEBUG $AUTOLOAD);
use Carp;
use DBI;
use Data::Stag qw(:all);
use DBIx::DBSchema;
use Text::Balanced qw(extract_bracketed);
#use SQL::Statement;
use Parse::RecDescent;
$VERSION="0.05";


our $DEBUG;

sub DEBUG {
    $DEBUG = shift if @_;
    return $DEBUG;
}

sub trace {
    my ($priority, @msg) = @_;
    return unless $ENV{DBSTAG_TRACE};
    print STDERR "@msg\n";
}

sub dmp {
    use Data::Dumper;
    print Dumper shift;
}

sub force {
    my $self = shift;
    $self->{_force} = shift if @_;
    return $self->{_force};
}


sub new {
    my $proto = shift; 
    my $class = ref($proto) || $proto;
    my ($dbh) = 
      rearrange([qw(dbh)], @_);

    my $self = {};
    bless $self, $class;
    if ($dbh) {
	$self->dbh($dbh);
    }
    $self;
}


sub connect {
    my $class = shift;
    my $dbi = shift;
    my $self;
    if (ref($class)) {
        $self = $class;
    }
    else {
        $self = {};
        bless $self, $class;
    }
    $dbi = $self->resolve_dbi($dbi);
    eval {
	$self->dbh(DBI->connect($dbi, @_));
    };
    if ($@ || !$self->dbh) {
	my $mapf = $ENV{DBSTAG_DBIMAP_FILE};
        if ($dbi =~ /^dbi:(\w+)/) {
            print STDERR <<EOM

Could not connect to database: "$dbi"

EITHER   The required DBD driver "$1" is not installed
    OR   There is no such database as "$dbi"

EOM
              ;
        }
        else {
            print STDERR <<EOM

Could not connect to database: "$dbi"

To connect to a database, you need to set the environment variable
DBSTAG_DBIMAP_FILE to the location of your DBI Stag resources file, OR
you need to specify the full dbi string of the database

A dbi string looks like this:

 dbi:Pg:dbname=foo;host=mypgserver.foo.com

A resources file provides mappings from logical names like "foo" to
full DBI locators suchas the one above

Please type "man DBI" for more information on DBI strings

If you are specifying a valid DBI locator or valid logical name and
still connect, check the database server is responding

EOM
              ;
        }
        exit 1;
    }
    # HACK
    $self->dbh->{RaiseError} = 1;
    $self->dbh->{ShowErrorStatement} = 1;
    if ($dbi =~ /dbi:(\w+)/) {
        $self->{_driver} = $1;
    }
    $self->setup;
    return $self;
}

sub resolve_dbi {
    my $self = shift;
    my $dbi = shift;
    if (!$dbi) {
	$self->throw("database name not provided!");
    }
    if ($dbi !~ /^dbi:/) {
	my $rh = $self->resources_hash;
	my $res = 
	  $rh->{$dbi};
	if (!$res) {
	    if ($dbi =~ /:/) {
		$res =
		  {loc=>"$dbi"}
	    }
	    else {
		$res =
		  {loc=>"Pg:$dbi"};
	    }
	}
	if ($res) {
	    my $loc = $res->{loc};
	    if ($loc =~ /(\w+):(\S+)\@(\S+)/) {
		my $dbms = $1;
		my $dbn = $2;
		my $host = $3;
		$dbi = "dbi:$dbms:database=$dbn:host=$host";
		if ($dbms =~ /pg/i) {
		    $dbi = "dbi:Pg:dbname=$dbn;host=$host";
		}
	    } 
	    elsif ($loc =~ /(\w+):(\S+)$/) {
		my $dbms = $1;
		my $dbn = $2;
		$dbi = "dbi:$dbms:database=$dbn";
		if ($dbms =~ /pg/i) {
		    $dbi = "dbi:Pg:dbname=$dbn";
		}
	    } 
	    else {
		$self->throw("$dbi -> $loc does not conform to standard.\n".
			     "<DBMS>:<DB>\@<HOST>");
	    }
	}
	else {
	    $self->throw("$dbi is not a valid DBI locator.\n");
	}
    }
    return $dbi;
}

sub resources_hash {
    my $self = shift;
    my $mapf = $ENV{DBSTAG_DBIMAP_FILE};
    my $rh;
    if ($mapf) {
	if (-f $mapf) {
	    $rh = {};
	    open(F, $mapf) || $self->throw("Cannot open $mapf");
	    while (<F>) {
		chomp;
		next if /^\#/;
		s/^\!//;
		my @parts =split(' ', $_);
		next unless (@parts >= 3);
		my ($name, $type, $loc, $tagstr) =@parts;
		my %tagh = ();
		if ($tagstr) {
		    my @parts = split(/;\s*/, $tagstr);
		    foreach (@parts) {
			my ($t, $v) = split(/\s*=\s*/, $_);
			$tagh{$t} = $v;
		    }
		}
		$rh->{$name} =
		  {
		   %tagh,
		   name=>$name,
		   type=>$type,
		   loc=>$loc,
		   tagstr=>$tagstr,
		  };
	    }
	    close(F) || $self->throw("Cannot close $mapf");
	} else {
	    $self->throw("$mapf does not exist");
	}
    }
    return $rh;
}


sub resources_list {
    my $self = shift;
    my $rh =
      $self->resources_hash;
    my $rl;
    if ($rh) {
	$rl =
	  [map {$_} values %$rh];
    }
    return $rl;
}

sub find_template {
    my $self = shift;
    my $tname = shift;
    my $path = $ENV{DBSTAG_TEMPLATE_DIRS} || '';
    my $tl = $self->template_list;
    my ($template, @rest) = grep {$tname eq $_->name} @$tl;

    if (!$template) {
	print STDERR "\n\nI could not find the Stag SQL template called \"$tname\".\n";
	if (!$path) {
	    print STDERR <<EOM1

In order to do use this or any other template, you need to set the environment
variable DBSTAG_TEMPLATE_DIRS to the directory or a set of directories
containing SQL templates. For example

  setenv DBSTAG_TEMPLATE_DIRS=".:\$HOME/my-sql-templates:/usr/share/system-sql-templates"

EOM1
;
	}
	else {
	    print STDERR <<EOM2

I am looking in the following directories:

  $path

Check the contents of the directory to see if the stag sql template
you require is there, and is readable by you. Stag SQL templates
should end with the suffix ".stg"

If you wish to search other directories, set the environment variable
DBSTAG_TEMPLATE_DIRS, like this:

  setenv DBSTAG_TEMPLATE_DIRS=".:\$HOME/my-sql-templates:$path"

EOM2
;
	}
	$self->throw("Could not find template \"$tname\" in: $path");
    }
    return $template;
}

sub find_templates_by_schema {
    my $self = shift;
    my $schema = shift;
    my $tl = $self->template_list;

    my @templates = grep {$_->stag_props->tmatch('schema', $schema)} @$tl;
    
    return \@templates;
}

sub find_templates_by_dbname {
    my $self = shift;
    my $dbname = shift;
    my $res = $self->resources_hash->{$dbname};
    my $templates;
    if ($res) {
	my $schema = $res->{schema} || '';
	if ($schema) {
	    $templates = $self->find_templates_by_schema($schema);
	}
	else {
	    # unknown schema - show all templates
#	    $templates = $self->template_list;
	}
    }
    else {
	$self->throw("unknown db: $dbname");
    }
    return $templates;
}

sub template_list {
    my $self = shift;
    my %already_got = ();
    if (!$self->{_template_list}) {
        my $path = $ENV{DBSTAG_TEMPLATE_DIRS} || '.';
        my @dirs = split(/:/, $path);
        my @templates = ();

        foreach my $dir (@dirs) {
            foreach my $fn (glob("$dir/*.stg")) {
                if (-f $fn) {
                    require "DBIx/DBStag/SQLTemplate.pm";
                    my $template = DBIx::DBStag::SQLTemplate->new;
                    $template->parse($fn);
                    push(@templates, $template) unless $already_got{$template->name};
		    $already_got{$template->name} = 1;
                }
            }
        }
        $self->{_template_list} = \@templates;
    }
    return $self->{_template_list};
}

sub find_schema {
    my $self = shift;
    my $dbname = shift;
    my $rl = $self->resouces_list || [];
    my ($r) = grep {$_->{name} eq $_ ||
		      $_->{loc} eq $_} @$rl;
    if ($r) {
	return $r->{schema};
    }
    return;
}

sub setup {
    my $self = shift;
    return;
}

# counter
sub next_id {
    my $self = shift;
    $self->{_next_id} = shift if @_;
    $self->{_next_id} = 0 unless $self->{_next_id};
    return ++$self->{_next_id};
}


sub dbh {
    my $self = shift;
    $self->{_dbh} = shift if @_;
    return $self->{_dbh};
}

sub dbschema {
    my $self = shift;
    $self->{_dbschema} = shift if @_;
    if (!$self->{_dbschema}) {
        if (!$self->dbh) {
            confess("you must establish connection using connect() first");
        }
        $self->dbschema(DBIx::DBSchema->new_native($self->dbh));
#	my $sth = $self->dbh->table_info(undef, undef, undef, 'VIEW') or die $self->dbh->errstr;
#	use Data::Dumper;
#	print Dumper $sth->fetchall_arrayref([2,3]);
    }
    return $self->{_dbschema};
}

sub parser {
    my $self = shift;
    $self->{_parser} = shift if @_;
    if (!$self->{_parser}) {
	$self->{_parser} = Parse::RecDescent->new($self->selectgrammar());
    }
    return $self->{_parser};
}

sub warn {
    my $self = shift;
    my $p = shift;
    my $fmt = shift;

    print STDERR "\nWARNING:\n";
    printf STDERR $fmt, @_;
    print STDERR "\n";
}

sub throw {
    my $self = shift;
    my $fmt = shift;

    print STDERR "\nERROR:\n";
    printf STDERR $fmt, @_;
    print STDERR "\n";
    confess;
}

sub get_pk_col {
    my $self = shift;
    my $table = shift;
    
    my $tableobj = $self->dbschema->table(lc($table));
    if (!$tableobj) {
        confess("Can't get table $table from db.\n".
                "Maybe DBIx::DBSchema does not work with your database?");
    }
    return $tableobj->primary_key;
}

sub get_all_cols {
    my $self = shift;
    my $table = shift;
    
    my $tableobj = $self->dbschema->table(lc($table));
    if (!$tableobj) {
        confess("Can't get table $table from db.\n".
                "Maybe DBIx::DBSchema does not work with your database?");
    }
    return $tableobj->columns;
}

sub get_unique_sets {
    my $self = shift;
    my $table = shift;

    my $tableobj = $self->dbschema->table(lc($table));
    if (!$tableobj) {
        confess("Can't get table $table from db.\n".
                "Maybe DBIx::DBSchema does not work with your database?");
    }
    return @{$tableobj->unique->lol_ref || []};
}

sub mapconf {
    my $self = shift;
    my $fn = shift;
    my $fh = FileHandle->new($fn) || confess("cannot open $fn");
    my @mappings = <$fh>;
    $fh->close;
    $self->mapping(\@mappings);
}

sub mapping {
    my $self = shift;
    if (@_) {
	my $ml = shift;
	my @nu =
	  map {
	      if (ref($_)) {
		  Data::Stag->nodify($_);
	      }
	      else {
		  if (/^(\w+)\/(\w+)\.(\w+)=(\w+)\.(\w+)/) {
		      Data::Stag->new(map=>[
					    [fktable_alias=>$1],
					    [table=>$2],
					    [col=>$3],
					    [fktable=>$4],
					    [fkcol=>$5]
					   ]);
		  }
		  elsif (/^(\w+)\.(\w+)=(\w+)\.(\w+)/) {
		      Data::Stag->new(map=>[
					    [table=>$1],
					    [col=>$2],
					    [fktable=>$3],
					    [fkcol=>$4]
					   ]);
		  }
		  else {
		      confess("incorrectly specified mapping: $_".
			      "(must be alias/tbl.col=ftbl.fcol)");
		      ();
		  }
	      }
	  } @$ml;
	$self->{_mapping} = \@nu;
    }
    return $self->{_mapping};
}

sub guess_mapping {
    my $self = shift;
    my $dbschema = $self->dbschema;

    $self->mapping([]);
    my %th =
      map { $_ => $dbschema->table($_) } $dbschema->tables;
    foreach my $tn (keys %th) {
        my @cns = $th{$tn}->columns;
        foreach my $cn (@cns) {
            my $ftn = $cn;
            $ftn =~ s/_id$//;
            if ($th{$ftn}) {
                push(@{$self->mapping},
                     Data::Stag->new(map=>[
                                           [table=>$tn],
                                           [col=>$cn],
                                           [fktable=>$ftn],
                                           [fkcol=>$cn]
                                          ]));
            }
        }
    }
}

sub linking_tables {
    my $self = shift;
    $self->{_linking_tables} = {@_} if @_;
    return %{$self->{_linking_tables} || {}};
}

sub add_linking_tables {
    my $self = shift;
    my %linkh = $self->linking_tables;
    return unless %linkh;
    my $struct = shift;
    foreach my $ltname (keys %linkh) {
        my ($t1, $t2) = @{$linkh{$ltname}};
        $struct->where($t1,
                       sub {
                           my $n=shift;
                           my @v = $n->getnode($t2);
                           return unless @v;
                           $n->unset($t2);
                           my @nv =
                             map {
                                 $n->new($ltname=>[$_]);
                             } @v;
#                           $n->setnode($ltname,
#                                       $n->new($ltname=>[@v]));
                           foreach (@nv) {
                               $n->addkid($_);
                           }
                           0;
                       });
    }
    return;
}

# ----------------------------------------

sub elt_card {
    my $e = shift;
    my $c = '';
    if ($e =~ /(.*)([\+\?\*])/) {
        ($e, $c) = ($1, $2);
    }
    # make the element RDB-safe
    $e =~ s/\-//g;
    return ($e, $c);
}

sub source_transforms {
    my $self = shift;
    $self->{_source_transforms} = shift if @_;
    return $self->{_source_transforms};
}

sub autotemplate {
    my $self = shift;
    my $schema = shift;
    return () unless grep {!stag_isterminal($_)} $schema->subnodes;
    my @J = ();
    my @W = ();
    my @EXAMPLE = ();
    my ($tname) = elt_card($schema->element);
    my %joinpaths = ();
    
    $schema->iterate(sub {
			 my $n = shift;
			 my $parent = shift;
			 my ($tbl, $card) = elt_card($n->element);
			 if (!$parent) {
			     push(@J, $tbl);
#			     $joinpaths{$tbl} = $tbl;
			     return;
			 }
			 my ($ptbl) = elt_card($parent->element);
			 if (stag_isterminal($n)) {
			     my $v = $ptbl.'_'.$tbl;
			     my $w = "$ptbl.$tbl => \&$v\&";
			     if ($ptbl eq $tname) {
				 push(@W,
				      "[ $w ]");
			     }
			     else {
				 my $pk = $tname.'_id';
				 my $subselect = 
				   "SELECT $pk FROM $joinpaths{$ptbl}".
				     " WHERE $w";
				 push(@W,
				      "[ $pk IN ($subselect) ]");
			     }
			     # produce example formula for non-ints
			     if ($n->data eq 's') {
				 push(@EXAMPLE,
				      "$v => SELECT DISTINCT $tbl FROM $ptbl");
			     }
			 }
			 else  {
			     my $jtype = 'INNER JOIN';
			     if ($card eq '*' || $card eq '?') {
				 $jtype = 'LEFT OUTER JOIN';
			     }
			     my $jcol = $ptbl.'_id';
			     push(@J,
				  "$jtype $tbl USING ($jcol)");
			     if ($joinpaths{$ptbl}) {
				 $joinpaths{$tbl} =
				   "$joinpaths{$ptbl} INNER JOIN $tbl USING ($jcol)";
			     }
			     else {
				 $joinpaths{$tbl} = $tbl;
			     }
			 }
			 return;
		     });
    my $from = join("\n  ", @J);
    my $where = join("\n  ", @W);
    my $nesting = $schema->duplicate;
    $nesting->iterate(sub {
			  my $n = shift;
			  if (stag_isterminal($n)) {
			      return;
			  }
			  my ($tbl, $card) = elt_card($n->element);
			  $n->element($tbl);
			  my @sn = $n->kids;
			  @sn =
			    grep {
				my ($tbl, $card) = elt_card($_->element);
				$_->element($tbl);
				!stag_isterminal($_)
			    } @sn;
			  if (@sn) {
			      $n->kids(@sn);
			  }
			  else {
			      $n->data([]);
			  }
		      });
    $nesting = Data::Stag->new(set=>[$nesting]);
    my $nstr = $nesting->sxpr;
    $nstr =~ s/^\'//;
    my $tt =
      join("\n",
	   ":SELECT *",
	   ":FROM $from",
	   ":WHERE $where",
	   ":USE NESTING",
	   "$nstr",
	   "",
	   "// ---- METADATA ----",
	   "schema:",
	   "desc: Fetches $tname objects",
	   "      This is an AUTOGENERATED template",
	   "",
	   (map {
	       "example_input: $_"
	   } @EXAMPLE),
	  );

#    my $template = DBIx::DBStag::SQLTemplate->new;
    my @sn = $schema->subnodes;
    my @tts = ();
    push(@tts, $self->autotemplate($_)) foreach @sn;
    return ([$tname=>$tt], @tts);
}

sub autoddl {
    my $self = shift;
    my $stag = shift;
    my $link = shift;
    $stag->makeattrsnodes;
    my $schema = $stag->autoschema;
    $self->source_transforms([]);;
    $self->_autoddl($schema, undef, $link);
}

sub _autoddl {
    my $self = shift;
    my $schema = shift;
    my $parent = shift;
    my $link = shift || [];   # link tables
    my $tbls = shift || [];
    my @sn = $schema->subnodes;
    my ($tbl, $card) = elt_card($schema->element);
    my @cols = (sprintf("%s_id serial PRIMARY KEY NOT NULL", $tbl));
    my $casc = " ON DELETE CASCADE";
    foreach (grep {stag_isterminal($_)} @sn) {
        my ($col, $card) = elt_card($_->element);
        my $pk = '';
        if ($col eq $tbl.'_id') {
            shift @cols;            
            $pk = ' PRIMARY KEY';
        }
        if ($card =~ /[\+\*]/) {
	    my $new_name =  sprintf("%s_%s", $tbl, $col);
	    my $tf = ["$tbl/$col", "$new_name/$col"];
	    push(@{$self->source_transforms}, $tf);
	    $_->name($new_name);
	    $_->data([[$col => $_->data]]);
#	    $self->throw("In the source data, '$col' is a multivalued\n".
#			 "terminal (data) node. This is difficult to transform");
        }
        else {
#            my $isnull = $card eq '?' ? '' : ' NOT NULL';
            my $isnull = '';
            push(@cols,
                 sprintf("%s %s$isnull$pk",
                         $col, $_->data));
        }
    }
    if ($parent) {
        my ($pn) = elt_card($parent->element);
        push(@cols,
             sprintf("%s_id INT", $pn));
        push(@cols,
             sprintf("FOREIGN KEY (%s_id) REFERENCES $pn(%s_id)$casc", $pn, $pn));
    }

    my $mapping = $self->mapping || [];

    if (grep {$_ eq $tbl} @$tbls) {
#	$self->throw("$tbl has >1 parent - you need to\n".
#		     "transform input data");
	return "";
    }
    push(@$tbls, $tbl);

    my $post_ddl = '';
    my $pre_ddl = '';
    foreach (grep {!stag_isterminal($_)} @sn) {
        # check for cases where we want to include FK to subnode
        my ($map) =
          grep { 
              $_->get_table eq $tbl &&
                ($_->get_fktable_alias eq $_->element ||
                 $_->get_fktable eq $_->element)
              } @$mapping;
	# linking tables
        if ($map ||
            grep {$_ eq $tbl} @$link) {
            my $ftbl = $_->element;
            push(@cols,
                 sprintf("%s_id INT", $ftbl));
            push(@cols,
                 sprintf("FOREIGN KEY (%s_id) REFERENCES $ftbl(%s_id)$casc", $ftbl, $ftbl));
            $pre_ddl .= $self->_autoddl($_, undef, $link, $tbls);
            
        }
        else {
            $post_ddl .= $self->_autoddl($_, $schema, $link, $tbls);
        }

    }
    my $ddl = 
      sprintf("CREATE TABLE $tbl (\n%s\n);\n\n",
              join(",\n", map {"    $_"} @cols));

    return $pre_ddl . $ddl . $post_ddl;;
}

# ----------------------------------------
# CACHE METHODS
#
# we keep a cache of what is stored in
# each table
#
# cache->{$element}->{$key}->{$val}
# ----------------------------------------

# list of table names that should be cached
sub cached_tables {
    my $self = shift;
    $self->{_cached_tables} = shift if @_;
    return $self->{_cached_tables};
}

sub is_caching_on {
    my $self = shift;
    my $element  = shift;
    $self->{_is_caching_on} = {}
      unless $self->{_is_caching_on};
    if (@_) {
        $self->{_is_caching_on}->{$element} = shift;
    }
    return $self->{_is_caching_on}->{$element};
}

sub query_cache {
    my $self = shift;
    my $element = shift;
    my $constr = shift;
    my $update_h = shift;
    my @keycols = sort keys %$constr;
    my $cache = $self->get_tuple_idx($element, \@keycols);
    my $valstr = join("\t", map {$constr->{$_}} @keycols);
#    use Data::Dumper;
#    print Dumper $cache;
    if ($update_h) {
        my $current_h = $cache->{$valstr} || {};
        $current_h->{$_} = $update_h->{$_} foreach keys %$update_h;
        $cache->{$valstr} = $current_h;
    }
    return $cache->{$valstr};
}

sub insert_into_cache {
    my $self = shift;
    my $element = shift;
    my $insert_h = shift;
    my $usets = shift;
    foreach my $uset (@$usets) {
        my @undef = grep {!defined $insert_h->{$_}} @$uset;
        if (@undef) {
            my @defined = grep {defined $insert_h->{$_}} @$uset;
            trace(1, 
                  "undefined column in unique key: @$uset IN $element/[@$uset] ".
                  join('; ', 
                       map {"$_=$insert_h->{$_}"} @defined,
                      )
                 );
            # cannot cache undefined values
            next;
        }
        my $cache = $self->get_tuple_idx($element, $uset);
        my $valstr = join("\t", map {$insert_h->{$_}} sort @$uset);
        $cache->{$valstr} = $insert_h;
    }
    return 1;
}

sub update_cache {
    my $self = shift;
    my $element = shift;
    my $store_hash = shift;
    my $unique_constr = shift;

    my $tuple = $self->query_cache($element,
                                   $unique_constr,
                                   $store_hash);
    return;
}

sub get_tuple_idx {
    my $self = shift;
    my $element = shift;
    my @keycols = sort @{shift || []} || $self->throw;
    
    my $cache = $self->cache;
    if (!$cache->{$element}) {
        $cache->{$element} = {};
    }
    my $eltcache = $cache->{$element};
    # we just use a flat perl hash - flatten the list of unique cols
    # to a string with spaces between
    my $k = "@keycols";
    if (!$eltcache->{$k}) {
        $eltcache->{$k} = {};
    }
    
    return $eltcache->{$k};    
}

sub cache {
    my $self = shift;
    $self->{_cache} = shift if @_;
    $self->{_cache} = {} unless $self->{_cache};
    return $self->{_cache};
}

# ---- END OF CACHE METHODS ----

# set this if we are loading a fresh/blank slate DB
# (will assume database is empty and not check for
#  existing tuples)
sub policy_freshbulkload {
    my $self = shift;
    $self->{_policy_freshbulkload} = shift if @_;
    return $self->{_policy_freshbulkload};
}
sub noupdate_h {
    my $self = shift;
    $self->{_noupdate_h} = shift if @_;
    return $self->{_noupdate_h} || {};
}
sub tracenode {
    my $self = shift;
    $self->{_tracenode} = shift if @_;
    return $self->{_tracenode};
}

sub mapgroups {
    my $self = shift;
    if (@_) {
        $self->{_mapgroups} = [@_];
        $self->{_colvalmap} = {}
          unless $self->{_colvalmap};
        foreach my $cols (@_) {
            my $h = {};
            foreach (@$cols) {
                $self->{_colvalmap}->{$_} = $h;
            }
        }
    }
    return @{$self->{_mapgroups} || []};
}

# DEPRECATED
sub get_mapping_for_col {
    my $self = shift;
    my $col = shift;
    $self->{_colvalmap}->{$col} = {} 
      unless $self->{_colvalmap}->{$col};
    return $self->{_colvalmap}->{$col};
}

# mapping of Old ID => New ID
# IDs are assumed to be global across ALL tables
sub id_remap_idx {
    my $self = shift;
    if (@_) {
        $self->{_id_remap_idx} = shift;
    }
    else {
        $self->{_id_remap_idx} = {}
          unless $self->{_id_remap_idx};
    }
    return $self->{_id_remap_idx};
}

sub trust_primary_key_values {
    my $self = shift;
    $self->{_trust_primary_key_values} = shift if @_;
    return $self->{_trust_primary_key_values};
}


sub make_stag_node_dbsafe {
    my $self = shift;
    my $node = shift;
    my $name = $node->name;
    my $safename = $self->dbsafe($name);
    if ($name ne $safename) {
	$node->name($safename);
    }
    my @kids = $node->kids;
    foreach (@kids) {
	$self->make_stag_node_dbsafe($_) if ref $_;
    }
    return;
}
sub dbsafe {
    my $self = shift;
    my $name = shift;
    $name = lc($name);
    $name =~ tr/a-z0-9_//cd;
    return $name;
}

#'(t1
#  (foo x)
#  (t2
#   (bar y)))
#
# '(fk
#   (table t2)
#   (ftable t1))
#   
# alg: store t1, then t2

#  '(t1
#    (foo x)
#    (t1_t2
#     (t2
#      (bar y))))
#
# '(fk
#   (table t1_t2)
#   (ftable t1))
# '(fk
#   (table t1_t2)
#   (ftable t2))
#
# 
# alg: store t1, hold on t1_t2, store t2

#   '(t1
#     (foo x)
#     (blah
#      (t2
#       (bar y))))
#
# '(fk
#   (table t1)
#   (fktable t2)
#   (fktable_alias "blah")
#   (fk  "blah_id")) 

# alg: store t2, store t1

# recursively stores a Data::Stag tree node in the database
sub storenode {
    my $self = shift;
    my $node = shift;
    my @args = @_;
    my $dupnode = $node->duplicate;
    $self->make_stag_node_dbsafe($dupnode);
    $self->add_linking_tables($dupnode);
    $self->_storenode($dupnode,@args);
}
sub _storenode {
    my $self = shift;
    my $node = shift;
    if (!$node) {
        confess("you need to pass in a node!");
    }
    my $element = $node->element;
    if ($element eq 'dbstag_metadata') {
        my @maps = $node->get_map;
        $self->mapping(\@maps);
        my @links = $node->get_link;
        if (@links) {
            my %h =
              map {
                  ($_->sget_table => [$_->sget_from, $_->sget_to])
              } @links;
            $self->linking_tables(%h);
        }
        return;
    }
    trace(0, "STORING $element\n", $node->xml);
    my $tracenode = $self->tracenode || '';
    my $tracekeyval;
    if ($tracenode && $tracenode =~ /^(\w+)\/(.*)/) {
        my $nn = $1;
        my $tag = $2;
        if ($nn eq $element) {
            $tracekeyval = $node->get($tag);
        }
    }

    my $dbh = $self->dbh;
    my $dbschema = $self->dbschema;

    my $is_caching_on = $self->is_caching_on($element) || 0;

    my $mapping = $self->mapping || [];

    # each relation has zero or one primary keys;
    # primary keys are assumed to be single-column
    my $pkcol = $self->get_pk_col($element);
    trace(0, "PKCOL: $pkcol");

    # -- PRE-STORE CHILD NON-TERMINALS --
    # before storing this node, we need to
    # see if we first need to store any child
    # non-terminal nodes (in order to get their
    # primary keys, to use as foreign keys in
    # the current relation)

    # store kids first
    my @ntnodes = $node->ntnodes;
    my @delayed_store = ();
    foreach my $nt (@ntnodes) {
        # we want to PRE-STORE any ntnodes that
        # are required for foreign key relationships
        # within this node;
        # ie this node N1 has a foreign key "fk_id" that
        # points to ntnode N2.
        # if there is an intermediate alias element in
        # between then we need to store the ntnode too
        #
        # check for either of these conditions
        my ($map) =
          grep { 
              $_->get_table && 
                $_->get_table eq $element &&
                ($_->get_fktable_alias && 
                 $_->get_fktable_alias eq $nt->element ||
                 ($_->get_fktable && 
                  $_->get_fktable eq $nt->element && !$_->get_fktable_alias))
            } @$mapping;
        # check to see if sub-element has FK to this element
        if (!$map) {
#            my $subtable = $dbschema->table($nt->element);
            my $table = $dbschema->table($element);
            my $ntelement = $nt->element;
            my $subpkcol = $self->get_pk_col($ntelement);
            
            my @cns = $table->columns;

            my $cn;    # col name (FK in current element)
            my $fcn;   # foreign col name (PK in sub element)

            # HACK - ASSUME NATURAL JOIN
            # for example, a FK: person.dept_id => dept.dept_id
            if ($subpkcol ne 'id') {
                foreach (@cns) {
                    if ($_ eq $subpkcol) {
                        $cn = $_;
                        $fcn = $_;
                    }
                }
            }

            # second chance; allow base "id" style
            # for example, a FK: person.dept_id => dept.id
            # via <person><dept>...</dept></person>
            if (!$cn) {
                if ($subpkcol eq 'id') {
                    foreach (@cns) {
                        if ($_ eq $ntelement."_id") {
                            $cn = $_;
                            $fcn = 'id';
                        }
                    }
                }
            }
            if ($cn) {
                $map =
                  Data::Stag->new(map=>[
                                        [table=>$element],
                                        [col=>$cn],
                                        [fktable=>$nt->element],
                                        [fkcol=>$fcn]
                                       ]);
            }
        }

        # if $map is set, then we have to pre-store this subnode
        if ($map) {
            # 1:many between this and child
            # (eg this has fk to child)
            # store child before this;
            # use fk in this
            my $fktable = $map->get_fktable;

            my $col = $map->get_col || $self->get_pk_col($fktable);

            # aliases map an extra table
            # eg table X col X.A => Y.B
            # fktable_alias = A
            my $fktable_alias = $map->get_fktable_alias;
            my $orig_nt = $nt;

            # if we have an alias, it means the actual node
            # we want to store is one beneath the alias;
            # eg <foo><alias><foo2>..</foo2></alias></foo>
            # we want to actually store the node foo2
            if ($fktable_alias) {
                my @nts = $nt->sgetnode($map->sget_fktable);
                if (!@nts) {
                    print STDERR $nt->sxpr;
                    confess("could not get node for: ".$map->sget_fktable);
                }
                if (@nts > 1) {
                    print STDERR $nt->sxpr;
                    confess("multiple nodes for: ".$map->sget_fktable);
                }
                $nt = shift @nts;
                if (!$nt) {
                    print STDERR $map->sxpr;
                    print STDERR $orig_nt->sxpr;
                    confess("bad nodes for: ".$map->sget_fktable);
                }
            }
            my $fk_id = $self->_storenode($nt);
            if (!defined($fk_id)) {
                confess("ASSERTION ERROR: could not get foreign key val\n".
                        "trying to store: $element\n".
                        "no fk returned when storing: $fktable");
            }
            trace(0, "SETTING $element.$col=$fk_id [via ".$orig_nt->element."]");
            $node->set($col, $fk_id);
            $node->unset($orig_nt->element);
        }
        else {
            # 1:many between child and this
            # (eg child has fk to this)
            # store child after
            trace(0, "WILL STORE LATER:\n", $nt->xml);
            $node->unset($nt->element);
            push(@delayed_store, $nt);
        }
#        $node->unset($nt->element); # clear it
    }
    # --- done storing kids

    # --- replace *IDs ---
    # dbstag XML allows placeholder values in primary key cols
    # (for now PKs are always assumed to be autoincrement/serial ints)
    # placeholder PKs get remapped to a new autogenerated ID
    # all FKs refering to this get remapped too
    my @tnodes = $node->tnodes;
    my %remap = ();   # indexed by column name; new PK value
    unless ($self->trust_primary_key_values) {
        foreach my $tnode (@tnodes) {
            if ($tnode->name eq $pkcol) {
                my $v = $tnode->data;
                trace(0, "REMAP $pkcol: $v => ? [do not know new value yet]");
                $remap{$tnode->name} = $v; # map after insert/update
                $node->unset($tnode->name); # discard placeholder
            } else {
                if ($tnode->name =~ /_id$/) {
                    # hack!! need proper FK refs...
                    my $colvalmap = $self->id_remap_idx;
                    #my $colvalmap = $self->get_mapping_for_col($nt->elememt);
                    if ($colvalmap) {
                        my $v = $tnode->data;
                        my $nv = $colvalmap->{$v};
                        if ($nv) {
                            trace(0, "remapping $v => $nv");
                            $tnode->data($nv);
                        }
                    }
                }
            }
        }
    }  # -- end of ID remapping
    
    # --- Get columns that need updating/inserting ---
    # turn all remaining tag-val pairs into a hash
    my %store_hash = $node->pairs;

    # All columns to be stored should be terminal nodes
    # in the Stag tree; if not there is a problem
    my @refcols = grep { ref($store_hash{$_}) } keys %store_hash;
    if (@refcols) {
        foreach (@$mapping) {
            trace(0, $_->sxpr);
        }
        confess("I can't store the current node; ".
                "These elements need to be mapped via FKs: ".
                join(', ', map {"\"@refcols\""} @refcols).
                "\n\nPerhaps you need to specify more schema metadata?");
    } # -- end of sanity check

    # each relation has zero or more unique keys;
    # unique keys may be compound (ie >1 column)
    my @usets = $self->get_unique_sets($element);
    trace(0, "USETS: ", map {"unique[ @$_ ]"} @usets);

    # get all the columns/fields/attributes of this relation
    my @cols = $self->get_all_cols($element);
    trace(0, "COLS: @cols");

    # store_node() will either perform an update or
    # an insert. if we are performing an update, we
    # need a query constraint to determine which row
    # to update.
    #
    # this hash is used to determine the key/val pairs
    my %unique_constr;

    # this is the value of the primary key of
    # the inserted/update row
    my $id;

    # if this relation has a primary key AND the stag node
    # being stored has the value of this column set, THEN
    # use this as the update constraint
    if (0 && $pkcol) {
        my $pk_id;
        $pk_id = $node->get($pkcol);
        if ($pk_id) {
            # unset the value of the pk in the node; there
            # is no point setting this in the UPDATE as it
            # is already part of the update constraint
            $node->unset($pkcol);

            # set the update constraint based on the PK value
            %unique_constr = ($pkcol => $pk_id);

            # return this value at the end
            $id = $pk_id;
            trace(0, "SETTING UPDATE CONSTR BASED ON PK $pkcol = $pk_id");
        }
    } # -- end of xxxx


    #        foreach my $sn ($node->kids) {
    #            my $name = $sn->element;
    #            my $nu_id = $self->id_mapping($name, $sn->data);
    #            # do the old 2 nu mapping
    #            # (the ids in the xml are just temporary
    #            #  for internal consistency)
    #            $sn->data($nu_id) if $nu_id;
    #        }

    # ---- EXPERIMENTAL ----
    # if no unique keys are provided, assume that all
    # non-PK columns together provide a compound unique key
    # <<DANGEROUS ASSUMPTION!!>> expedient for now!
    if (!@usets) {
        #        push(@usets, [grep {$_ ne $pkcol} @cols]);
        @usets = ( [grep {$_ ne $pkcol} @cols] );
    }
    if ($pkcol) {
        # make single PK the first unique key set;
        # add to beginning as this is the most efficient
        unshift(@usets, [$pkcol]);
    }

    # get the column to select to get the pk for this element
    my $select_col = $pkcol;

    # -------- find update constraint by unique keys ----
    # if the unique_constr hash is set, we know we
    # are doing an UPDATE, and we know the query
    # constraint that will be used;
    #
    # otherwise loop through all unique keys; if
    # all the columns in the key are set, then we
    # can safely use this unique key as the update
    # constraint.
    # if no update constraint can be found, this node
    # is presumed not to exist in the DB and an INSERT 
    # is performed
    foreach my $uset (@usets) {
        # we already know & have the primary key
        last if %unique_constr;

        # if we are loading up a fresh/blank slate
        # database then we don't need to check for
        # existing tuples, as everything should
        # have been inserted/updated this session
        if ($self->policy_freshbulkload) {
            next;
        }

        # already tried PK
#        if (scalar(@$uset) == 1 && 
#            $uset->[0] eq $pkcol) {
#            next;
#        }
        trace(0, "TRYING USET: ;@$uset; [pk=$pkcol]");

        # get the values of the unique key columns;
        # %constr is a candidate unique key=>val mapping
        my %constr =
          map {
              my $v = $node->sget($_);
              $_ => $v
          } @$uset;

        # each column in the unique key must be 
        # non-NULL; try the next unique key if
        # this one is unsuitable
        next if grep { !defined($_) } values %constr;
        %unique_constr = %constr;
        if (!$select_col && @$uset == 1) {
            $select_col = $uset->[0];
        }
        trace(0, "GOT unique_constr, select_col=$select_col");
        last;
    }
    # -- END OF @usets --

    # %unique_constr is set; a mapping for a unique key colset
    # if this is not set, then we must insert

    if (%unique_constr) {

        # -- IN-MEMORY CACHING --
        # check if we have already updated/inserted
        # this tuple this session; and if so, what
        # the update constraint used was
        if ($is_caching_on == 1 || $is_caching_on == 3) {

            #$self->throw("no select col for $element") unless $select_col;
            # fetch values of unique_constr from cache
            my %cached_colvals =
              %{$self->query_cache($element,
                                   \%unique_constr)
                  || {}};
            # have we stored anything with uniq key %unique_constr before?
            if (%cached_colvals) {
                if ($pkcol) {
                    $id = $cached_colvals{$pkcol};
                    if ($id) {
                        # use the cached pk id for efficiency
                        #%unique_constr = {$pkcol => $id};
                        trace(0, "CACHED $pkcol  = $id");
                    }
                    else {
                        trace(0, "NO CACHED COLVAL FOR $pkcol :: ".
                             join("; ",map {"$_ = $cached_colvals{$_}"} keys %cached_colvals));
                    }
                }

                # yes - has it changed?
                foreach my $col (keys %cached_colvals) {
                    if ($cached_colvals{$col} && $store_hash{$col} &&
                        $cached_colvals{$col} && $store_hash{$col}) {
                        # don't bother re-storing anything
                        delete $store_hash{$col};
                    }
                }
                if (%store_hash) {
                    my @x = keys %store_hash;
                    trace(0, "WILL STORE: @x");
                }
                else {
                    trace(0, "UNCHANGED - WILL NOT STORE; store_hash empty");
                }
            }
            else {
            }
        }
        # -- END OF CACHING CHECK --

        # -- GET PK VAL $id BASED ON unique_constr --
        # (we may already have this based on memory-cache)
        if (!$id) {

            # the input node contains all the keys in %update_constr
            # - check to see if this relation exists in the DB

            my $vals;
            if ($is_caching_on >= 2) {
                $vals = [];
            }
            else {
                my $sql =
                  $self->makesql($element,
                                 \%unique_constr,
                                 $select_col);
                trace(0, "SQL: $sql");
                $vals =
                  $dbh->selectcol_arrayref($sql);
            }

            if (@$vals) {
                # yes it does exist in DB; check if there is a
                # pkcol - if there is, it means we can do an
                # update and 
                if ($pkcol && $select_col && $select_col eq $pkcol) {
                    # this is the value we return at the
                    # end
                    $id = $vals->[0];
                    if ($remap{$pkcol}) {
                        #my $colvalmap = $self->get_mapping_for_col($pkcol);
                        my $colvalmap = $self->id_remap_idx;
                        #my $colvalmap = $self->get_mapping_for_col($element);
                        $colvalmap->{$remap{$pkcol}} = $id;
                        trace(0, "COLVALMAP $pkcol $remap{$pkcol} = $id");
                    }
                }
                else {
                    # $id not set, but we will later perform an update anyway
                }
            }
            else {
                # this node is not in the DB; force insert
                %unique_constr = ();
            }
        }
    } # end of get pk val

    # ---- UPDATE OR INSERT -----
    # at this stage we know if we are updating
    # or inserting, depending on whether a suitable
    # update constraint has been found

    if (%unique_constr) {
        # ** UPDATE **
        if ($self->noupdate_h->{$element}) {
            if ($tracekeyval) {
                printf STDERR "NOUPDATE: $tracenode = $tracekeyval\n"
            }
            trace(0, sprintf("NOUPDATE on %s OR child nodes (We have %s)",
                             $element,
                             join('; ',values %unique_constr)
                            ));
            # don't return yet; there are still the delayed nodes
            ##return $id;
        }
        else {
            # if there are no fields modified,
            # no change
            foreach (keys %unique_constr) {
                # no point setting any column
                # that is part of the update constraint
                delete $store_hash{$_};
            } 
            
            # only update if there are cols set that are
            # not part of unique constraint
            if (%store_hash) {
                if ($tracekeyval) {
                    printf STDERR "UPDATE: $tracenode = $tracekeyval\n"
                }

                $self->updaterow($element,
                                 \%store_hash,
                                 \%unique_constr);
                # -- CACHE RESULTS --
                if ($is_caching_on == 1 || $is_caching_on == 3) {
                    $self->update_cache($element,
                                        \%store_hash,
                                        \%unique_constr);
                }
            }
            else {
                trace(0, sprintf("NOCHANGE on %s (We have %s)",
                                 $element,
                                 join('; ',values %unique_constr)
                            ));
                if ($tracekeyval) {
                    printf STDERR "NOCHANGE: $tracenode = $tracekeyval\n"
                }
            }
        }
    } else {
        # ** INSERT **
        $id =
          $self->insertrow($element,
                           \%store_hash,
                           $pkcol);
        if ($tracekeyval) {
            printf STDERR "INSERT: $tracenode $tracekeyval [val = $id]\n"
        }
        if ($pkcol) {
            if ($remap{$pkcol}) {
                my $colvalmap = $self->id_remap_idx;
                #my $colvalmap = $self->get_mapping_for_col($element);
                $colvalmap->{$remap{$pkcol}} = $id;
                trace(0, "colvalmap $remap{$pkcol} = $id");
            }
        }

        # -- CACHE RESULTS --
        if ($is_caching_on) {
            my %cache_hash = %store_hash;
            if ($pkcol) {
                $cache_hash{$pkcol} = $id;
            }
            $self->insert_into_cache($element,
                                     \%cache_hash,
                                     \@usets);
            trace(0, "CACHING: $element");
        }

    }  # -- end of UPDATE/INSERT
    

    # -- DELAYED STORE --
    # Any non-terminal child nodes of the current one have
    # some kind of foreign key relationship to the current
    # relation. Either it is 1:many or many:1
    #
    # if the relation for the child node has a foreign key
    # into the current relation, we need to store the current
    # relation first to get the current relation's primary key.
    #
    # we have already done this, so now is the time to store
    # any of these child nodes
    if (@delayed_store) {
        foreach my $sn (@delayed_store) {
            # ASSUMPTION - FK and PK are named the same
            # WRONG????????????????
            my $fk = $pkcol;

	    # HACK - specific to databases that use 'id' for PK
	    # and <ftable>_id for FK
	    if ($fk eq 'id') {
		$fk = $element . '_id';
	    }

            $sn->set($fk, $id);

            trace(0, "NOW TIME TO STORE [curr pk val = $id] [fkcol = $fk] ", $sn->xml);
            $self->_storenode($sn);
        }
    } # -- end of @delayed_store

    return $id;
}

# -- QUERYING --

sub rmake_nesting {
    my $node = shift;

    if ($node->element eq 'composite') {
	my $first = $node->getnode_first;
	my $second = $node->getnode_second;
	my $head = rmake_nesting($first->data->[0]);
	my $tail = rmake_nesting($second->data->[0]);
	if ($head->isterminal) {
	    return 
	      Data::Stag->new($head->element => [$tail]);
	}
	$head->addkid($tail);
	return $head;
    }
    elsif ($node->element eq 'leaf') {
	my $alias = $node->get_alias;
	my $tn = $alias || $node->get_name;
	return Data::Stag->new($tn=>1);
    }
    else {
	die;
    }
}

# last SQL SELECT statement executed
sub last_stmt {
    my $self = shift;
    $self->{_last_stmt} = shift if @_;
    return $self->{_last_stmt};
}

sub last_sql_and_args {
    my $self = shift;
    $self->{_last_sql_and_args} = shift if @_;
    return $self->{_last_sql_and_args};
}


sub sax_handler {
    my $self = shift;
    $self->{_sax_handler} = shift if @_;
    return $self->{_sax_handler};
}


# delegates to selectall_stag and turns tree to XML
sub selectall_xml {
    my $self = shift;
    my $stag = $self->selectall_stag(@_);
    return $stag->xml;
}

# delegates to selectall_stag and turns tree to SAX
# (candidate for optimisation - TODO - use event firing model)
sub selectall_sax {
    my $self = shift;
    my ($sql, $nesting, $h) = 
      rearrange([qw(sql nesting handler)], @_);
    my $stag = $self->selectall_stag(@_);
    $h = $h || $self->sax_handler;
    if (!$h) {
	$self->throw("You must specify the sax handler;\n".
		     "Either use \$dbh->sax_handler(\$h), or \n".
		     "\$dbh->selectall_sax(-sql=>\$sql, handler->\$h)");
    }
    return $stag->sax($h);
}

# delegates to selectall_stag and turns tree to S-Expression
sub selectall_sxpr {
    my $self = shift;
    my $stag = $self->selectall_stag(@_);
    return $stag->sxpr;
}

# does not bother decomposing and nesting the results; just
# returns the denormalised table from the SQL query.
# arrayref of arrayrefs - rows x cols
# first row of rows is column headings
sub selectall_rows {
    my $self = shift;
    my ($sql, $nesting, $bind, $template) = 
      rearrange([qw(sql nesting bind template)], @_);
    my $rows =
      $self->selectall_stag(-sql=>$sql,
			    -nesting=>$nesting,
			    -bind=>$bind,
			    -template=>$template,
			    -return_arrayref=>1,
			   );
    return $rows;
}

# ---------------------------------------
# selectall_stag(sql, nesting)
#
# Takes an sql string containing a SELECT statement,
# parses it to get the tree structure; this can be
# overridden with the nesting optional argument.
#
# The SELECT statement is executed, and the relations are
# transformed into a stag tree
#
# ---------------------------------------
sub selectall_stag {
    my $self = shift;
    my ($sql, $nesting, $bind, $template, $return_arrayref) = 
      rearrange([qw(sql nesting bind template return_arrayref)], @_);
    my $prep_h = $self->prepare_stag(@_);
    my $cols = $prep_h->{cols};
    my $sth = $prep_h->{sth};
    my $exec_args = $prep_h->{exec_args};
    # TODO - make this event based so we don't have to
    # load all into memory
    my $rows =
      $self->dbh->selectall_arrayref($sth, undef, @$exec_args);
    if ($return_arrayref) {
	my @hdrs = ();
	for (my $i=0; $i<@$cols; $i++) {
	    my $h = $prep_h->{col_aliases_ordered}->[$i] || $cols->[$i];
	    push(@hdrs, $h);
	}
	return [\@hdrs, @$rows];
    }

    trace(0, sprintf("Got %d rows\n", scalar(@$rows)));
    # --- reconstruct tree from relations
    my $stag =
      $self->reconstruct(
                         -rows=>$rows,
                         -cols=>$cols,
                         -alias=>$prep_h->{alias},
                         -nesting=>$prep_h->{nesting}
                        );
    return $stag;
}

sub prepare_stag {
    my $self = shift;
    my ($sql, $nesting, $bind, $template, $return_arrayref) = 
      rearrange([qw(sql nesting bind template return_arrayref)], @_);

    my $parser = $self->parser;

    my $sth;
    my @exec_args = ();
    if (ref($sql)) {
	$template = $sql;
    }
    if ($template) {
	if (!ref($template)) {
	    $template = $self->find_template($template);
	}
	($sql, @exec_args) = $template->get_sql_and_args($bind);
    }
    trace 0, "parsing_sql: $sql\n";

    # PRE-parse SQL statement for stag-specific extensions
    if ($sql =~ /(.*)\s+use\s+nesting\s*(.*)/si) {
	my ($pre, $post) = ($1, $2);
	my ($extracted, $remainder) =
	  extract_bracketed($post, '()');
        if ($nesting) {
            $self->throw("nestings clash: $nesting vs $extracted");
        }
	$nesting = Data::Stag->parsestr($extracted);
	$sql = "$pre $remainder";
    }


    # get the parsed SQL SELECT statement as a stag node
    my $stmt = $parser->selectstmt($sql);
    if (!$stmt) {
	# there was some error parsing the SQL;
	# DBI can probably give a better explanation.
	eval {
	    my $sth = $self->dbh->prepare($sql);
	    
	};
	if ($@) {
	    $self->throw("SQL ERROR:\n$@");
	}
	# DBI accepted it - must be a bug in the DBStag grammar
	$self->throw("I'm sorry but the SQL statement you gave does\n".
		     "not conform to the more limited subset of SQL\n".
		     "that DBStag supports. Please see the DBStag docs\n".
		     "for details.\n".
		     "\n".
		     "Remember to check you explicitly declare all aliases\n".
		     "using AS\n\n\nSQL:$sql");
    }


    trace 0, "parsed_sql: $sql\n";
#    trace 0, $stmt->xml;
    my $dbschema = $self->dbschema;

    $self->last_stmt($stmt);

    # stag node of FROM part of SQL
    my $fromstruct = $stmt->get_from;

    # --- aliases ---

    # keep a hash of table aliases
    # KEY: table alias
    # VAL: base table
    # for example, 'SELECT * FROM person AS p'
    # will result in $alias_h = { p => person }
    my $alias_h = {};

    # build alias hash using FROM node
    foreach my $sn ($fromstruct->subnodes) {
        get_table_alias_map($sn, $alias_h);
    }

    # as well as an alias hash map, 
    # keep an array of stag nodes representing all the aliases
    my @aliases = ();
    foreach my $alias (keys %$alias_h) {
        push(@aliases, 
             Data::Stag->new(alias=>[
                                     [name=>$alias],
                                     [table=>$alias_h->{$alias}->[0]]
                                    ]));
    }
    my $aliasstruct = Data::Stag->new(alias=>[@aliases]);

    # --- nestings ---
    #
    # the cartesian product that results from a SELECT can
    # be turned into a tree - there is more than one tree to
    # choose from; eg with "x NJ y NJ z" we can have trees:
    # [x [y [z]]]
    # [x [y z]]
    # [z [x y]]
    # etc
    #
    # the actual allowed nestings through the graph is constrained
    # by the FK relationships; we do not utilise this yet (TODO!)
    # later the user need only specify the root. for now they
    # must specify the full nesting OR allow the bracket structure
    # of the joins...

    # if the user did not explicitly supply a nesting,
    # guess one from the bracket structure of the FROM 
    # clause (see rmake_nesting)
    # [TODO: be more clever in guessing the nesting using FKs]
    if (!$nesting) {
	$nesting = Data::Stag->new(top=>1);
#	my $cons = rmake_cons($fromstruct->data->[0], $nesting);
	$nesting = rmake_nesting($fromstruct->data->[0]);
        $nesting = Data::Stag->new(top=>[$nesting]);
	trace(0, "\n\nNesting:\n%s\n\n",  $nesting->xml);
    }
    if ($nesting && !ref($nesting)) {
        $nesting = Data::Stag->parsestr($nesting);
    }

    # keep an array of named relations used in the query -
    # the named relation is the alias if present;
    # eg
    # SELECT * FROM person AS p NATURAL JOIN department
    # the named relations here are 'p' and 'department'
    my @namedrelations = ();
    $fromstruct->iterate(sub {
			     my $n = shift;
			     if ($n->element eq 'leaf') {
                                 my $v = $n->sget_alias || $n->sget_name;
				 push(@namedrelations, $v)
			     }
			 });

    # --- fetch columns ---
    #
    # loop through all the columns in the SELECT clause
    # making them all of a standard form; eg dealing
    # with functions and '*' wildcards appropriately

    my @col_aliases_ordered = ();
    my @cols =
      map {
	  # $_ iterator variable is over the columns
	  # specified in the SELECT part of the query;
	  # each column is represented as a stag node

	  # column name
          my $name = $_->get_name;

	  # column alias, if exists
	  # eg in 'SELECT name AS n' the alias is 'n'
          my $col_alias = $_->get_alias;
	  push(@col_aliases_ordered, $col_alias);

	  # make the name the alias; prepend named relation if supplied.
	  # eg in 'SELECT person.name AS n' the name will become
	  # 'person.n'
	  if ($col_alias) {
	      $name = $col_alias;
	      if ($_->get_table) {
		  $name = $_->get_table . '.'. $name;
	      }
	  }

	  my $func = $_->getnode('func');

	  # from here on determines returned value of the
	  # map iteration:

	  if ($func) {
	      # a typical column node for a function looks like
	      # this:
	      #
	      #    (col
	      #      (func
	      #        (name "somefunc")
	      #        (args
	      #          (col
	      #            (name "x.foo")
	      #            (table "x")))))
	      #      (alias "myname"))
	      #
	      # if a function is included, and the function
	      # return value is aliased, use that alias;
	      # otherwise ...

	      my $funcname = $func->get_name;
	      # query the function stag node for the element
	      # 'col'
	      my ($col) = 
		$func->where('col',
			     sub {shift->get_table});
	      my $table = $col_alias || $funcname;
	      if (!$col_alias) {
		  $col_alias = $funcname;
	      }
	      if ($col) {
		  $table = $col->get_table;
	      }
#	      if ($col_alias =~ /(\w+)__(\w+)/) {
#		  $table = $1;
#		  $col_alias = $2;
#	      }
	      $name = $table . '.' . $col_alias;
	      # return:
	      $name;
	  }
          elsif ($name =~ /^(\w+)\.\*$/) {
	      # if the column name is of the form
	      # RELATION.*, then replace the * with
	      # all the actual columns from the base relation
	      # RELATION
	      #
	      # the final result will be TABLE.col1, TABLE.col2,...

              my $tn = $1;
              my $tn_alias = $tn;

	      # use base relation name to introspect schema
              if ($alias_h->{$tn}) {
                  $tn = $alias_h->{$tn}->[0];
              }
              my $tbl = $dbschema->table(lc($tn));
              if (!$tbl) {
                  confess("No such table as $tn");
              }
	      # introspect schema to get columns for this table
              my @cns = $tbl->columns;

#	      trace(0, Dumper $tbl);
	      trace(0, "TN:$tn ALIAS:$tn_alias COLS:@cns");

	      # return:
              map { "$tn_alias.$_" } @cns;
          }
          elsif ($name =~ /^\*$/) {
	      # if the column name is '*' (ie select all)
	      # then replace the * with
	      # all the actual columns from the base relations in
	      # the query (use FROM clause)
	      #

              my %got = ();
              my @allcols =
                map {
                    my $tn = $_;
                    my $baserelname = 
                      $alias_h->{$tn} ? 
                        $alias_h->{$tn}->[0] : $tn;
                    my $tbl = $dbschema->table(lc($baserelname));
                    if (!$tbl) {
                        confess("Don't know anything about table:$tn\n".
                                "Maybe DBIx::DBSchema does not work for your DBMS?\n".
                                "If $tn is a view, you may need to modify DBIxLLDBSchema");
                    }
                    my @cns = $tbl->columns;
                    #                  @cns = grep { !$got{$_}++ } @cns;
                    map { "$tn.$_"} @cns;
                } @namedrelations;

              # This is a bit hacky; if the user specifies
              # SELECT * FROM... then there is no way
              # to introspect the actual column returned
              # using DBI->selectall_arrayref
              #
              # maybe we should selectall_hashref
              # instead? this is generally slower; also
              # even if we get it with a hashref, the
              # result can be ambiguous since DBI only
              # gives us the colun names back
              #
              # to get round this we just replace the *
              # in the user's query (ie in the actual SQL)
	      # with the full column list
              my $replace = join(', ', @allcols);
	      # rewrite SQL statement; assum only one instance of
	      # string '*' in these cases
              $sql =~ s/\*/$replace/;
	      # return:
              @allcols;
	  }
          else {
	      # no * wildcard in column, and not a function;
	      # just give back the node

	      # return:
              $name
          }
      } $stmt->sgetnode_cols->getnode_col;

    @cols =
      map {
	  if (/(\w+)__(\w+)/) {
	      "$1.$2";
	  }
	  else {
	      $_
	  }
      } @cols;

    # ---- end of column fetching ---

    trace(0, "COLS:@cols");



    # --- execute SQL SELECT statement ---
    if ($template) {
	$sth = $template->cached_sth->{$sql};
	if (!$sth) {
	    $sth = $self->dbh->prepare($sql);
	    $template->cached_sth->{$sql} = $sth;	  
	}
#	($sql, $sth, @exec_args) = 
#	  $template->prepare($self->dbh, $bind);
    }
    else {
        $sth = $self->dbh->prepare($sql);
    }
    my $sql_or_sth = $sql;
    if ($sth) {
	$sql_or_sth = $sth;
    }
    trace(0, "SQL:$sql");
    trace(0, "Exec_args: @exec_args") if @exec_args;
    $self->last_sql_and_args([$sql, @exec_args]);
    return 
      {
       sth=>$sth,
       exec_args=>\@exec_args,
       cols=>\@cols,
       col_aliases_ordered=>\@col_aliases_ordered,
       alias=>$aliasstruct,
       nesting=>$nesting
      };
}


# ============================
# get_table_alias_map(tablenode, alias hash)
#
# checks a tablenode (eg the stag representing 
# a table construct in the FROM clause) and adds
# it to the alias hash if it specifies an alias
# ============================
sub get_table_alias_map {
    my $s = shift;
    my $h = shift;

    # the FROM clause is natively stored as a binary tree
    # (in order to group the joins by brackets) - recursively
    # descend building the hash map

    if ($s->name eq 'leaf') {
        my $alias = $s->get_alias;
        if ($alias) {
            $h->{$alias} = [$s->get_name];
        }
        return ($s->get_name);
    }
    elsif ($s->name eq 'composite') {
        my ($first, $second) = 
          ($s->getnode_first,
           $s->getnode_second);
        my $alias = $s->get_alias;
        my @sn = ($first->subnodes, $second->subnodes);
        my @subtbls = map {
            get_table_alias_map($_, $h),
        } @sn;
        if ($alias) {
            $h->{$alias} = [@subtbls];
        }
        return @subtbls;
    }
    else {
        confess $s->name;
    }
}

# ============================
# reconstruct(schema, rows, top, cols, constraints, nesting, aliasstruct)
#
# mainly called by: selectall_stag(...)
#
# takes an array of rows (ie the result of an SQL query, probably
# involving JOINs, which is a denormalised relation) and
# decomposes this relation into a tree structure
#
# in order to do this, it requires schema information, and a nesting
# through the implicit result graph to build a tree
# ============================
sub reconstruct {
    my $self = shift;
    my $tree = Data::Stag->new();
    my ($schema,         # OPTIONAL - meta data on relation
        $rows,           # REQUIRED - relation R - array-of-array
        $top,            # OPTIONAL - root node name
        $cols,           # REQUIRED - array of stag nodes per column of R
        $constraints,    # NOT USED!!!
        $nesting,           # REQUIRED - tree representing decomposed schema
        $aliasstruct,    # OPTIONAL - renaming of columns in R
        $noaliases) =
          rearrange([qw(schema
                        rows
                        top
                        cols
                        constraints
                        nesting
                        alias
                        noaliases)], @_);

    # --- get the schema ---
    #
    # $schema is a stag representing the schema
    # of the input releation R (not the schema of
    # the db that produced it.... hmm, this could
    # be misleading)
    #
    # it conforms to the following stag-struct:
    #
    #'(schema
    #  (top? "RECORDSET-ELEMENT-NAME")
    #  (cols?
    #   (col+
    #    (relation "RELATION-NAME")
    #    (name     "COLUMN-NAME")
    #    ))
    #  (nesting?
    #   (* "NESTING-TREE")))
    #
    # each column represents the 
    
    if (!$schema) {
        $schema = $tree->new(schema=>[]);
    }
    if (!ref($schema)) {
        # it is a string - parse it
        # (assume sxpr)
        $schema = $tree->from('sxprstr', $schema);
    }

    # TOP - this is the element name
    # to group the structs under.
    # [override if specified explicitly]
    if ($top) {
        stag_set($schema, 'top', $top);
    }
#    $top = $schema->get_top || "set";
    if (!$top) {
	if ($nesting) {
	    # use first element in nesting
	    $top = $nesting->element;
	}
	else {
	    $top = 'set';
	}
    }
    my $topstruct = $tree->new($top, []);

    # COLS - this is the columns (attribute names)
    # in the order they appear
    # [override if specified explicitly]
    if ($cols) {
        my @ncols =
          map {
              if (ref($_)) {
                  $_
              }
              else {
                  # presume it's a string
                  # format = RELATION.ATTRIBUTENAME
                  if (/(\w+)\.(\w+)/) {
                      $tree->new(col=>[
                                       [relation=>$1],
                                       [name=>$2]]);
                  }
                  elsif (/(\w+)/) {
                      confess("Not implemented yet - must specify tbl for $_");
                      $tree->new(col=>[
                                       [relation=>'unknown'],
                                       [name=>$2]]);
                  }
                  else {
                      confess "I am confused by this column: $_";
                  }
              }
          } @$cols;
        $schema->set_cols([@ncols]);
    }


    # NESTING - this is the tree structure in
    # which the relations are structured
    # [override if specified explicitly]
    if ($nesting) {
        if (ref($nesting)) {
        }
        else {
            $nesting = $tree->from('sxprstr', $nesting);
        }
        $schema->set_nesting([$nesting]);
    }
    else {
        $nesting = $schema->sgetnode_nesting;
    }
    if (!$nesting) {
        confess("no nesting!");
    }

    # --- alias structure ---
    #
    # use this to get a hash map of alias => baserelation 

    ($aliasstruct) = $schema->getnode_aliases unless $aliasstruct;
    if ($aliasstruct && !ref($aliasstruct)) {
        $aliasstruct = $tree->from('sxprstr', $aliasstruct);
    }
    my @aliases = ();
    if ($aliasstruct && !$noaliases) {
        @aliases = $aliasstruct->getnode_alias;
    }
    my %alias2baserelation =
      map {
          $_->sget_name => $_->sget_table
      } @aliases;

    # column headings; (ie all columns in R)
    my @cols = $schema->sgetnode_cols->getnode_col();

    # --- primary key info ---

    # set the primary key for each relation (one per relation);
    # the default is *all* the columns in that relation
    my %pkey_by_relationname = ();  # eg {person => [person_id]
    my %cols_by_relationname = ();  # eg {person => [person_id, fname, lname]

    # loop through all columns in R, setting above hash maps
    foreach my $col (@cols) {

	# the stag struct for each $col looks like this:
	#
	#   (col+
	#    (relation "RELATION-NAME")
	#    (name     "COLUMN-NAME")
	#    ))
	
        my $relationname = $col->get_relation;
        my $colname = $col->get_name;

	# pkey defaults to all columns in a relation
	# (we may override this later)
        $pkey_by_relationname{$relationname} = []
          unless $pkey_by_relationname{$relationname};
        push(@{$pkey_by_relationname{$relationname}},
             $colname);

	# all columns in a relation
	# (note: same as default PK)
        $cols_by_relationname{$relationname} = []
          unless $cols_by_relationname{$relationname};
        push(@{$cols_by_relationname{$relationname}},
             $colname);
    }
    my @relationnames = keys %pkey_by_relationname;

    # override PK if explicitly set as a constraint
    my @pks = $schema->findnode("primarykey");
    foreach my $pk (@pks) {

	# $pk looks like this:
	#
	# '(primarykey
	#   (relation "R-NAME")
	#   (col+ "COL-NAME"))

        my $relationname = $pk->get_relation;
        my @cols = $pk->get_col;

	# the hash %pkey_by_relationname should
	# be keyed by the named relations, not the
	# base relations
        my @aliasnames =
          grep { 
              $alias2baserelation{$_} eq $relationname 
          } keys %alias2baserelation;

        # relation is not aliased
        if (!@aliasnames) {
            @aliasnames = ($relationname);
        }
        foreach (@aliasnames) {
            $pkey_by_relationname{$_} = [@cols];
        }
    }

    # ------------------
    #
    # loop through denormalised rows,
    # putting the columns into their
    # respecive relations
    #
    # eg
    #
    #  <----- a ----->   <-- b -->
    #  a.1   a.2   a.3   b.1   b.2
    #
    # algorithm:
    #  use nesting/tree to walk through
    #
    # ------------------

    #~~~  keep a hash of all relations by their primary key vals
    #~~~   outer key = relationname
    #~~~   inner key = pkval
    #~~~   hash val  = relation structure
    #~~~ my %all_relation_hh = ();
    #~~~ foreach my $relationname (@relationnames) {
    #~~~     $all_relation_hh{$relationname} = {};
    #~~~ }

    #~~~ keep an array of all relations
    #~~~  outer key = relationname
    #~~~  inner array = ordered list of relations
    #~~~    my %all_relation_ah = ();
    #~~~    foreach my $relationname (keys %pkey_by_relationname) {
    #~~~        $all_relation_ah{$relationname} = [];
    #~~~    }

    # start at top of nesting tree
    #
    # a typical nesting tree may look like this:
    #
    # '(tableA
    #   (tableB "1")
    #   (tableC
    #    (tableD "1")))
    #
    # terminals ie "1" are ignored

    my ($first_in_nesting) = $nesting->subnodes;
    if (!$first_in_nesting) {
        $first_in_nesting = $nesting;
    }
    my $fipname = $first_in_nesting ? $first_in_nesting->name : '';

    # recursive hash representing tree
    #
    # $record =
    #   {child_h => { 
    #                $relation_name* => {
    #                                   $pk_val => $record
    #                                  }
    #               },
    #    struct => $stag_obj
    #   }
    #
    # this is recursively constructed using the make_a_tree() method
    # below. the nesting tree (see above) is traversed depth first,
    # constructing both the child_h hash and the resulting Stag
    # structure.

    my $top_record_h = 
      {
       child_h=>{ $fipname ? ($fipname=>{}) : () },
       struct=>$topstruct
      };
    # loop through rows in R
    foreach my $row (@$rows) {
        my @colvals = @$row;

        # keep a record of all table names in
        # this row from R
        my %current_relation_h = ();
        for (my $i=0; $i<@cols; $i++) {
            my $colval = $colvals[$i];
            my $col = $cols[$i];
            my $relationname = $col->get_relation;
            my $colname = $col->get_name;
            my $relation = $current_relation_h{$relationname};
            if (!$relation) {
                $relation = {};
                $current_relation_h{$relationname} = $relation;
            }
            $relation->{$colname} = $colval;
        }

#	print "ROW=@$row\n";
#	dmp(\%pkey_by_relationname);
#	dmp($top_record_h);

        # we now have a hash of hashes -
        #  outer keyed by relation id
        #  inner keyed by relation attribute name
        
        # traverse depth first down nesting;
        # add new nodes as children of the parent
        $self->make_a_tree($tree,
                    $top_record_h,
		    $first_in_nesting,
		    \%current_relation_h,
                    \%pkey_by_relationname,
		    \%cols_by_relationname,
                    \%alias2baserelation);
    }
    return $topstruct;
}
*norm = \&reconstruct;
*normalise = \&reconstruct;
*normalize = \&reconstruct;

# ============================
# make_a_tree(...) RECURSIVE
#
# called by: reconstruct(...)
#
# ============================
sub make_a_tree {
    my $self = shift;
    my $tree = shift;
    my $parent_rec_h = shift;
    my $node = shift;
    my %current_relation_h= %{shift ||{}};
    my %pkey_by_relationname = %{shift ||{}};
    my %cols_by_relationname = %{shift ||{}};
    my %alias2baserelation = %{shift ||{}};
    
    my $relationname = $node->name;
    my $relationrec = $current_relation_h{$relationname};
    my $pkcols = $pkey_by_relationname{$relationname};
    my $rec; # this is the next node down in the hash tree

    if (!$pkcols || !@$pkcols) {
	# if we have no columns for a particular part of
	# the nesting through the relation, it means it
	# was ommitted from the select clause - just skip
	# this part of the nesting.
	#
	# for example: SELECT a.*, b.* FROM a NJ a_to_b NJ b
	# the default nesting will be: [a [a_to_b [b]]]
	# the relation R will have columns:
	# a.c1    a.c2    b.c1   b.c2
	#
	# we want to build a resulting structure like this:
	# (a
	#  (c1 "x") (c2 "y")
	#  (b
	#   (c1 "a") (c2 "b")))
	#
	# so we just miss out a_to_b in the nesting, because it
	# has no columns in the relation R.
	$rec = $parent_rec_h;
    }
    else {

	my $pkval = 
	  CORE::join("\t",
		     map {
			 esctab($relationrec->{$_} || '')
		     } @$pkcols);

	$rec = $parent_rec_h->{child_h}->{$relationname}->{$pkval};

	if (!$rec) {
	    my $relationcols = $cols_by_relationname{$relationname};
	    my $has_non_null_val = grep {defined($relationrec->{$_})} @$relationcols;
	    return unless $has_non_null_val;
	    my $relationstruct =
	      $tree->new($relationname=>[
					 map {
					     defined($relationrec->{$_}) ? [$_ => $relationrec->{$_}] : ()
					 } @$relationcols
					]);
	    my $parent_relationstruct = $parent_rec_h->{struct};
	    if (!$parent_relationstruct) {
		confess("no parent for $relationname");
	    }
        
	    # if we have an aliased relation, add an extra
	    # level of nesting
	    my $baserelation = $alias2baserelation{$relationname};
	    if ($baserelation) {
#		trace(0, "R=$relationname BASE=$baserelation\n");
		my $baserelationstruct =
		  Data::Stag->new($baserelation =>
				  $relationstruct->data);
		stag_add($parent_relationstruct,
			 $relationname,
			 [$baserelationstruct]);
	    } else {
		stag_add($parent_relationstruct,
			 $relationstruct->name,
			 $relationstruct->data);
	    }
	    $rec =
	      {struct=>$relationstruct,
	       child_h=>{}};
	    foreach ($node->subnodes) {
		# keep index of children by PK
		$rec->{child_h}->{$_->name} = {};
	    }
	    $parent_rec_h->{child_h}->{$relationname}->{$pkval} = $rec;
	}
    }
    foreach ($node->subnodes) {
        $self->make_a_tree($tree,
			   $rec,
			   $_,
			   \%current_relation_h,
			   \%pkey_by_relationname,
			   \%cols_by_relationname,
			   \%alias2baserelation);
    }
}


# -------- GENERAL SUBS -----------

sub esctab {
    my $w=shift;
    $w =~ s/\t/__MAGICTAB__/g;
    $w;
}

sub makesql {
    my $self = shift;
    my ($table,
        $where,
        $select,
        $order,
        $group,
        $distinct) =
          rearrange([qw(table
                        where
                        select
                        order
                        group
                        distinct)], @_);

    confess("must specify table") unless $table;

    # array of tables
    if (ref($table)) {
        if (ref($table) eq "HASH") {
            $table =
              [
               map {
                   "$table->{$_} AS $_"
               } keys %$table
              ];
        }
    }
    else {
        $table = [$table];
    }

    $where = [] unless $where;
    # array of ANDed where clauses
    if (ref($where)) {
        if (ref($where) eq "HASH") {
            $where =
              [
               map {
                   "$_ = ".$self->quote($where->{$_})
               } keys %$where
              ];
        }
    }
    else {
        $where = [$where];
    }

    $select = ['*'] unless $select;
    # array of SELECT cols
    if (ref($select)) {
        if (ref($select) eq "HASH") {
            $select =
              [
               map {
                   "$select->{$_} AS $_"
               } keys %$select
              ];
        }
    }
    else {
        $select = [$select];
    }

    $order = [] unless $order;
    # array of order tables
    if (ref($order)) {
        if (ref($order) eq "HASH") {
            confess("order must be an array");
        }
    }
    else {
        $order = [$order];
    }

    $group = [] unless $group;
    # array of group tables
    if (ref($group)) {
        if (ref($group) eq "HASH") {
            confess("group must be an array");
        }
    }
    else {
        $group = [$group];
    }

    $distinct = $distinct ? '' : ' DISTINCT';
    my $sql =
      sprintf("SELECT%s %s FROM %s%s%s",
              $distinct,
              join(', ', @$select),
              join(', ', @$table),
              (scalar(@$where) ? 
               ' WHERE '.join(' AND ', @$where) : ''),
              (scalar(@$group) ? 
               ' GROUP BY '.join(', ', @$group) : ''),
              (scalar(@$order) ? 
               ' ORDER BY '.join(', ', @$order) : ''),
             );
    return $sql;
}



sub selectval {
    my $self = shift;
    trace(0, "@_");
    return $self->dbh->selectcol_arrayref(@_)->[0];
}

sub insertrow {
    my $self = shift;
    my ($table, $colvalh, $pkcol) = @_;
      
    my @cols = keys %$colvalh;
    my @vals = 
      map {
          defined($_) ? $self->quote($colvalh->{$_}) : 'NULL'
      } @cols;
    my $sql =
      sprintf("INSERT INTO %s (%s) VALUES (%s)",
              $table,
              join(", ", @cols),
              join(", ", @vals),
             );
    if (!@cols) {
	$sql = "INSERT INTO $table DEFAULT VALUES";
    }

    trace(0, "SQL:$sql");
    my $rval;
    eval {
        $rval = $self->dbh->do($sql);
    };
    if ($@) {
	if ($self->force) {
	    # what about transactions??
	    $self->warn("IN SQL: $sql\nWARNING: $@");
	}
	else {
	    confess $@;
	}
    }
    my $pkval;
    if ($pkcol) {
        $pkval = $colvalh->{$pkcol};
        if (!$pkval) {
            # POSTGRES HARDCODE ALERT
            if (0) {
                my $seqn = sprintf("%s_%s_seq",
                                   $table,
                                   $pkcol);
                trace(0, "CURRVAL $seqn = $pkval");
                $pkval  = $self->selectval("select currval('$seqn')");        
            }
            if (1) {
                # THIS IS NOT TRANSACTION SAFE
                # ONLY WORKS FOR SERIALS
                $pkval  = $self->selectval("select max($pkcol) from $table");        
            }
        }
        trace(0, "PKVAL = $pkval");
    }
    return $pkval;
}

sub updaterow {
    my $self = shift;
    my ($table, $set, $where) = @_;

    confess("must specify table") unless $table;

    my $dbh = $self->dbh;

    # array of WHERE cols
    if (ref($where)) {
        if (ref($where) eq "HASH") {
            $where =
              [
               map {
                   "$_ = ".$dbh->quote($where->{$_})
               } keys %$where
              ];
        }
    }
    else {
        $where = [$where];
    }
    confess("must specify constraints") unless @$where;

    confess("must set update vals") unless $set;
    my @bind = ();
    # array of SET colvals
    if (ref($set)) {
        if (ref($set) eq "HASH") {
            $set =
              [
               map {
                   push(@bind, defined $set->{$_} ? $set->{$_} : 'NULL');
                   "$_ = ?"
               } keys %$set
              ];
        }
    }
    else {
        $set = [$set];
    }
    
    my $sql =
      sprintf("UPDATE %s SET %s WHERE %s",
              $table,
              join(', ', @$set),
              join(' AND ', @$where),
             );
    trace(0, "SQL:$sql [",join(', ',@bind)."]");

    my $sth = $dbh->prepare($sql) || confess($sql."\n\t".$dbh->errstr);
    return $sth->execute(@bind) || confess($sql."\n\t".$sth->errstr);
}

#$::RD_HINT = 1;

$::RD_AUTOACTION = q { [@item] };
sub selectgrammar {
    return q[

             {
              use Data::Dumper;
              use Data::Stag;
              sub N {
                  Data::Stag->new(@_);
              }
          }
         ]
       .
         q[

         selectstmts: selectstmt ';' selectstmts
         selectstmts: selectstmt
           #           selectstmt: /select/i selectcols /from/i fromtables
         selectstmt: /select/i selectq(?) selectcols /from/i fromtables where(?) group(?) having(?) combiner(?) order(?) limit(?) offset(?) 
           {
               N(select => [
                            [qual => $item{'selectq'}[0]],
                            [cols => $item[3]],
                            [from => $item[5]],
#                            [where => $item[6]],
#                            [group => $item{'group'}[0]],
#                            [having => $item{'having'}[0]],
                           ]);
           }
           | <error>
         selectq: /all/i | /distinct/i 
               { $item[1] }
           | <error>               
#	 as: /\s+as\s+/i
	 as: /as/i
         selectcols: selectexpr /\,/ selectcols
           { [$item[1], @{$item[3]}] }
           | <error>
         selectcols: selectexpr
             { [$item[1]] }
           | <error>
         selectexpr: bselectexpr as aliasname
           {
               my $col = $item{bselectexpr};
               $col->set_alias($item{aliasname}->[1]);
               $col;
           }
           | <error>
	 selectexpr: bselectexpr
            { $item[1] }
            | <error>
         bselectexpr: funccall
           { $item[1] }
           | <error>
         bselectexpr: selectcol
           { $item[1] }
           | <error>

	 selectcol: brackselectcol operator selectcol
	   {
	       N(col=>[
		       [func => [
				 [name => $item[2]->[1]],
				 [args => [$item[1],$item[3]]]
				]
		       ]
		      ]);
	   }
###	   { $item[1]}
	   | <error>
	 selectcol: brackselectcol
	   { $item[1]}
	   | <error>

	 brackselectcol: '(' selectcol ')' 
	   { $item[2]}
	   | <error>

	 brackselectcol: bselectcol
	   { $item[1]}
	   | <error>

         bselectcol: /(\w+)\.(\w+)/
           { N(col=>[
                     [name => $item[1]],
		     [table=>$1],
                    ]) 
           }
           | <error>
         bselectcol: /(\w+)\.\*/
           { N(col=>[
                     [name => $item[1]],
                     [table=>$1],
                    ]) 
           }
           | <error>
         bselectcol: /\*/
           { N(col=>[
                     [name => $item[1]]
                    ]) 
           }
           | <error>
         bselectcol: /\w+/
           { N(col=>[
                     [name => $item[1]]
                    ]) 
           }
           | <error>
         bselectcol: expr
           { N(col=>[
                     [expr => $item[1]]
                    ]) }
           | <error>
         funccall: funcname '(' distinct(?) selectcols ')' 
           {
            my $col = N(col=>[
                              [func => [
                                        [name => $item[1]->[1]],
                                        [args => $item[4]]
                                       ]
                              ]
                             ]);
            $col;
           }
           | <error>

         distinct: /distinct/i

	 operator: '+' | '-' | '*' | '/' | '||'
	   

         fromtables: jtable
           { [$item[1]] }
           | <error>
         jtable: join_jtable
           { $item[1] }
           | <error>
         join_jtable: qual_jtable jointype join_jtable 
           { 
               shift @{$item[2]};
               my $j =
                 N(composite=>[
                               [ctype=>"@{$item[2]}"],
                               [first=>[$item[1]]],
                               [second=>[$item[3]]]
                              ]);
               $j;
         }
           | <error>
         join_jtable: qual_jtable
           { $item[1] }
           | <error>
         qual_jtable: alias_jtable joinqual 
           { 
               my $j = $item[1];
               $j->setnode_qual($item[2]);
               $j;
           }
           | <error>
         qual_jtable: alias_jtable
           { $item[1] }
           | <error>
         alias_jtable: brack_jtable /as\s+/i aliasname 
           { 
               my $j = $item[1];
               $j->set_alias($item[3][1]);
               $j;
           }
           | <error>
         alias_jtable: brack_jtable
           { $item[1] }
           | <error>
         brack_jtable: '(' jtable ')' 
           { $item[2] }
           | <error>
         brack_jtable: table
           { N(leaf=>[[name=>$item[1]->[1]]]) }
           | <error>

         joinqual: /on\s+/i bool_expr
           { N(qual => [
                        [type=>'on'],
                        [expr=>"@{$item[2]}"]
                       ])
         }
           | <error>
         joinqual: /using\s+/i '(' cols ')'
           { N(qual =>[
                       [type=>'using'],
                       [expr=>"@{$item[3]}"]
                      ])
         }
           | <error>

         table: tablename
           { $item[1] }
           | <error>

         funcname: /\w+/
         tablename: /\w+/
         aliasname: /\w+/


         cols: col(s)
         col: /\w+\.\w+/
         col: /\w+/

         jointype: /\,/
         jointype: /natural/i bjointype /join/i
         jointype: /natural/i /join/i
         jointype: bjointype /join/i
         jointype: /join/i
         bjointype: /inner/i
         bjointype: lrf(?) /outer/i
         lrf: /left/i | /right/i | /full/i
         bjointype: /cross/i

         number: float | int
         float: /\d*\.?\d+/ 'e' sign int
         float: /\d*\.\d+/
         int: /\d+/
         string: /\'.*?\'/
         sign: '+' | '-'
           
         exprs: '(' exprs ')'
         exprs: expr ',' exprs
         exprs: expr

	   # bool_expr - eg in where clause
         bool_expr: not_bool_expr boolop bool_expr | not_bool_expr
         not_bool_expr: '!' brack_bool_expr | brack_bool_expr
         brack_bool_expr: '(' bool_expr ')' | bool_exprprim
         bool_exprprim: boolval | expr
         boolval: /true/i | /false/i | /null/i

         expr: brack_expr op expr | brack_expr
         brack_expr: '(' expr ')' | exprprim
         exprprim: col | val
         val: number | string
           
         op: /not\s+/i /like\s+/i
         op: /like\s+/i
         op: /is\s+/i /not\s+/i
         op: /is\s+/i
         op: '=' | '!=' | '<>' | '<=' | '>=' | '<' | '>'
         boolop: /and\s+/i | /or\s+/i | /not\s+/i

#           where: /where/i /.*/
           where: /where/i bool_expr
           group: /group/i /by/i exprs
           having: /having/i /.*/
           combiner: combinekwd selectstmt
           combinekwd: /union/i | /intersect/i | /update/i
           order: /order/i /by/i orderexprs
           orderexprs: orderexpr ',' orderexprs
           orderexprs: orderexpr
           orderexpr: expr /asc/i
           orderexpr: expr /desc/i
           orderexpr: expr /using/i op
           orderexpr: expr
           limit: /limit/i /\w+/
           offset: /offset/i /\d+/
            ];
}

no strict 'refs';
sub AUTOLOAD {
    my $self = shift;
    my @args = @_;

    my $name = $AUTOLOAD;
    $name =~ s/.*://;   # strip fully-qualified portion

    if ($name eq "DESTROY") {
	# we dont want to propagate this!!
	return;
    }
    
    unless ($self->isa("DBIx::DBStag")) {
        confess("no such subroutine $name");
    }
    if ($self->dbh) {
	if ($self->dbh->can($name)) {
	    return $self->dbh->$name(@args);
	}
    }
    confess("no such method:$name)");
}

sub rearrange {
  my($order,@param) = @_;

  # If there are no parameters, we simply wish to return
  # an undef array which is the size of the @{$order} array.
  return (undef) x $#{$order} unless @param;

  # If we've got parameters, we need to check to see whether
  # they are named or simply listed. If they are listed, we
  # can just return them.
  return @param unless (defined($param[0]) && $param[0]=~/^-/);

  # Now we've got to do some work on the named parameters.
  # The next few lines strip out the '-' characters which
  # preceed the keys, and capitalizes them.
  my $i;
  for ($i=0;$i<@param;$i+=2) {
      if (!defined($param[$i])) {
	  cluck("Hmmm in $i ".CORE::join(";", @param)." == ".CORE::join(";",@$order)."\n");
      }
      else {
	  $param[$i]=~s/^\-//;
	  $param[$i]=~tr/a-z/A-Z/;
      }
  }
  
  # Now we'll convert the @params variable into an associative array.
  my(%param) = @param;

  my(@return_array);
  
  # What we intend to do is loop through the @{$order} variable,
  # and for each value, we use that as a key into our associative
  # array, pushing the value at that key onto our return array.
  my($key);

  foreach $key (@{$order}) {
      $key=~tr/a-z/A-Z/;
      my($value) = $param{$key};
      delete $param{$key};
      push(@return_array,$value);
  }
  
  # catch user misspellings resulting in unrecognized names
  my(@restkeys) = keys %param;
  if (scalar(@restkeys) > 0) {
       carp("@restkeys not processed in rearrange(), did you use a
       non-recognized parameter name ? ");
  }
  return @return_array;
}

#sub loadschema {
#    my $self = shift;
#    my ($ddl, $ddlf, $dialect) = 
#      rearrange([qw(ddl ddlf dialect)], @_);
#    if ($ddlf) {
#        my $fh = FileHandle->new($ddlf) || $self->throw("no file $ddlf");
#        $ddl = join('',<$fh>);
#        $fh->close;
#    }
#    $self->throw("no DDL") unless $ddl;
#    if ($dialect) {
#        my $driver = $self->{_driver} || 'Pg';
#        if ($driver ne $dialect) {
            
#        }
#    }
#}

1;

__END__

=head1 NAME

  DBIx::DBStag - Relational Database to Hierarchical (Stag/XML) Mapping

=head1 SYNOPSIS

  use DBIx::DBStag;
  my $dbh = DBIx::DBStag->connect("dbi:Pg:dbname=moviedb");
  my $sql = q[
	      SELECT 
               studio.*,
               movie.*,
               star.*
	      FROM
               studio NATURAL JOIN 
               movie NATURAL JOIN
               movie_to_star NATURAL JOIN
               star
	      WHERE
               movie.genre = 'sci-fi' AND star.lastname = 'Fisher'
              USE NESTING
               (set(studio(movie(star))))
	     ];
  my $dataset = $dbh->selectall_stag($sql);
  my @studios = $dataset->get_studio;

  # returns nested data that looks like this -
  #
  # (studio
  #  (name "20th C Fox")
  #  (movie
  #   (name "star wars") (genre "sci-fi")
  #   (star
  #    (firstname "Carrie")(lastname "Fisher")))))

  # iterate through result tree -
  foreach my $studio (@studios) {
	printf "STUDIO: %s\n", $studio->get_name;
	my @movies = $studio->get_movie;

	foreach my $movie (@movies) {
	    printf "  MOVIE: %s (genre:%s)\n", 
	      $movie->get_name, $movie->get_genre;
	    my @stars = $movie->get_star;

	    foreach my $star (@stars) {
		printf "    STARRING: %s:%s\n", 
		  $star->get_firstname, $star->get_lastname;
	    }
	}
  }
  
  # manipulate data then store it back in the database
  my @allstars = $dataset->get("movie/studio/star");
  $_->set_fullname($_->get_firstname.' '.$_->get_lastname)
    foreach(@allstars);

  $dbh->storenode($dataset);
  exit 0;

Or from the command line:

  unix> selectall_xml.pl -d 'dbi:Pg:dbname=moviebase'     \
       'SELECT * FROM studio NATURAL JOIN movie NATURAL   \
          JOIN movie_to_star NATURAL JOIN star            \
          USE NESTING (set(studio(movie(star))))'

Or using a predefined template:

  unix> selectall_xml.pl -d moviebase /mdb-movie genre=sci-fi

=cut

=head1 DESCRIPTION

This module is for mapping between relational databases and Stag
objects (Structured Tags - see L<Data::Stag>). Stag objects can also
be represented as XML. The module has two main uses:

=over

=item Querying

This module can take the results of any SQL query and decompose the
flattened results into a tree data structure which reflects the
foreign keys in the underlying relational schema. It does this by
looking at the SQL query and introspecting the database schema, rather
than requiring metadata or an object model.

In this respect, the module works just like a regular L<DBI> handle, with
a few extra methods.

Queries can also make use of predefined B<templates>

=item Storing Data

DBStag objects can store any tree-like datastructure (such as XML
documents) into a database using normalized schema that reflects the
structure of the tree being stored. This is done using little or no
metadata.

XML can also be imported, and a relational schema automatically generated.

=back

For a tutorial on using DBStag to build and query relational databases
from XML sources, please see L<DBIx::DBStag::Cookbook>

=head2 HOW QUERY RESULTS ARE TURNED INTO STAG/XML

This is a general overview of the rules for turning SQL query results
into a tree like data structure. You don't need to understand all
these rules to be able to use this module - you can experiment by
using the B<selectall_xml.pl> script which comes with this
distribution.

=head3 Mapping Relations

Relations (i.e. tables and views) are elements (nodes) in the
tree. The elements have the same name as the relation in the database.

These nodes are always non-terminal (ie they always have child nodes)

=head3 Mapping Columns

Table and view columns of a relation are sub-elements of the table or
view to which they belong. These elements will be B<data elements>
(i.e. terminal nodes). Only the columns selected in the SQL query
will be present.

For example, the following query

  SELECT name, job FROM person;

will return a data structure that looks like this:

  (set
   (person
    (name "fred")
    (job "forklift driver"))
   (person
    (name "joe")
    (job "steamroller mechanic")))

The data is shown as a lisp-style S-Expression - it can also be
expressed as XML, or manipulated as an object within perl.

=head3 Handling table aliases

If an ALIAS is used in the FROM part of the SQL query, the relation
element will be nested inside an element with the same name as the
alias. For instance, the query

  SELECT name FROM person AS author WHERE job = 'author';

Will return a data structure like this:

  (set
   (author
    (person
     (name "Philip K Dick"))))

The underlying assumption is that aliasing is used for a purpose in
the original query; for instance, to determine the context of the
relation where it may be ambiguous.

  SELECT *
  FROM person AS employee 
           INNER JOIN 
       person AS boss ON (employee.boss_id = boss.person_id)

Will generate a nested result structure similar to this -

  (set
   (employee
    (person
     (person_id "...")
     (name "...")
     (salary  "...")
     (boss
      (person
       (person_id "...")
       (name "...")
       (salary  "..."))))))

If we neglected the alias, we would have 'person' directly nested
under 'person', and the meaning would not be obvious. Note how the
contents of the SQL query dynamically modifies the schema/structure of
the result tree.

=head3 NOTE ON SQL SYNTAX

Right now, DBStag is fussy about how you specify aliases; you must use
B<AS> - you must say

  SELECT name FROM person AS author;

instead of

  SELECT name FROM person author;

=head3 Nesting of relations

The main utility of querying using this module is in retrieving the
nested relation elements from the flattened query results. Given a
query over relations A, B, C, D,... there are a number of possible
tree structures. Not all of the tree structures are meaningful or
useful.

Usually it will make no sense to nest A under B if there is no foreign
key relationship linking either A to B, or B to A. This is not always
the case - it may be desirable to nest A under B if there is an
intermediate linking table that is required at the relational level
but not required in the tree structure.

DBStag will guess a structure/schema based on the ordering of the
relations in your FROM clause. However, this guess can be over-ridden
at either the SQL level (using DBStag specific SQL extensions) or at
the API level.

The default algorithm is to nest each relation element under the
relation element preceeding it in the FROM clause; for instance:

  SELECT * FROM a NATURAL JOIN b NATURAL JOIN c

If there are appropriately named foreign keys, the following data will
be returned (assuming one column 'x_foo' in each of a, b and c)

  (set
   (a
    (a_foo "...")
    (b
     (b_foo "...")
     (c
      (c_foo "...")))))

where 'x_foo' is a column in relation 'x'

This is not always desirable. If both b and c have foreign keys into
table a, DBStag will not detect this - you have to guide it. There are
two ways of doing this - you can guide by bracketing your FROM clause
like this:

  SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN c

This will generate

  (set
   (a
    (a_foo "...")
    (b
     (b_foo "..."))
    (c
     (c_foo "..."))))
 
Now b and c are siblings in the tree. The algorithm is similar to
before: nest each relation element under the relation element
preceeding it; or, if the preceeding item in the FROM clause is a
bracketed structure, nest it under the first relational element in the
bracketed structure.

(Note that in MySQL you may not place brackets in the FROM clause in
this way)

Another way to achieve the same thing is to specify the desired tree
structure using a DBStag specific SQL extension. The DBStag specific
component is removed from the SQL before being presented to the
DBMS. The extension is the B<USE NESTING> clause, which should come at
the end of the SQL query (and is subsequently removed before
processing by the DBMS).

  SELECT * 
  FROM a NATURAL JOIN b NATURAL JOIN c 
  USE NESTING (set (a (b)(c)));

This will generate the same tree as above (i.e. 'b' and 'c' are
siblings). Notice how the nesting in the clause is the same as the
nesting in the resulting tree structure.

Note that 'set' is not a table in the underlying relational schema -
the result data tree requires a named top level node to group all the
'a' relations under. You can call this top level element whatever you
like.

If you are using the DBStag API directly, you can pass in the nesting
structure as an argument to the select call; for instance:

  my $xmlstr =
    $dbh->selectall_xml(-sql=>q[SELECT * 
                                FROM a NATURAL JOIN b 
                                     NATURAL JOIN c],
                        -nesting=>'(set (a (b)(c)))');

or the equivalent -

  my $xmlstr =
    $dbh->selectall_xml(q[SELECT * 
                          FROM a NATURAL JOIN b 
                               NATURAL JOIN c],
                        '(set (a (b)(c)))');

If you like, you can also use XML here (only at the API level, not at
the SQL level) -

  my $seq =
    $dbh->selectall_xml(-sql=>q[SELECT * 
                                FROM a NATURAL JOIN b 
                                     NATURAL JOIN c],
                        -nesting=>q[
                                    <set>
                                      <a>
                                        <b></b>
                                        <c></c>
                                      </a>
                                    </set>
                                   ]);

As you can see, this is a little more verbose than the S-Expression

Most command line scripts that use this module should allow
pass-through via the '-nesting' switch.

=head3 Aliasing of functions and expressions

If you alias a function or an expression, DBStag needs to know where
to put the resulting column; the column must be aliased.

This is inferred from the first named column in the function or
expression; for example, the SQL below uses the minus function:

  SELECT blah.*, foo.*, foo.x-foo.y AS z

The B<z> element will be nested under the B<foo> element

You can force different nesting using a B<double underscore>:

  SELECT blah.*, foo.*, foo.x - foo.y AS blah__z

This will nest the B<z> element under the B<blah> element

=head2 Conformance to DTD/XML-Schema

DBStag returns L<Data::Stag> structures that are equivalent to a
simplified subset of XML (and also a simplified subset of lisp
S-Expressions).

These structures are examples of B<semi-structured data> - a good
reference is this book -

  Data on the Web: From Relations to Semistructured Data and XML
  Serge Abiteboul, Dan Suciu, Peter Buneman
  Morgan Kaufmann; 1st edition (January 2000)

The schema for the resulting Stag structures can be seen to conform to
a schema that is dynamically determined at query-time from the
underlying relational schema and from the specification of the query itself.

If you need to generate a DTD you can ause the B<stag-autoschema.pl>
script, which is part of the L<Data::Stag> distribution

=head1 QUERY METHODS

The following methods are for using the DBStag API to query a database

=head2 connect

  Usage   - $dbh = DBIx::DBStag->connect($DSN);
  Returns - L<DBIx::DBStag>
  Args    - see the connect() method in L<DBI>

This will be the first method you call to initiate a DBStag object

The DSN may be a standard DBI DSN, or it can be a DBStag alias

=head2 selectall_stag

 Usage   - $stag = $dbh->selectall_stag($sql);
           $stag = $dbh->selectall_stag($sql, $nesting_clause);
           $stag = $dbh->selectall_stag(-template=>$template,
                                        -bind=>{%variable_bindinfs});
 Returns - L<Data::Stag>
 Args    - sql string, 
           [nesting string], 
           [bind hashref],
           [template DBIx::DBStag::SQLTemplate]

Executes a query and returns a L<Data::Stag> structure

An optional nesting expression can be passed in to control how the
relation is decomposed into a tree. The nesting expression can be XML
or an S-Expression; see above for details

=cut

=head2 selectall_xml

 Usage   - $xml = $dbh->selectall_xml($sql);
 Returns - string
 Args    - See selectall_stag()

As selectall_stag(), but the results are transformed into an XML string

=cut

=head2 selectall_sxpr

 Usage   - $sxpr = $dbh->selectall_sxpr($sql);
 Returns - string
 Args    - See selectall_stag()

As selectall_stag(), but the results are transformed into an
S-Expression string; see L<Data::Stag> for more details.

=cut

=head2 selectall_sax

 Usage   - $dbh->selectall_sax(-sql=>$sql, -handler=>$sax_handler);
 Returns - string
 Args    - sql string, [nesting string], handler SAX

As selectall_stag(), but the results are transformed into SAX events

[currently this is just a wrapper to selectall_xml but a genuine event
generation model will later be used]

=cut

=head2 selectall_rows

 Usage   - $tbl = $dbh->selectall_rows($sql);
 Returns - arrayref of arrayref
 Args    - See selectall_stag()

As selectall_stag(), but the results of the SQL query are left
undecomposed and unnested. The resulting structure is just a flat
table; the first row is the column headings. This is similar to
DBI->selectall_arrayref(). The main reason to use this over the direct
DBI method is to take advantage of other stag functionality, such as
templates

=head2 prepare_stag PRIVATE METHOD

 Usage   - $prepare_h = $dbh->prepare_stag(-template=>$template);
 Returns - hashref (see below)
 Args    - See selectall_stag()

Returns a hashref

      {
       sth=>$sth,
       exec_args=>\@exec_args,
       cols=>\@cols,
       col_aliases_ordered=>\@col_aliases_ordered,
       alias=>$aliasstruct,
       nesting=>$nesting
      };


=cut

=head1 STORAGE METHODS

The following methods are for using the DBStag API to store nested
data in a database

=head2 storenode

  Usage   - $dbh->storenode($stag);
  Returns - 
  Args    - L<Data::Stag>

SEE ALSO: The B<stag-storenode.pl> script

Recursively stores a stag tree structure in the database.

The database schema is introspected for most of the mapping data, but
you can supply your own (see later)

Before a node is stored, certain subnodes will be pre-stored; these are
subnodes for which there is a foreign key mapping FROM the parent node
TO the child node. This pre-storage is recursive.

After these nodes are stored, the current node is either INSERTed or
UPDATEd. The database is introspected for UNIQUE constraints; these
are used as keys. If there exists a row in the database with matching
key, then the node is UPDATEd; otherwise it is INSERTed.

(primary keys from pre-stored nodes become foreign key values in the
existing node)

Subsequently, all subnodes that were not pre-stored are now
post-stored.  The primary key for the existing node will become
foreign keys for the post-stored subnodes.

=head3 Database table and column name restrictions

Before storage, all node names are made B<DB-safe>; they are
lowercased, and the following transform is applied:

  tr/a-z0-9_//cd;

=head2 mapping

  Usage   - $dbh->mapping(["alias/table.col=fktable.fkcol"]);
  Returns - 
  Args    - array

Creates a stag-relational mapping (for storing data only)

Occasionally not enough information can be obtained from db
introspection; you can provide extra mapping data this way.

Occasionally you stag objects/data/XML will contain aliases that do
not correspond to actual SQL relations; the aliases are intermediate
nodes that provide information on which foreign key column to use

For example, with data like this:

  (person
   (name "...")
   (favourite_film
    (film (....))
   (least_favourite_film
    (film (....)))))

There may only be two SQL tables: person and film; person would have
two foreign key columns into film. The mapping may look like this

  ["favourite_film/person.favourite_film_id=film.film_id",
   "least_favourite_film/person.least_favourite_film_id=film.film_id"]

The mapping can also be supplied in the xml that is loaded; any node
named "dbstag_metadata" will not be loaded; it is used to supply the
mapping. For example:

  <personset>
    <dbstag_mapping>
      <map>favourite_film/person.favourite_film_id=film.film_id</map>
      <map>least_favourite_film/person.least_favourite_film_id=film.film_id</map>
    </dbstag_mapping>
    <person>...


=head2 mapconf

  Usage   - $dbh->mapconf("mydb-stagmap.stm");
  Returns - 
  Args    - filename

sets the conf file containing the stag-relational mappings

See mapping() above

The file contains line like:

  favourite_film/person.favourite_film_id=film.film_id
  least_favourite_film/person.least_favourite_film_id=film.film_id

=head2 noupdate_h

  Usage   - $dbh->noupdate_h({person=>1})
  Returns - 
  Args    - hashref

Keys of hash are names of nodes that do not get updated - if a unique
key is queried for and does not exist, the node will be inserted and
subnodes will be stored; if the unique key does exist in the db, then
this will not be updated; subnodes will not be stored

=head2 trust_primary_key_values

  Usage   - $dbh->trust_primary_key_values(1)
  Returns - bool
  Args    - bool (optional)

The default behaviour of the storenode() method is to remap all
B<surrogate> PRIMARY KEY values it comes across.

A surrogate primary key is typically a primary key of type SERIAL (or
AUTO_INCREMENT) in MySQL. They are identifiers assigned automatically
be the database with no semantics.

It may be desirable to store the same data in two different
databases. We would generally not expect the surrogate IDs to match
between databases, even if the rest of the data does.

(If you do not use surrogate primary key columns in your load xml,
then you can ignore this accessor)

If you use primary key columns in your XML, and the primary keys are
not surrogate, then youshould set this.  If this accessor is set to
non-zero (true) then the primary key values in the XML will be used.

If your db has surrogate/auto-increment/serial PKs, and you wish to
use these PK columns in your XML, yet you want to make XML that can be
exported from one db and imported into another, then the default
behaviour will be fine.

For example, if we extract a 'person' from a db with surrogate PK
B<id> and unique key B<ssno>, we may get this:

  <person>
    <id>23</id>
    <name>fred</name>
    <ssno>1234-567</ssno>
  </person>

If we then import this into an entirely fresh db, with no rows in
table B<person>, then the default behaviour of storenode() will create a
row like this:

  <person>
    <id>1</id>
    <name>fred</name>
    <ssno>1234-567</ssno>
  </person>

The PK val 23 has been mapped to 1 (all foreign keys that point to
person.id=23 will now point to person.id=1)

If we were to first call $sdbh->trust_primary_key_values(1), then
person.id would remain to be 23. This would only be appropriate
behaviour if we were storing back into the same db we retrieved from.

=head2 is_caching_on B<ADVANCED OPTION>

  Usage   - $dbh->is_caching_on('person', 1)
  Returns - number
  Args    - number
                   0: off (default)
                   1: memory-caching ON
                   2: memory-caching OFF, bulkload ON
                   3: memory-caching ON, bulkload ON

IN-MEMORY CACHING

By default no in-memory caching is used. If this is set to 1,
then an in-memory cache is used for any particular element. No cache
management is used, so you should be sure not to cache elements that
will cause memory overloads.

Setting this will not affect the final result, it is purely an
efficiency measure for use with storenode().

The cache is indexed by all unique keys for that particular
element/table, wherever those unique keys are set

BULKLOAD

If bulkload is used without memory-caching (set to 2), then only
INSERTs will be performed for this element. Note that this could
potentially cause a unique key violation, if the same element is
present twice

If bulkload is used with memory-caching (set to 3) then only INSERTs
will be performed; the unique serial/autoincrement identifiers for
those inserts will be cached and used. This means you can have the
same element twice. However, the load must take place in one session,
otherwise the contents of memory will be lost

=cut

=head1 SQL TEMPLATES

DBStag comes with its own SQL templating system. This allows you to
reuse the same canned SQL or similar SQL qeuries in different
contexts. See L<DBIx::DBStag::SQLTemplate>

=head2 find_template

  Usage   - $template = $dbh->find_template("my-template-name");
  Returns - L<DBIx::DBStag::SQLTemplate>
  Args    - str

Returns an object representing a canned paramterized SQL query. See
L<DBIx::DBStag::SQLTemplate> for documentation on templates

=head2 list_templates

  Usage   - $templates = $dbh->list_templates();
  Returns - Arrayref of L<DBIx::DBStag::SQLTemplate>
  Args    - 

Returns a list of ALL defined templates - See
L<DBIx::DBStag::SQLTemplate>

=head2 find_templates_by_schema

  Usage   - $templates = $dbh->find_templates_by_schema($schema_name);
  Returns - Arrayref of L<DBIx::DBStag::SQLTemplate>
  Args    - str

Returns a list of templates for a particular schema - See
L<DBIx::DBStag::SQLTemplate>

=head2 find_templates_by_dbname

  Usage   - $templates = $dbh->find_templates_by_dbname("mydb");
  Returns - Arrayref of L<DBIx::DBStag::SQLTemplate>
  Args    - db name

Returns a list of templates for a particular db

Requires resources to be set up (see below)


=cut

=head1 RESOURCES

Generally when connecting to a database, it is necessary to specify a
DBI style DSN locator. DBStag also allows you specify a B<resource
list> file which maps logical names to full locators

The following methods allows you to use a resource list

=head2 resources_list

  Usage   - $rlist = $dbh->resources_list
  Returns - arrayref to a hashref
  Args    - none

Returns a list of resources; each resource is a hash
  
  {name=>"mydbname",
   type=>"rdb",
   schema=>"myschema",
  }

=head1 SETTING UP RESOURCES

The above methods rely on you having a file describing all the
relational dbs available to you, and setting the env var
DBSTAG_DBIMAP_FILE set (this is a B<:> separated list of paths).

B<This is alpha code - not fully documented, API may change>

Currently a resources file is a whitespace delimited text file -
XML/Sxpr/IText definitions may be available later

Here is an example of a resources file:

  # LOCAL
  mytestdb         rdb        Pg:mytestdb                      schema=test
  
  # SYSTEM
  worldfactbook    rdb      Pg:worldfactbook@db1.mycompany.com  schema=wfb
  employees        rdb      Pg:employees@db2.mycompany.com      schema=employees

The first column is the B<nickname> or B<logical name> of the
resource/db. This nickname can be used instead of the full DBI locator
path (eg you can just use B<employees> instead of
B<dbi:Pg:dbname=employees;host=db2.mycompany.com>

The second column is the resource type - rdb is for relational
database. You can use the same file to track other system datasources
available to you, but DBStag is only interested in relational dbs.

The 3rd column is a way of locating the resource - driver:name@host

The 4th column is a B<;> separated list of B<tag>=B<value> pairs; the
most important tag is the B<schema> tag. Multiple dbs may share the
same schema, and hence share SQL Templates

=cut

=head1 COMMAND LINE SCRIPTS

DBStag is usable without writing any perl, you can use command line
scripts and files that utilise tree structures (XML, S-Expressions)

=over

=item selectall_xml.pl

 selectall_xml.pl -d <DSN> [-n <nestexpr>] <SQL>

Queries database and writes decomposed relation as XML

Can also be used with templates:

 selectall_xml.pl -d <DSN> /<templatename> <var1> <var2> ... <varN>

=item selectall_html.pl

 selectall_html.pl -d <DSN> [-n <nestexpr>] <SQL>

Queries database and writes decomposed relation as HTML with nested
tables indicating the nested structures.

=item stag-storenode.pl

 stag-storenode.pl -d <DSN> <file>

Stores data from a file (Supported formats: XML, Sxpr, IText - see
L<Data::Stag>) in a normalized database. Gets it right most of the time.

TODO - metadata help

=item stag-autoddl.pl

 stag-autoddl.pl [-l <linktable>]* <file>

Takes data from a file (Supported formats: XML, Sxpr, IText - see
L<Data::Stag>) and generates a relational schema in the form of SQL
CREATE TABLE statements.

=back

=head1 ENVIRONMENT VARIABLES

=over

=item DBSTAG_TRACE

setting this environment will cause all SQL statements to be printed
on STDERR, as well as a full trace of how nodes are stored

=back

=head1 BUGS

The SQL parsing can be quite particular - sometimes the SQL can be
parsed by the DBMS but not by DBStag. The error messages are not
always helpful.

There are probably a few cases the SQL SELECT parsing grammar cannot deal with.

If you want to select from views, you need to hack DBIx::DBSchema (as of v0.21)

=head1 TODO

Use SQL::Translator to make SQL DDL generation less Pg-specific; also
for deducing foreign keys (right now foreign keys are guessed by the
name of the column, eg table_id)

Can we cache the grammar so that startup is not so slow?

Improve algorithm so that events are fired rather than building up
entire structure in-memory

Tie in all DBI attributes accessible by hash, i.e.: $dbh->{...}

Error handling

=head1 WEBSITE

L<http://stag.sourceforge.net>

=head1 AUTHOR

Chris Mungall <F<cjm AT fruitfly DOT org>>

=head1 COPYRIGHT

Copyright (c) 2004 Chris Mungall

This module is free software.
You may distribute this module under the same terms as perl itself

=cut



1;

