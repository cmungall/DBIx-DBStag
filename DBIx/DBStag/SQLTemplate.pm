# $Id: SQLTemplate.pm,v 1.3 2003/05/27 06:48:38 cmungall Exp $
# -------------------------------------------------------
#
# Copyright (C) 2003 Chris Mungall <cjm@fruitfly.org>
#
# This module is free software.
# You may distribute this module under the same terms as perl itself

#---
# POD docs at end of file
#---

package DBIx::DBStag::SQLTemplate;

use strict;
use vars qw($VERSION @ISA @EXPORT_OK %EXPORT_TAGS $DEBUG $AUTOLOAD);
use Carp;
use DBI;
use Data::Stag qw(:all);
use DBIx::DBStag;
use DBIx::DBStag::Constraint;
use Text::Balanced qw(extract_bracketed);
#use SQL::Statement;
use Parse::RecDescent;
$VERSION = '0.01';


sub DEBUG {
    $DBIx::DBStag::DEBUG = shift if @_;
    return $DBIx::DBStag::DEBUG;
}

sub trace {
    my ($priority, @msg) = @_;
    return unless $ENV{DBSTAG_TRACE};
    print "@msg\n";
}

sub dmp {
    use Data::Dumper;
    print Dumper shift;
}


sub new {
    my $proto = shift; 
    my $class = ref($proto) || $proto;

    my $self = {};
    bless $self, $class;
    $self->cached_sth({});
    $self;
}

sub name {
    my $self = shift;
    $self->{_name} = shift if @_;
    return $self->{_name};
}

sub fn {
    my $self = shift;
    $self->{_fn} = shift if @_;
    return $self->{_fn};
}


sub sth {
    my $self = shift;
    $self->{_sth} = shift if @_;
    return $self->{_sth};
}

sub cached_sth {
    my $self = shift;
    $self->{_cached_sth} = shift if @_;
    return $self->{_cached_sth};
}


sub sql_clauses {
    my $self = shift;
    $self->{_sql_clauses} = shift if @_;
    return $self->{_sql_clauses};
}

sub set_clause {
    my $self = shift;
    my $ct = lc(shift);
    my $v = shift;
    $v =~ s/^ *//;
    my $add = 0;
    if ($v =~ /\+(.*)/) {
	$v = $1;
	$add = 1;
    }
    my $is_set = 0;
    my $clauses = $self->sql_clauses;
    foreach my $clause (@$clauses) {
	if (lc($clause->{name}) eq $ct) {
	    if ($add && $clause->{value}) {
		$clause->{value} .= " and $v";
	    }
	    else {
		$clause->{value} = $v;
	    }
	    $is_set = 1;
	}
    }
    $self->throw("Cannot set $ct") unless $is_set;
    return;
}

sub properties {
    my $self = shift;
    $self->{_properties} = shift if @_;
    return $self->{_properties};
}


# given a template and a binding, this will
# create an SQL statement and a list of exec args
# - the exec args correspond to ?s in the SQL
#
# for example WHERE foo = &foo&
# called with binding foo=bar
#
# will become WHERE foo = ?
# and the exec args will be ('bar')
#
# if the template contains option blocks eg
#
# WHERE [foo = &foo&]
#
# then the part in square brackets will only be included if
# there is a binding for variable foo
#
# if multiple option blocks are included, they will be ANDed
#
#
# if this idiom appears
#
# WHERE foo => &foo&
#
# then the operator used will either be =, LIKE or IN
# depending on the value of the foo variable
#
# if the foo variable contains % it will be LIKE
# if the foo variable contains an ARRAY it will be IN
#
# (See DBI manpage for discussion of placeholders)
##
#sub zget_sql_and_args {
#    my $self = shift;
#    my $bind = shift;

#    my @args = ();
#    my %argh = ();

#    if ($bind &&
#	ref($bind) eq 'HASH') {
#	%argh = %$bind;
#    }
#    if ($bind &&
#	ref($bind) eq 'ARRAY') {
#	@args = @$bind;
#    }

#    my $sql_clauses = $self->sql_clauses;
#    my $sql = '';

#    foreach my $clause (@$sql_clauses) {
#	my ($n, $v) = ($clause->{name}, $clause->{value});
#	trace "N=$n; V=$v\n";
#	if (lc($n) eq 'where') {
#	    my $vari = 0;
#	    my %vari_by_name = ();
#	    my $sub =
#	      sub {
#		  my $str = shift;
#		  my $is_set = 1;
#		  while ($str =~ /(=>)?\s*\&(\w+)\&/) {
#		      my $op = $1 || '';
#		      my $varname = $2;
#		      $is_set = 0;
#		      if (%argh) {
#			  my $argval = $argh{$varname};
#			  if (!exists $argh{$varname}) {
##			      $self->throw("not set $varname");
#			  }
#			  else {
#			      $args[$vari] = $argval;
#			      $is_set = 1;
#			  }
#		      }
		      
#		      if (@args > $vari) {
#			  $is_set = 1;
#		      }
#		      # if var appears twice, it is already bound
#		      if (@args <= $vari &&
#			  defined($vari_by_name{$varname})) {
#			  $args[$vari] =
#			    $args[$vari_by_name{$varname}];
#		      }
#		      push(@{$vari_by_name{$varname}}, $vari);

#		      if ($is_set) {
#			  my $val = $args[$vari];
#			  if ($op) {
#			      $op = '= ';
#			      if ($val =~ /\%/) {
#				  $op = ' like ';
#			      }
#			  }
#			  if (ref($val)) {
#			      my $vals =
#				join(',',
#				     map {$self->dbh->quote($_)} @$val);
#			      $str =~ s/(=>)?\s*\&$varname\&/ in \($vals\)/;
#			  }
#			  else {
#			      $str =~ s/(=>)?\s*\&$varname\&/$op\?/;
#			      $vari++;
#			  }
#		      }
#		      else {
#			  $str = '';
#		      }
#		  }
#		  return $str;
#	      };
#	    my @constrs = ();
#	    while (1) {
#		my ($extracted, $remainder, $skip) =
#		  extract_bracketed($v, '[]');
#		print "($extracted, $remainder, $skip)\n";
#		$remainder =~ s/^\s+//;
#		$remainder =~ s/\s+$//;
#		$skip =~ s/^\s+//;
#		$skip =~ s/\s+$//;
		
#		push(@constrs,
#		     $sub->($skip));
#		if ($extracted) {
#		    $extracted =~ s/^\s*\[//;
#		    $extracted =~ s/\]\s*$//;
#		    push(@constrs,
#			 $sub->($extracted));
#		}
#		else {
#		    push(@constrs,
#			 $sub->($remainder));
#		    last;
#		}
#		$v = $remainder;
#	    }
#	    @constrs = grep {$_} @constrs;
#	    $v = join(' AND ', @constrs);
#            trace(0, join(';', @constrs));
#	}
#	$sql .= "$n $v\n";
#    }
#    return ($sql, @args);
#}

sub get_sql_and_args {
    my $self = shift;
    my $bind = shift || {};

    my $where_blocks = $self->split_where_clause;
    my $varnames = $self->get_varnames; # ORDERED list of variables in Q

    my %argh = ();
    my ($sql, @args);
    my $where;

    # binding can be a simple array of VARVALs
    if ($bind &&
	ref($bind) eq 'ARRAY') {

        # assume that the order of arguments specified is
        # the same order that appears in the query
        for (my $i=0; $i<@$bind; $i++) {
            $argh{$varnames->[$i]} = $bind->[$i];
        }
    }
    if ($bind &&
	ref($bind) eq 'HASH') {
	%argh = %$bind;
    }
    if (%argh) {
        # simple rules for substituting variables
        ($where, @args) = $self->_get_sql_where_and_args_from_hashmap(\%argh);
    }
    else {
        # COMPLEX BOOLEAN CONSTRAINTS
        my $constr;
        $constr = $bind;
        ($where, @args) = $self->_get_sql_where_and_args_from_constraints($constr);
    }
    
    my $sql_clauses = $self->sql_clauses;
    $sql = join("\n",
                map {
                    if (lc($_->{name}) eq 'where') {
                        "WHERE $where";
                    }
                    else {
                        "$_->{name} $_->{value}";
                    }
                } @$sql_clauses);
    return ($sql, @args);
}

# takes a simple set of hash variable bindings, and
# a set of option blocks [...][...]
#
# generates SQL for every block required, replaces with
# DBI placeholders, and returns SQL plus DBI execute args list
sub _get_sql_where_and_args_from_hashmap {
    my $self = shift;
    my %argh = %{shift || {}};

    my $where_blocks = $self->split_where_clause;

    # sql clauses to be ANDed
    my @sqls = ();

    # args to be fed to DBI execute() [corresponds to placeholder ?s]
    my @args = ();

    # index of variables replaced by ?s
    my $vari = 0;

  NEXT_BLOCK:
    foreach my $wb (@$where_blocks) {
        my $where = $wb->{text};
        my $varnames = $wb->{varnames};
        
        my $str = $where;
        while ($str =~ /(=>)?\s*\&(\w+)\&/) {
            my $op = $1 || '';
            my $varname = $2;

            my $argval = $argh{$varname};
            if (!exists $argh{$varname}) {
                next NEXT_BLOCK;
            }
                
                
            if ($op) {
                $op = '= ';
                if ($argval =~ /\%/) {
                    $op = ' like ';
                }
            }
            my $replace_with;
            # replace arrays with IN (1,2,3,...)
            if (ref($argval)) {
                $replace_with =
                  join(',',
                       map {$self->dbh->quote($_)} @$argval);
                $op = ' in ';
            }
            else {
                $replace_with = '?';
                $args[$vari] = $argval;
                $vari++;
            }
            $str =~ s/(=>)?\s*\&$varname\&/$op$replace_with/;
        }
        push(@sqls, $str);
    }
    my $sql = join(' AND ', @sqls);
    trace(0, "WHERE:$sql");
    return ($sql, @args);
}

# takes complex boolean constraints and generates SQL
sub _get_sql_where_and_args_from_constraints {
    my $self = shift;
    my $constr = shift;

    if ($constr->is_leaf) {
        my $where_blocks = $self->split_where_clause;
        die("TODO");
    }
    else {
        my $bool = $constr->bool;
        my $children = $constr->children;
        my @all_args = ();
        my @sqls = ();
        foreach my $child (@$children) {
            my ($sql, @args) = $self->_get_sql_where_and_args($constr);            
            push(@sqls, $sql);
            push(@all_args, @args);
        }
        my $sql = '('.join(" $bool ",
                           @sqls).')';
        return ($sql, @all_args);
    }
    $self->throw("ASSERTION ERROR");
}


# splits a WHERE clause with option blocks [ x=&x& ] [ y=&y& and z=&z& ] into
# blocks, and attaches the variable names to the block
sub split_where_clause {
    my $self = shift;
    my $sql_clauses = $self->sql_clauses;
    my $sql = '';

    my ($clause) = grep {lc($_->{name}) eq 'where'} (@$sql_clauses);
    my $where = $clause->{value} || '';

    my $vari = 0;
    my %vari_by_name = ();
    my $sub =
      sub {
          my $textin = shift;
          return unless $textin;
          my $str = $textin;

          my @varnames = ();
          while ($str =~ /(=>)?\s*\&(\w+)\&/) {
              my $op = $1 || '';
              my $varname = $2;
              push(@varnames, $varname);
              $str =~ s/(=>)?\s*\&$varname\&//;
          }
          return
            {text=>$textin,
             varnames=>\@varnames}
        };
    my @constrs = ();
    while (1) {
        my ($extracted, $remainder, $skip) =
          extract_bracketed($where, '[]');
        $extracted ||= '';
        $remainder ||= '';
        trace(0, "($extracted, $remainder, $skip)\n");
        $remainder =~ s/^\s+//;
        $remainder =~ s/\s+$//;
        $skip =~ s/^\s+//;
        $skip =~ s/\s+$//;
        
        push(@constrs,
             $sub->($skip));
        if ($extracted) {
            $extracted =~ s/^\s*\[//;
            $extracted =~ s/\]\s*$//;
            push(@constrs,
                 $sub->($extracted));
        }
        else {
            push(@constrs,
                 $sub->($remainder));
            last;
        }
        $where = $remainder;
    }
    @constrs = grep {$_} @constrs;
    return \@constrs;
}

sub get_varnames {
    my $self = shift;
    my $parts = $self->split_where_clause;
    return [map {@{$_->{varnames}}} @$parts];
}

sub prepare {
    my $self = shift;
    my $dbh = shift;
    my $bind = shift;
    my ($sql, @exec_args) = $self->get_sql_and_args($bind);
    my $sth = $self->cached_sth->{$sql};
    if (!$sth) {
	$sth = $dbh->prepare($sql);
	$self->cached_sth->{$sql} = $sth;	  
    }
    return ($sql, $sth, @exec_args);
}

sub parse {
    my $self = shift;
    my $fn = shift;
    my $fh = FileHandle->new($fn) || $self->throw("cannot open $fn");
    $self->fn($fn);
    my $name = $fn;
    $name =~ s/.*\///;
    $name =~ s/\.\w+$//;
    $self->name($name);

    my $eosql_tag_idx;
    my $tag = {name=>'', value=>''};
    my @tags = ();
    while (<$fh>) {
	chomp;
	if (/^\/\//) {
	    $eosql_tag_idx = scalar(@tags)+1;
	    next;
	}
	if (/^:(\w+)\s*(.*)/) {
	    push(@tags, $tag);
	    $tag = {name=>$1, value => $2};
	}
	elsif (/^(\w+):\s*(.*)/) {
	    push(@tags, $tag);
	    $tag = {name=>$1, value => $2};
	}
	else {
	    if (substr($_, -1) eq '\\') {
	    }
	    else {
		$_ = "$_ ";
	    }
	    $tag->{value} .= $_;
	}
    }
    push(@tags, $tag);
    if (!defined($eosql_tag_idx)) {
	$eosql_tag_idx = scalar(@tags);
    }
    my @clauses = splice(@tags, 0, $eosql_tag_idx);
    if (!@clauses) {
	$self->throw("No SQL in $fn");
    }
    if (@clauses == 1 && !$clauses[0]->{name}) {
	my $j = join('|',
		     'select',
		     'from',
		     'where',
		     'order',
		     'limit',
		     'group',
		     'having',
		     'use nesting',
		    );
	my @parts =
	  split(/($j)/i, $clauses[0]->{value});
	@clauses = ();
	while (my ($n, $v) = splice(@parts, 0, 2)) {
	    push(@clauses, {name=>$n, value=>$v});
	}
    }
    $self->sql_clauses(\@clauses);
    $self->properties(\@tags);
    $fh->close;
}

sub throw {
    my $self = shift;
    my $fmt = shift;

    print STDERR "\nERROR:\n";
    printf STDERR $fmt, @_;
    print STDERR "\n";
    confess;
}



1;

__END__

=head1 NAME

  DBIx::DBStag::SQLTemplate - A Template for an SQL query

=head1 SYNOPSIS

  $template = $dbh->find_template("mydb-myquery");
  $xml = $dbh->selectall_xml(-template=>$template, 
                             -bind=>{name => "fred"});
 

=cut

=head1 DESCRIPTION

A template represents a canned query that can be parameterized.

Templates are collected in directories (in future it will be possible
to store them in files or in the db itself).

To tell DBStag where your templates are, you should set:

  setenv DBSTAG_TEMPLATE_DIRS "$HOME/mytemplates:/data/bioconf/templates"

Your templates should end with the suffix .stg

A template file should contain at minimum some SQL; for example:


=over

=item Example template 1

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
               [movie.genre = &genre&] [star.lastname = &lastname&]
  USE NESTING (set(studio(movie(star))))

Thats all! However, there are ways to make your template more useful


=item Example template 2



  :SELECT 
               studio.*,
               movie.*,
               star.*
  :FROM
               studio NATURAL JOIN 
               movie NATURAL JOIN
               movie_to_star NATURAL JOIN
               star
  :WHERE
               [movie.genre = &genre&] [star.lastname = &lastname&]
  :USE NESTING (set(studio(movie(star))))

  //
  desc: query for fetching movies

By including :s at the beginning it makes it easier for parsers to
assemble SQL (this is not necessary for DBStag however)

After the // you can add tag: value data.

=back

=head2 VARIABLES

WHERE clause variables in the template look like this

  &foo&

variables are bound at query time

  my $set = $dbh->selectall_stag(-template=>$t,
                                 -bind=>["bar"]);

or

  my $set = $dbh->selectall_stag(-template=>$t,
                                 -bind=>{foo=>"bar"});

If the former is chosen, variables are bound from the bind list as
they are found

=head2 OPTIONAL BLOCKS

  WHERE [ foo = &foo& ]

If foo is not bound then the part between the square brackets is left out

Multiple option blocks are ANDed together

=head2 BINDING OPERATORS

The operator can be bound at query time too

  WHERE [ foo => &foo& ]

Will become either

  WHERE foo = ?

or

  WHERE foo LIKE ?

or

  WHERE foo IN (f0, f1, ..., fn)

Depending on whether foo contains the % character, or if foo is bound
to an ARRAY

=head1 WEBSITE

L<http://stag.sourceforge.net>

=head1 AUTHOR

Chris Mungall <F<cjm@fruitfly.org>>

=head1 COPYRIGHT

Copyright (c) 2003 Chris Mungall

This module is free software.
You may distribute this module under the same terms as perl itself

=cut



1;

