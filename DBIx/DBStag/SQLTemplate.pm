# $Id: SQLTemplate.pm,v 1.1 2003/05/22 01:32:24 cmungall Exp $
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

sub get_sql_and_args {
    my $self = shift;
    my $bind = shift;

    my @args = ();
    my %argh = ();

    if ($bind &&
	ref($bind) eq 'HASH') {
	%argh = %$bind;
    }
    if ($bind &&
	ref($bind) eq 'ARRAY') {
	@args = @$bind;
    }

    my $sql_clauses = $self->sql_clauses;
    my $sql = '';
    my %varh = ();
    my $vari = 0;
    foreach my $clause (@$sql_clauses) {
	my ($n, $v) = ($clause->{name}, $clause->{value});
	trace "N=$n; V=$v\n";
	if (lc($n) eq 'where') {
	    while ($v =~ /\&(\w+)\&/) {
		my $varname = $1;
		if (%argh) {
		    my $argval = $argh{$varname};
		    if (!exists $argh{$varname}) {
			$self->throw("not set $varname");
		    }
		    $args[$vari] = $argval;
		}
		$varh{$vari} = $varname;
		$v =~ s/\&$varname\&/\?/;
		$vari++;
	    }
	}
	$sql .= " $n $v";
    }
    return ($sql, @args);
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


=cut

=head1 DESCRIPTION

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

