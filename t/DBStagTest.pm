package DBIStagTest;
use strict;
use base qw(Exporter);
use DBIx::DBIStag;

BEGIN {
    use Test;
    if (0) {
        plan tests=>1;
        skip(1, 1);
        exit 0;
    }
}

use vars qw(@EXPORT);

#our $dbname = "dbistagtest";
#our $testdb = "dbi:Pg:dbname=$dbname;host=localhost";


@EXPORT = qw(connect_to_cleandb dbh drop);

sub dbh {
    # this file defines sub connect_args()
    unless (defined(do 'db.config')) {
        die $@ if $@;
        die "Could not reade db.config: $!\n";
    }
    my $dbh;
    my @conn = connect_args();

    eval {
        $dbh = DBIx::DBIStag->connect(@conn);    
    };
    if (!$dbh) {
        printf STDERR "COULD NOT CONNECT USING DBI->connect(@conn)\n\n";
        die;
    }
    $dbh;
}

*connect_to_cleandb = \&dbh;

sub ddl {
    my $dbh = dbh();
    my $ddl = shift;
    
}

sub alltbl {
    qw(person2address person address );
}

sub drop {
    unless (defined(do 'db.config')) {
        die $@ if $@;
        die "Could not reade db.config: $!\n";
    }
    my $cmd = recreate_cmd();
    if (system($cmd)) {
	print STDERR "PROBLEM recreating using: $cmd\n";
    }
}

sub zdrop {
#    my @t = @_;
    my @t = alltbl;
    my $dbh = dbh();
    my %created = ();
    if (1) {
	use DBIx::DBSchema;
	my $s = DBIx::DBSchema->new_native($dbh->dbh);
	use Data::Dumper;
	%created = map {$_=>1} $s->tables;
    }
    
#    foreach (@t) {
#        eval {
#            $dbh->do("DROP TABLE $_");
#        };
    
    foreach (@t) {
	if ($created{$_}) {
	    eval {
		$dbh->do("DROP TABLE $_");
	    };
	}
    }
    $dbh->disconnect;
}

1;
