#$Id: Makefile.PL,v 1.1 2003/04/23 00:59:29 cmungall Exp $

# This Makefile.PL was cut-n-pasted from the DBIx::Abstract
# Nakefile.PL, by Andrew Turner <turnera@cpan.org>

use strict;
BEGIN { $^W = 1 } # use warnings in Perl 5.6 parlance
use ExtUtils::MakeMaker qw( prompt WriteMakefile );
use Config ();
use Getopt::Long();
use Data::Dumper ();
use lib '.';
use lib 't';

use vars qw($opt);
$opt =
  { "help" => \&Usage,
  };
Getopt::Long::GetOptions($opt, "help", 
                         "testdsn=s", 
#                         "testdb=s", 
                         "testhost=s",
# 			 "testport=s", 
#                         "testdriver=s",
			 "testuser=s", 
                         "testpassword=s",
			 "testrecreate=s",
			 );

my $hostflag = $ENV{TESTHOST} || $opt->{testhost};
my $hostdsn = $hostflag ? ";host=$hostflag" : '';
$hostflag = $hostflag ? "-h $hostflag" : '';

my $TESTDB  = $ENV{TESTDB} || "test";
my $TESTDRIVER = "Pg";
my $TESTDSN  = $ENV{TESTDSN} || "dbi:Pg:dbname=test$hostdsn";
my $TESTRECREATE  = $ENV{TESTRECREATE} || "dropdb $hostflag test; createdb $hostflag test";


my $source = {};

foreach my $key (qw/testdsn testuser testpassword testrecreate/) {
  Configure($opt, $source, $key);
}

print <<"MSG";
I will use the following settings for compiling and testing:

MSG

delete $opt->{'help'};
my $keylen = 0;
foreach my $key (keys %$opt) {
  $keylen = length($key) if length($key) > $keylen;
}
my $slen = 0;
foreach my $val (values %$source) {
  $slen = length($val) if length($val) > $slen;
}
foreach my $key (sort { $a cmp $b} keys %$opt) {
  printf("  %-" . $keylen . "s (%-" . $slen . "s) = %s\n",
	 $key, $source->{$key}, $opt->{$key})
}

print <<"MSG";

To change these settings, see 'perl Makefile.PL --help'.

MSG

#sleep 5;

eval { require File::Spec };
my $fileName = $@ ?
  "t/db.config" : File::Spec->catfile("t", "db.config");
#die "Failed to determine location of $fileName" unless -f $fileName;
if (open(FILE, ">$fileName")) {
    print FILE "{ my " . Data::Dumper->Dump([$opt], ["opt"]) .
               "  sub connect_args { return (\n".
               "      \$opt->{'testdsn'},\n" .
               "      \$opt->{'testuser'},\n" .
               "      \$opt->{'testpassword'},\n" .
               "      ) }\n".
               "  sub recreate_cmd { return (\n".
               "      \$opt->{'testrecreate'},\n" .
               "      ) }\n".
               "} 1;\n";
    close(FILE) or die "Failed to create $fileName: $!";
}

WriteMakefile(
	      'NAME'         => "DBIx",
	      'ABSTRACT'     => 'DBIStag',
	      'AUTHOR'       => 'Chris Mungall <cjm@fruitfly.org>',
	      'dist'  => { COMPRESS    => 'gzip -9f',
			   SUFFIX      => 'gz'},
	      'VERSION_FROM' => "DBIx/DBIStag.pm",
	      'PREREQ_PM'    => {
				 'Data::Stag'         => 0,
				},
	      'EXE_FILES'    => [
				],
	      
	      clean => {
			FILES => 'DBIx-DBIStag-$(VERSION).tar.gz',
		       },
	     );

use DBIStagTest;
eval {
    dbh()->disconnect;
};
if ($@) {
    print STDERR <<EOM
===================================
** WARNING **

Could not connect using these parameters; test suite will probably fail

EOM
}

{

package MY;

sub processPL {
    my($self) = shift;
    my @output;
    foreach (keys(%{$self->{'PL_FILES'}})) {
        my $from = $_;
        my $to = $self->{'PL_FILES'}{$_};
        my $cfg = 'config.pl';
        push(@output, qq|
pm_to_blib: $to

$to: $from
	\$(PERL) -I\$(INST_ARCHLIB) -I\$(INST_LIB) -I\$(PERL_ARCHLIB) \\
			-I\$(PERL_LIB) -Ilib -MExtUtils::PerlPP \\
			-e ppp "$from" "$to" "$cfg"

|);
    }
    join('', @output);
}

}

sub Configure {
    my($opt, $source, $param) = @_;

    if (exists($opt->{$param})) {
        $source->{$param} = "Users choice";
        return;
    }

    if ($param eq "testdriver") {
        $source->{$param} = "default";
        $opt->{$param} = $TESTDRIVER;
    } elsif ($param eq "testdb") {
        $source->{$param} = "default";
        $opt->{$param} = $TESTDB;
    } elsif ($param eq "testdsn") {
        $source->{$param} = "default";
        $opt->{$param} = $TESTDSN;
    } elsif ($param eq "testrecreate") {
        $source->{$param} = "default";
        $opt->{$param} = $TESTRECREATE;
    } elsif ($param eq "testuser"  || $param eq "testpassword" || 
             $param eq "testdsn" || $param eq "testhost" ||
             $param eq "testport") {
        $source->{$param} = "default";
        $opt->{$param} = "";
    } else {
        die "Unknown configuration parameter: $param";
    }
}


sub Usage {
  print STDERR <<"USAGE";
Usage: perl $0 [options]

Possible options are:


  --testhost=<host>      Use the DBMS on <host> for testing
                         defaults to empty (localhost)

  --testdsn=<db>         Use the DBI datasource <dsn> for running the test suite
                         defaults to $TESTDSN

  --testuser=<user>      Use the username <user> for running the test suite;
                         defaults to no username

  --testpassword=<pwd>   Use the password <pwd> for running the test suite;
                         defaults to no password

  --testrecreate=<cmd>   Unix command string for dropping and creating the db

  --help                 Print this message and exit

Examples:

perl Makefile.PL

  uses the postgresql database 'test' on localhost

perl Makefile.PL --testhost mydbserver

  uses the postgresql database 'test' on host mydbserver

perl Makefile.PL -testdsn 'dbi:Pg:dbname=mytestdb;host=mydbserver' -mytestdbrecreate 'dropdb -h mydbserver mytestdb; createdb -h mydbserver mytestdb'

  uses the postgresql database 'mytestdb' on host mydbserver

Note: the options are only important for running the test suite - if
you are willing to risk an install without running the test suite,
just do this:

perl Makefile.PL
make install

USAGE
  exit 1;
}


__END__

