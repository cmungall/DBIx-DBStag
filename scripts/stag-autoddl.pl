#!/usr/local/bin/perl -w

=head1 NAME 

stag-autoddl.pl

=head1 SYNOPSIS

  stag-autoddl.pl -parser XMLAutoddl -handler ITextWriter file1.txt file2.txt

  stag-autoddl.pl -parser MyMod::MyParser -handler MyMod::MyWriter file.txt

=head1 DESCRIPTION

script wrapper for the Data::Stag modules

=head1 ARGUMENTS

=cut



use strict;

use Carp;
use Data::Stag qw(:all);
use DBIx::DBStag;
use Getopt::Long;

my $parser = "";
my $handler = "";
my $mapf;
my $tosql;
my $toxml;
my $toperl;
my $debug;
my $help;
my @link = ();
GetOptions(
           "help|h"=>\$help,
           "parser|format|p=s" => \$parser,
           "handler|writer|w=s" => \$handler,
           "xml"=>\$toxml,
           "perl"=>\$toperl,
           "debug"=>\$debug,
           "link|l=s@"=>\@link,
          );
if ($help) {
    system("perldoc $0");
    exit 0;
}

my $db = DBIx::DBStag->new;

my @files = @ARGV;
foreach my $fn (@files) {

    my $tree = 
      Data::Stag->parse($fn, 
                        $parser);
    print $db->autoddl($tree, \@link);
}

