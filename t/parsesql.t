use lib 't';

BEGIN {
    # to handle systems with no installed Test module
    # we include the t dir (where a copy of Test.pm is located)
    # as a fallback
    eval { require Test; };
    use Test;    
    plan tests => 1;
}
use DBIx::DBStag;
use FileHandle;
use strict;

my $dbh = DBIx::DBStag->new;

if (1) {
    my $sql =
      q[
	SELECT * FROM x NATURAL JOIN y
       ];
    
    my $s = $dbh->parser->selectstmt($sql);
    print $s->sxpr; 
    my @cols = $s->get_cols->get_col;
    ok(@cols == 1);
    ok($cols[0]->get_name eq '*');
    my $f = $s->get_from;
    my @tbls = sort map {$_->get_name} $f->find_leaf;
    print "@tbls\n";
    ok("@tbls" eq "x y");
}
if (1) {
    my $sql =
      q[
	SELECT a, b AS y FROM x
       ];
    
    my $s = $dbh->parser->selectstmt($sql);
    print $s->sxpr; 
    my @cols = $s->get_cols->get_col;
    ok(@cols == 2);
    ok($cols[0]->get_name eq 'a');
    my $f = $s->get_from;
}
if (1) {
    my $sql =
      q[
	SELECT somefunc(x.foo), func2(bar), func3(y) AS r FROM x
       ];
    
    my $s = $dbh->parser->selectstmt($sql);
    print $s->sxpr; 
    my @cols = $s->get_cols->get_col;
    ok(@cols == 3);
    ok($cols[0]->get_func->get_name eq 'somefunc');
    ok($cols[0]->get_func->get_args->get_col->get_name eq 'x.foo');
    ok($cols[1]->get_func->get_args->get_col->get_name eq 'bar');
}
if (1) {

    # TODO - expressions
    my $sql =
      q[
	SELECT 5+3 FROM x
       ];
    
    my $s = $dbh->parser->selectstmt($sql);
#    print $s->sxpr; 
}

if (1) {
    my $sql =
      q[
SELECT 
  transcript.name, transcript_loc.nbeg, transcript_loc.nend, exon.name, exon_loc.nbeg, exon_loc.nend 
FROM
  feature_relationship INNER JOIN 
  f_type AS transcript ON (feature_relationship.subjfeature_id = transcript.feature_id)
  INNER JOIN featureloc AS transcript_loc ON (transcript_loc.feature_id = transcript.feature_id)
  INNER JOIN f_type AS exon ON (feature_relationship.objfeature_id = exon.feature_id)
  INNER JOIN featureloc AS exon_loc ON (exon_loc.feature_id = exon.feature_id)
WHERE 
  transcript.type = 'transcript' AND
  exon.type = 'exon' AND
  transcript.name = 'CG12345-RA';

       ];
    
    my $s = $dbh->parser->selectstmt($sql);
    print $s->sxpr; 
}
