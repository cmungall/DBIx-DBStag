use lib 't';

BEGIN {
    # to handle systems with no installed Test module
    # we include the t dir (where a copy of Test.pm is located)
    # as a fallback
    eval { require Test; };
    use Test;    
    use DBStagTest;
    plan tests => 3;
}
use DBIx::DBStag;
use DBI;
use Data::Stag;
use FileHandle;
use strict;

open(F, "t/data/chado-cvterm.sql") || die;
my $ddl = join('',<F>);
close(F);
drop();
my $dbh = connect_to_cleandb();
#DBI->trace(1);

$dbh->do($ddl);

my $chado  = Data::Stag->parse("t/data/test.chadoxml");
$dbh->storenode($_) foreach $chado->subnodes;
ok(1);
my $termset =
  $dbh->selectall_stag(q[
SELECT * 
FROM cvterm
 INNER JOIN dbxref ON (cvterm.dbxref_id = dbxref.dbxref_id)
 INNER JOIN db ON     (dbxref.db_id = db.db_id)
 INNER JOIN cv ON     (cvterm.cv_id = cv.cv_id)
WHERE
 cvterm.definition LIKE '%snoRNA%'
USE NESTING (set(cvterm(cv)(dbxref(db))))
]);
print $termset->xml;
my @terms = $termset->get_cvterm;
ok(@terms,1);
my $term = shift @terms;
ok($term->sget_cv->sget_name eq 'biological_process');
ok($term->sget_dbxref->sget_db->sget_name eq 'GO');

$dbh->disconnect;
