use lib 't';

BEGIN {
    # to handle systems with no installed Test module
    # we include the t dir (where a copy of Test.pm is located)
    # as a fallback
    eval { require Test; };
    use Test;    
    use DBStagTest;
    plan tests => 1;
}
use DBIx::DBStag;
use FileHandle;

my $dbh = dbh(@ARGV);

my $xmlstruct =
  $dbh->selectall_stag(q[
		 
                         SELECT bioentry.*, ftype.term_name, seqfeature.*, seqfeature_location.*, seqfeature_qualifier_value.qualifier_value, qualifier_term.term_name FROM bioentry NATURAL JOIN ((seqfeature NATURAL JOIN (seqfeature_qualifier_value NATURAL JOIN ontology_term AS qualifier_term)) INNER JOIN ontology_term AS ftype ON ftype.ontology_term_id = seqfeature_key_id)NATURAL JOIN seqfeature_location WHERE ftype.term_name != 'source' LIMIT 100

                      
		      ]
);

print $xmlstruct->xml;

$dbh->disconnect;
