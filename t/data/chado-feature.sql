
-- ================================================
-- TABLE: feature
-- ================================================

create table feature (
    feature_id serial not null,
    primary key (feature_id),
    dbxref_id int,
    foreign key (dbxref_id) references dbxref (dbxref_id) on delete set null INITIALLY DEFERRED,
    organism_id int not null,
    foreign key (organism_id) references organism (organism_id) on delete cascade INITIALLY DEFERRED,
    name varchar(255),
    uniquename text not null,
    residues text,
    seqlen int,
    md5checksum char(32),
    type_id int not null,
    foreign key (type_id) references cvterm (cvterm_id) on delete cascade INITIALLY DEFERRED,
    is_analysis boolean not null default 'false',
    is_obsolete boolean not null default 'false',
    timeaccessioned timestamp not null default current_timestamp,
    timelastmodified timestamp not null default current_timestamp,
    constraint feature_c1 unique (organism_id,uniquename,type_id)
);
-- dbxref_id here is intended for the primary dbxref for this feature.   
-- Additional dbxref links are made via feature_dbxref
-- name: the human-readable common name for a feature, for display
-- uniquename: the unique name for a feature; may not be particularly human-readable

-- timeaccessioned and timelastmodified are for handling object accession/
-- modification timestamps (as opposed to db auditing info, handled elsewhere).
-- The expectation is that these fields would be available to software 
-- interacting with chado.
-- is_obsolete is for marking records obsolete which none the less need to 
-- remain in the table. 
create sequence feature_uniquename_seq;
create index feature_name_ind1 on feature(name);
create index feature_idx1 on feature (dbxref_id);
create index feature_idx2 on feature (organism_id);
create index feature_idx3 on feature (type_id);
create index feature_idx4 on feature (uniquename);
create index feature_idx5 on feature (lower(name));
--This ALTER TABLE statement changes the way sequence data
--is stored on disk to make extracting substrings much faster
--at the expense of more disk space
--ALTER TABLE feature ALTER COLUMN residues SET STORAGE EXTENDED;


-- ================================================
-- TABLE: featureloc
-- ================================================

-- each feature can have 0 or more locations.
-- multiple locations do NOT indicate non-contiguous locations.
-- instead they designate alternate locations or grouped locations;
-- for instance, a feature designating a blast hit or hsp will have two
-- locations, one on the query feature, one on the subject feature.
-- features representing sequence variation could have alternate locations
-- instantiated on a feature on the mutant strain.
-- the field "rank" is used to differentiate these different locations.
-- the default rank '0' is used for the main/primary location (eg in
-- similarity features, 0 is query, 1 is subject), although sometimes
-- this will be symmeytical and there is no primary location.
--
-- redundant locations can also be stored - for instance, the position
-- of an exon on a BAC and in global coordinates. the field "locgroup"
-- is used to differentiate these groupings of locations. the default
-- locgroup '0' is used for the main/primary location, from which the
-- others can be derived via coordinate transformations. another
-- example of redundant locations is storing ORF coordinates relative
-- to both transcript and genome. redundant locations open the possibility
-- of the database getting into inconsistent states; this schema gives
-- us the flexibility of both 'warehouse' instantiations with redundant
-- locations (easier for querying) and 'management' instantiations with
-- no redundant locations.

-- most features (exons, transcripts, etc) will have 1 location, with
-- locgroup and rank equal to 0
--
-- an example of using both locgroup and rank:
-- imagine a feature indicating a conserved region between the chromosomes
-- of two different species. we may want to keep redundant locations on
-- both contigs and chromosomes. we would thus have 4 locations for the
-- single conserved region feature - two distinct locgroups (contig level
-- and chromosome level) and two distinct ranks (for the two species).

-- altresidues is used to store alternate residues of a feature, when these
-- differ from feature.residues. for instance, a SNP feature located on
-- a wild and mutant protein would have different alresidues.
-- for alignment/similarity features, the altresidues is used to represent
-- the alignment string.

-- note on variation features; even if we don't want to instantiate a mutant
-- chromosome/contig feature, we can still represent a SNP etc with 2 locations,
-- one (rank 0) on the genome, the other (rank 1) would have most fields null,
-- except for altresidues

-- IMPORTANT: fnbeg and fnend are space-based (INTERBASE) coordinates
-- this is vital as it allows us to represent zero-length
-- features eg splice sites, insertion points without
-- an awkward fuzzy system

-- Note that nbeg and nend have been replaced with fmin and fmax,
-- which are the minimum and maximum coordinates of the feature
-- relative to the parent feature.  By contrast,
-- nbeg, nend are for feature natural begin/end
-- by natural begin, end we mean these are the actual
-- beginning (5' position) and actual end (3' position)
-- rather than the low position and high position, as
-- these terms are sometimes erroneously used.  To compensate
-- for the removal of nbeg and nend from featureloc, a view
-- based on featureloc, dfeatureloc, is provided in sequence_views.sql.

create table featureloc (
    featureloc_id serial not null,
    primary key (featureloc_id),
    feature_id int not null,
    foreign key (feature_id) references feature (feature_id) on delete cascade INITIALLY DEFERRED,
    srcfeature_id int,
    foreign key (srcfeature_id) references feature (feature_id) on delete set null INITIALLY DEFERRED,
    fmin int,
    is_fmin_partial boolean not null default 'false',
    fmax int,
    is_fmax_partial boolean not null default 'false',
    strand smallint,
    phase int,
    residue_info text,
    locgroup int not null default 0,
    rank int not null default 0,
    constraint featureloc_c1 unique (feature_id,locgroup,rank),
    constraint featureloc_c2 check (fmin <= fmax)
);
-- phase: phase of translation wrt srcfeature_id.  Values are 0,1,2
create index featureloc_idx1 on featureloc (feature_id);
create index featureloc_idx2 on featureloc (srcfeature_id);
create index featureloc_idx3 on featureloc (srcfeature_id,fmin,fmax);

-- ================================================
-- TABLE: feature_pub
-- ================================================

create table feature_pub (
    feature_pub_id serial not null,
    primary key (feature_pub_id),
    feature_id int not null,
    foreign key (feature_id) references feature (feature_id) on delete cascade INITIALLY DEFERRED,
    pub_id int not null,
    foreign key (pub_id) references pub (pub_id) on delete cascade INITIALLY DEFERRED,
    constraint feature_pub_c1 unique (feature_id,pub_id)
);
create index feature_pub_idx1 on feature_pub (feature_id);
create index feature_pub_idx2 on feature_pub (pub_id);

-- ================================================
-- TABLE: featureprop
-- ================================================

create table featureprop (
    featureprop_id serial not null,
    primary key (featureprop_id),
    feature_id int not null,
    foreign key (feature_id) references feature (feature_id) on delete cascade INITIALLY DEFERRED,
    type_id int not null,
    foreign key (type_id) references cvterm (cvterm_id) on delete cascade INITIALLY DEFERRED,
    value text null,
    rank int not null default 0,
    constraint featureprop_c1 unique (feature_id,type_id,rank)
);
create index featureprop_idx1 on featureprop (feature_id);
create index featureprop_idx2 on featureprop (type_id);

-- ================================================
-- TABLE: featureprop_pub
-- ================================================

create table featureprop_pub (
    featureprop_pub_id serial not null,
    primary key (featureprop_pub_id),
    featureprop_id int not null,
    foreign key (featureprop_id) references featureprop (featureprop_id) on delete cascade INITIALLY DEFERRED,
    pub_id int not null,
    foreign key (pub_id) references pub (pub_id) on delete cascade INITIALLY DEFERRED,
    constraint featureprop_pub_c1 unique (featureprop_id,pub_id)
);
create index featureprop_pub_idx1 on featureprop_pub (featureprop_id);
create index featureprop_pub_idx2 on featureprop_pub (pub_id);

-- ================================================
-- TABLE: feature_dbxref
-- ================================================
-- links a feature to dbxrefs.  Note that there is also feature.dbxref_id
-- link for the primary dbxref link.

create table feature_dbxref (
    feature_dbxref_id serial not null,
    primary key (feature_dbxref_id),
    feature_id int not null,
    foreign key (feature_id) references feature (feature_id) on delete cascade INITIALLY DEFERRED,
    dbxref_id int not null,
    foreign key (dbxref_id) references dbxref (dbxref_id) on delete cascade INITIALLY DEFERRED,
    is_current boolean not null default 'true',
    constraint feature_dbxref_c1 unique (feature_id,dbxref_id)
);
create index feature_dbxref_idx1 on feature_dbxref (feature_id);
create index feature_dbxref_idx2 on feature_dbxref (dbxref_id);


