use strict;
use warnings;
use inc::Module::Install;

WriteMakefile(
    NAME                => 'DBIx::Simple::Batch',
    AUTHOR              => q{Al Newkirk <al.newkirk@awnstudio.com>},
    VERSION_FROM        => 'lib/DBIx/Simple/Batch.pm',
    ABSTRACT_FROM       => 'lib/DBIx/Simple/Batch.pm',
    LICENSE             => 'Perl',
    PL_FILES            => {},
    PREREQ_PM => {
        'DBIx::Simple'          => 0,
        'SQL::Abstract'         => 0,
        'SQL::Interp'           => 0,
        'File::Find::Object'    => 0.2.3,
        'Test::More'            => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-Simple-Batch-*' },
);
