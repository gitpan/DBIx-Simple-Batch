#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'DBIx::Simple::Batch' );
}

diag( "Testing DBIx::Simple::Batch $DBIx::Simple::Batch::VERSION, Perl $], $^X" );
