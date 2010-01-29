use Test::More;

BEGIN {
    eval { require FindBin; 1 }
        or plan skip_all => 'FindBin required';
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan tests => 4;
    use_ok('FindBin');
    use_ok('DBIx::Simple');
    use_ok('DBIx::Simple::Batch');
}

# Test body borrowed from dbix-simple

my $fs = "$FindBin::Bin/../sql/";
my @p  = ('dbi:SQLite:dbname=:memory:', '', '', { RaiseError => 1 });
my $db = DBIx::Simple::Batch->new($fs, @p);

# connected
ok($db, 'connection test');