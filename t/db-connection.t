use Test::More;

BEGIN {
    eval { require FindBin; 1 }
        or plan skip_all => 'FindBin required';
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan tests => 7;
    use_ok('FindBin');
    use_ok('DBIx::Simple');
    use_ok('DBIx::Simple::Batch');
}

# Test body borrowed from dbix-simple

my $fs = "$FindBin::Bin/../sql/";
my $db = DBIx::Simple::Batch->new(
    $fs,
    'dbi:SQLite:dbname=:memory:', '', '', { RaiseError => 1 } # dbi source specification
);

# connected
ok($db, 'connection test');

# read sql file queue
ok($db->queue('tables/users/getall'), 'test sql file access');

# processed queue
ok($db->process_queue('this is a test'), 'process queue');

# named resultset 
is(ref($db->rs('group')), "ARRAY", "named resultset test");
