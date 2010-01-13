BEGIN {
    use FindBin;
    use lib $FindBin::Bin . "/../lib";
}
use DBD::SQLite;
use DBIx::Simple::Batch;

my $fs = "$FindBin::Bin/../sql/tables/users/*.*";
my @p  = ('dbi:SQLite:dbname=:memory:', '', '', { RaiseError => 1 });
my $db = DBIx::Simple::Batch->new($fs, @p);

$db->call->getall('am i working?');
print @{$db->rs('group')};