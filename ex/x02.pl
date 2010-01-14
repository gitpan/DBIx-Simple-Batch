BEGIN {
    use FindBin;
    use lib $FindBin::Bin . "/../lib";
}
use DBD::SQLite;
use DBIx::Simple::Batch;

my $fs = "$FindBin::Bin/../sql/tables/users/*.*";
my $db = DBIx::Simple::Batch->new($fs);

$db->call->testall('am i working?');
print @{$db->rs('group')};