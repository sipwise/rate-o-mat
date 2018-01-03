package Utils::Env;

use strict;
use warnings;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    $DATETIME_TIMEZONE
);

## no critic (Variables::RequireLocalizedPunctuationVars)
$ENV{RATEOMAT_BILLING_DB_HOST} = '192.168.0.84';
$ENV{RATEOMAT_PROVISIONING_DB_HOST} = '192.168.0.84';
$ENV{RATEOMAT_ACCOUNTING_DB_HOST} = '192.168.0.84';
$ENV{RATEOMAT_CONNECTION_TIMEZONE} = '+0:00'; #'UTC'; #vagrant SYSETM timezone is "Etc/UTC"

$ENV{CATALYST_SERVER} = 'https://127.0.0.1:1443';

our $DATETIME_TIMEZONE = undef; #'UTC';

1;