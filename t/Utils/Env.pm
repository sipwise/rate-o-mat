package Utils::Env;

use strict;
use warnings;

use Test::More;
use POSIX qw(tzset strftime);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    set_local_timezone
);

## no critic (Variables::RequireLocalizedPunctuationVars)
$ENV{RATEOMAT_BILLING_DB_HOST}      //= '127.0.0.1';
$ENV{RATEOMAT_PROVISIONING_DB_HOST} //= '127.0.0.1';
$ENV{RATEOMAT_ACCOUNTING_DB_HOST}   //= '127.0.0.1';
$ENV{CATALYST_SERVER}               //= 'https://127.0.0.1:1443';

sub set_local_timezone {
	(my $tz,$ENV{RATEOMAT_CONNECTION_TIMEZONE}) = @_;
	if ($tz) {
		my $old_tz = strftime("%Z", localtime());
		$ENV{TZ} = $tz;
		tzset;
		diag("switching local timezone from $old_tz to " . strftime("%Z", localtime()));
	}
}

1;
