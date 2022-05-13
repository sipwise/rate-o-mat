use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet prepaid calls with costs from prepaid costs
### table
###
### this tests verify that prepaid costs are properly
### cached and cleaned up.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

$Utils::Rateomat::rateomat_timeout = 30;

my $provider = Utils::Api::setup_provider('test.com',
	[ #rates:
		{
            prepaid                 => 1,
			onpeak_init_rate        => 2,
			onpeak_init_interval    => 60,
			onpeak_follow_rate      => 1,
			onpeak_follow_interval  => 30,
			offpeak_init_rate        => 2,
			offpeak_init_interval    => 60,
			offpeak_follow_rate      => 1,
			offpeak_follow_interval  => 30,
		},
	],
);

my $call_costs = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval} +
    $provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_rate} *
	$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_interval});

my $call_count = 3;
my $balance = $call_count * $call_costs / 100.0;
my $profiles_setup = $provider->{subscriber_fees}->[0]->{profile};
my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
#my $caller2 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
#my $caller3 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

foreach my $cache_size (($call_count - 1,$call_count)) {
    diag('rateomat prepaid costs cache size: '.$cache_size);
    local $ENV{RATEOMAT_PREPAID_COSTS_CACHE} = $cache_size;

    my @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
        Utils::Rateomat::prepare_prepaid_costs_cdr($caller->{subscriber},undef,$caller->{reseller},
                $callee->{subscriber},undef,$callee->{reseller},
                '192.168.0.1',Utils::Api::current_unix(),
                $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval} + 1,
                $call_costs,0);
    } (1..$call_count)]) };

    ok(Utils::Rateomat::check_prepaid_costs_cdrs('',1,
        map { $_ => {
                id => $_,
                rating_status => 'unrated',
            }; } @cdr_ids
    ),'cdrs and prepaid costs were all prepared');

    if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(1),'rate-o-mat executed')) {
        ok(Utils::Rateomat::check_prepaid_costs_cdrs('',0,
            map { $_ => {
                id => $_,
                rating_status => 'ok',
                source_customer_cost => Utils::Rateomat::decimal_to_string($call_costs),
            }; } @cdr_ids
        ),'cdrs were all processed');
        Utils::Api::check_interval_history('',$caller->{customer}->{id},[
            { cash => $balance }, # rateomat must not touch balance
        ]);
    }
}

done_testing();
exit;
