
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use DateTime qw();
use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### test calls starting/ending at 23:59:59.xxx

my $provider = create_provider();
{

    my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},undef,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $caller = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},undef,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $costs_initial = $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
    $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval};

    my $now = Utils::Api::get_now();
    my $start = $now->clone->add(months => 1)->truncate(to => 'month')->subtract(seconds => 2)->epoch + 0.567;
    my $duration = 1.0;

    my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
        Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
            $callee->{subscriber},undef,$callee->{reseller},
            '192.168.0.1',$start,$duration),
    ]) };

    if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
        ok(Utils::Rateomat::check_cdrs('',
            map { $_ => {
                id => $_,
                rating_status => 'ok',
                source_customer_cost => Utils::Rateomat::decimal_to_string($costs_initial) };
            } @cdr_ids
        ),'cdrs were all processed');
        my $label = 'cdr with end time at '.Utils::Api::datetime_to_string(DateTime->from_epoch(epoch => ($start + $duration))).': ';
        Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
            { start => Utils::Api::datetime_to_string($now->clone->truncate(to => 'month')),
              stop => Utils::Api::datetime_to_string($now->clone->add(months => 1)->truncate(to => 'month')->subtract(seconds => 1)),
            },
        ]);
    }
}

{

    my $amount = 5;
    my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},undef,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $caller = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},$amount,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $costs_initial = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
    $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval}) / 100.0;

    my $now = Utils::Api::get_now();
    my $start = $now->clone->add(months => 1)->truncate(to => 'month')->subtract(seconds => 1)->epoch + 0.567;

    my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
        Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
            $callee->{subscriber},undef,$callee->{reseller},
            '192.168.0.1',$start,1),
    ]) };

    if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
        ok(Utils::Rateomat::check_cdrs('',
            map { $_ => {
                id => $_,
                rating_status => 'ok', };
            } @cdr_ids
        ),'cdrs were all processed');
        my $label = 'cdr with start time at '.Utils::Api::datetime_to_string(DateTime->from_epoch(epoch => $start)).': ';
        Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
            { start => Utils::Api::datetime_to_string($now->clone->truncate(to => 'month')),
              stop => Utils::Api::datetime_to_string($now->clone->add(months => 1)->truncate(to => 'month')->subtract(seconds => 1)),
              cash => $amount - $costs_initial,
            },
            { start => Utils::Api::datetime_to_string($now->clone->add(months => 1)->truncate(to => 'month')),
              stop => Utils::Api::datetime_to_string($now->clone->add(months => 2)->truncate(to => 'month')->subtract(seconds => 1)),
              cash => $amount - $costs_initial,
            },
        ]);
    }

}

done_testing();
exit;

sub create_provider {
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{ #initial:
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
		[ #billing networks:
		]
	);
}
