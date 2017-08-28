
use strict;
use warnings;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### first onnet calls of a caller with hourly-based balance intervals starting
### after DST (daylight saving time) barriers
###
### this short tests verify that created contract_balance records show a
### correct gap in their hourly balance intervals.

if ('Europe/Vienna' eq Utils::Api::get_now->time_zone->name) {
    Utils::Api::set_time(Utils::Api::datetime_from_string('2015-03-01 00:00:00'));

    my $provider = create_provider();
    my $amount = 5;
    my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},undef,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $costs_initial = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0;

	my $profiles_setup = Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{subscriber_fees}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:

		],
		balance_interval_start_mode => 'create',
		balance_interval_value => 1,
		balance_interval_unit => 'hour',
		carry_over_mode => 'carry_over',
		initial_balance => $amount*100,
	)->{package};

    {
        my $dt = Utils::Api::datetime_from_string('2015-03-29 01:26:00');
        ok(!$dt->is_dst(),Utils::Api::datetime_to_string($dt)." is not in daylight saving time (winter)");
        Utils::Api::set_time($dt);
        my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });

        $dt = Utils::Api::datetime_from_string('2015-03-29 03:26:00');
        ok($dt->is_dst(),Utils::Api::datetime_to_string($dt)." is in daylight saving time (summer)");
        Utils::Api::set_time($dt);

        my $now = Utils::Api::get_now();

        my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
            Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
                $callee->{subscriber},undef,$callee->{reseller},
                '192.168.0.1',$now->epoch,1),
        ]) };

        if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
            ok(Utils::Rateomat::check_cdrs('',
                map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
            ),'cdrs were all processed');
            my $label = 'rateomat catchup over winter>summer time transition: ';
            Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
                { start => '2015-03-29 00:00:00', stop => '2015-03-29 00:59:59', cash => $amount },
                { start => '2015-03-29 01:00:00', stop => '2015-03-29 01:59:59', cash => $amount },
                #{ start => '2015-03-29 02:00:00', stop => '2015-03-29 02:59:59', cash => $amount },
                { start => '2015-03-29 03:00:00', stop => '2015-03-29 03:59:59', cash => $amount - $costs_initial },
            ]);
        }
    }

    {
        my $dt = Utils::Api::datetime_from_string('2015-10-25 01:26:00');
        ok($dt->is_dst(),Utils::Api::datetime_to_string($dt)." is in daylight saving time (summer)");
        Utils::Api::set_time($dt);
        my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });

        $dt = Utils::Api::datetime_from_string('2015-10-25 03:26:00');
        ok(!$dt->is_dst(),Utils::Api::datetime_to_string($dt)." is not in daylight saving time (winter)");
        Utils::Api::set_time($dt);

        my $now = Utils::Api::get_now();

        my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
            Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
                $callee->{subscriber},undef,$callee->{reseller},
                '192.168.0.1',$now->epoch,1),
        ]) };

        if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
            ok(Utils::Rateomat::check_cdrs('',
                map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
            ),'cdrs were all processed');
            my $label = 'rateomat catchup over winter>summer time transition: ';
            Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
                { start => '2015-10-25 00:00:00', stop => '2015-10-25 00:59:59', cash => $amount },
                { start => '2015-10-25 01:00:00', stop => '2015-10-25 01:59:59', cash => $amount },
                { start => '2015-10-25 02:00:00', stop => '2015-10-25 02:59:59', cash => $amount },
                { start => '2015-10-25 03:00:00', stop => '2015-10-25 03:59:59', cash => $amount - $costs_initial },
            ]);
        }
    }

    Utils::Api::set_time();

} else {
    diag("time zone '" . Utils::Api::get_now->time_zone->name . "', skipping DST test");
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
