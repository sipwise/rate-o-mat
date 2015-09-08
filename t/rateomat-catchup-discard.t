
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;
use Storable qw();

Utils::Api::set_time(Utils::Api::get_now->subtract(months => 5));
#provider contract needs to be created in the past as well:
my $provider = create_provider();
my $callee = Utils::Api::setup_subscriber($provider,$provider->{profiles}->[0]->{profile},undef,{ cc => 888, ac => '2<n>', sn => '<t>' });
Utils::Api::set_time();

my $amount = 5;
my $costs = ($provider->{profiles}->[0]->{fee}->{onpeak_init_rate} *
	$provider->{profiles}->[0]->{fee}->{onpeak_init_interval})/100.0;
my $interval_days = 30;
my $timely_days = $interval_days / 3;

foreach my $start_mode ('create','1st') {
	foreach my $carry_over_mode ('carry_over','discard','carry_over_timely') {
		my $begin = Utils::Api::get_now->subtract(days => (3 * $interval_days + 1));
		my $profiles_setup = Utils::Api::setup_package($provider,
			[ #initial:
				$provider->{profiles}->[0]->{profile}
			],
			[ #topup:

			],
			[ #underrun:

			],
			balance_interval_start_mode => $start_mode,
			balance_interval_value => $interval_days,
			balance_interval_unit => 'day',
			carry_over_mode => $carry_over_mode,
			timely_duration_unit => 'day',
			timely_duration_value => $timely_days,
			notopup_discard_intervals => 2 * $interval_days,
		)->{package};

		Utils::Api::set_time($begin);

		my $caller_notopup = Utils::Api::setup_subscriber($provider,$profiles_setup,$amount,{ cc => 888, ac => '1<n>', sn => '<t>' });
		my $caller_topup = Utils::Api::setup_subscriber($provider,$profiles_setup,$amount,{ cc => 888, ac => '1<n>', sn => '<t>' });
		my $caller_timelytopup = Utils::Api::setup_subscriber($provider,$profiles_setup,$amount,{ cc => 888, ac => '1<n>', sn => '<t>' });

		$begin->truncate(to => 'month') if '1st' eq $start_mode;
		my $within_first_interval = $begin->clone->add(days => ($interval_days / 2));
		Utils::Api::set_time($within_first_interval);
		Utils::Api::perform_topup($caller_topup->{subscriber},$amount);

		my $within_first_topup = $begin->clone->add(days => ($interval_days - $timely_days / 2));
		Utils::Api::set_time($within_first_topup);
		Utils::Api::perform_topup($caller_timelytopup->{subscriber},$amount);

		Utils::Api::set_time();

		my $now = Utils::Api::get_now();

		my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
			Utils::Rateomat::prepare_cdr($caller_notopup->{subscriber},undef,$caller_notopup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$within_first_interval->epoch,1),
			Utils::Rateomat::prepare_cdr($caller_notopup->{subscriber},undef,$caller_notopup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$now->epoch,1),

			Utils::Rateomat::prepare_cdr($caller_topup->{subscriber},undef,$caller_topup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$within_first_interval->epoch,1),
			Utils::Rateomat::prepare_cdr($caller_topup->{subscriber},undef,$caller_topup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$now->epoch,1),

			Utils::Rateomat::prepare_cdr($caller_timelytopup->{subscriber},undef,$caller_timelytopup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$within_first_interval->epoch,1),
			Utils::Rateomat::prepare_cdr($caller_timelytopup->{subscriber},undef,$caller_timelytopup->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$now->epoch,1),
		]) };

		if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat(),'rate-o-mat executed')) {
			ok(Utils::Rateomat::check_cdrs('',
				map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
			 ),'cdrs were all processed');
			my $label = $start_mode . '/' . $carry_over_mode;
			$begin->truncate(to => 'day');
			my @intervals = map { {
				start => Utils::Api::datetime_to_string($begin),
				stop => Utils::Api::datetime_to_string($begin->add(days => $interval_days)->clone->subtract(seconds => 1)),
				}; } 1..4;
			if ('carry_over' eq $carry_over_mode) {
				Utils::Api::check_interval_history($label . ' no topup: ',$caller_notopup->{customer}->{id},[
					set_cash($intervals[0],$amount - $costs),
					set_cash($intervals[1],$amount - $costs),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' topup: ',$caller_topup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],2*$amount - $costs),
					set_cash($intervals[2],2*$amount - $costs),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' timely topup: ',$caller_timelytopup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],2*$amount - $costs),
					set_cash($intervals[2],2*$amount - $costs),
					set_cash($intervals[3],0),
				]);
			} elsif ('discard' eq $carry_over_mode) {
				Utils::Api::check_interval_history($label . ' no topup: ',$caller_notopup->{customer}->{id},[
					set_cash($intervals[0],$amount - $costs),
					set_cash($intervals[1],0),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' topup: ',$caller_topup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],0),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' timely topup: ',$caller_timelytopup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],0),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
			} elsif ('carry_over_timely' eq $carry_over_mode) {
				Utils::Api::check_interval_history($label . ' no topup: ',$caller_notopup->{customer}->{id},[
					set_cash($intervals[0],$amount - $costs),
					set_cash($intervals[1],0),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' topup: ',$caller_topup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],0),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
				Utils::Api::check_interval_history($label . ' timely topup: ',$caller_timelytopup->{customer}->{id},[
					set_cash($intervals[0],2*$amount - $costs),
					set_cash($intervals[1],2*$amount - $costs),
					set_cash($intervals[2],0),
					set_cash($intervals[3],0),
				]);
			}
		}
	}
}

foreach my $start_mode ('topup','topup_interval') {
	my $carry_over_mode = 'carry_over';
	my $profiles_setup = Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{profiles}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:

		],
		balance_interval_start_mode => $start_mode,
		balance_interval_value => $interval_days,
		balance_interval_unit => 'day',
		carry_over_mode => $carry_over_mode,
		timely_duration_unit => 'day',
		timely_duration_value => $timely_days,
		notopup_discard_intervals => 2 * $interval_days,
	)->{package};

	my $begin = Utils::Api::get_now->subtract(days => (3 * $interval_days + 1));
	Utils::Api::set_time($begin);

	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,$amount,{ cc => 888, ac => '1<n>', sn => '<t>' });

	Utils::Api::set_time();
	my @cash_values = ( $amount - $costs, $amount - 2*$costs, 0 );
	my $i = 0;
	foreach my $call_time ($begin->clone->add(days => ($interval_days / 2)),
						   $begin->clone->add(days => (1.5*$interval_days)),
						   $begin->clone->add(days => (2.5*$interval_days))) {

		my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
			Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
					$callee->{subscriber},undef,$callee->{reseller},
					'192.168.0.1',$call_time->epoch,1),
		]) };

		if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat(),'rate-o-mat executed')) {
			ok(Utils::Rateomat::check_cdrs('',
				map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
			 ),'cdrs were all processed');
			my $label = $start_mode . '/' . $carry_over_mode;
			Utils::Api::check_interval_history($label . ' no topup: ',$caller->{customer}->{id},[
				{ start => '~'.Utils::Api::datetime_to_string($begin),
				  stop => Utils::Api::datetime_to_string(Utils::Api::infinite_future()),
				  cash => $cash_values[$i] }
			]);
		}
		$i++;
	}
}

done_testing();
exit;

sub create_provider {
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{ #any
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

sub set_cash {
	my ($interval,$cash) = @_;
	$interval = Storable::dclone($interval);
	$interval->{cash} = $cash;
	return $interval;
}