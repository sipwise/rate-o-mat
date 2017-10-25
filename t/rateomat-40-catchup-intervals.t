
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;
use Data::Dumper;

### testcase outline:
### onnet calls of a caller with and without profile packages to check
### the correct creation of billing.contract_balance records ("catchup")
###
### the tests verify, that a correct history of balance interval records
### (contract_balance records) are created while rating a cdr. beside the
### default case of customer with no package, all combinations of interval
### start modes and interval duration units are covered.
### note: since it also includes minute-based balance intervals, this tests
### takes longer time to complete

$ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

#goto SKIP;
{ #no package:
	my $now = Utils::Api::get_now();
	my $begin = $now->clone->subtract(months => 3);

	Utils::Api::set_time($begin);

	#provider contract needs to be created in the past as well:
	my $provider = create_provider();

	my $balance = 5;
	my $profiles_setup = $provider->{subscriber_fees}->[0]->{profile};
	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
	my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

	Utils::Api::set_time();

	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
				$callee->{subscriber},undef,$callee->{reseller},
				'192.168.0.1',$begin->epoch,1),
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
				$callee->{subscriber},undef,$callee->{reseller},
				'192.168.0.1',$begin->clone->add(months => 1)->epoch,1),
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
				$callee->{subscriber},undef,$callee->{reseller},
				'192.168.0.1',$now->epoch,1),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			$cdr_ids[0] => {
				id => $cdr_ids[0],
				rating_status => 'ok',
			},
			$cdr_ids[1] => {
				id => $cdr_ids[1],
				rating_status => 'ok',
			},
		),'cdrs were all processed');
		my $costs = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
					   $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0;
		Utils::Api::check_interval_history('',$caller->{customer}->{id},[
			{ start => Utils::Api::datetime_to_string($begin->truncate(to => 'month')),
			  stop  => Utils::Api::datetime_to_string($begin->add(months => 1)->clone->subtract(seconds => 1)),
			  cash => $balance - $costs,
			},
			{ start => Utils::Api::datetime_to_string($begin),
			  stop  => Utils::Api::datetime_to_string($begin->add(months => 1)->clone->subtract(seconds => 1)),
			  cash => $balance - 2*$costs,
			},
			{ start => Utils::Api::datetime_to_string($begin),
			  stop  => Utils::Api::datetime_to_string($begin->add(months => 1)->clone->subtract(seconds => 1)),
			  cash => $balance - 2*$costs,
			},
			{ start => Utils::Api::datetime_to_string($begin),
			  stop  => Utils::Api::datetime_to_string($begin->add(months => 1)->clone->subtract(seconds => 1)),
			  cash => $balance - 3*$costs,
			},
		]);
	}
}

{
	my %stats = ();
	#start_mode/interval units matrix:
	foreach my $start_mode ('create','1st','topup','topup_interval') {
		foreach my $unit ('minutes','hours','days','weeks','months') {
			next if (('create' eq $start_mode or '1st' eq $start_mode) and ('minutes' eq $unit or 'hours' eq $unit));
			my $num_intervals = 3;
			my $begin = Utils::Api::get_now->subtract($unit => $num_intervals);

			Utils::Api::set_time($begin);

			#provider contract needs to be created in the past as well:
			my $provider = create_provider();

			Utils::Api::setup_package($provider,
				[ #initial:
					$provider->{subscriber_fees}->[0]->{profile}
				],
				[ #topup:

				],
				[ #underrun:

				],
				balance_interval_start_mode => $start_mode,
				balance_interval_value => 1,
				balance_interval_unit => substr($unit,0,length($unit) - 1),
				carry_over_mode => 'carry_over',
			);

			my $t1 = time;
			my $amount = 5;
			my $is_topup_start_mode = 'topup' eq $start_mode || 'topup_interval' eq $start_mode;
			my $balance = ($is_topup_start_mode ? undef : $amount);
			my $profiles_setup = $provider->{packages}->[0]->{package};
			my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
			my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

			my $t2 = time;

			Utils::Api::perform_topup($caller->{subscriber},$amount) if $is_topup_start_mode;

			my $first_call_ts = Utils::Api::get_now();
			$first_call_ts->add(seconds => 1) if $is_topup_start_mode; #ensure first call will be in next interval, otherwise there will be a debit on the first cash_balance_interval

			Utils::Api::set_time();

			my $last_call_ts = Utils::Api::get_now();

			my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
				Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$first_call_ts->epoch,1),
				Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
						$callee->{subscriber},undef,$callee->{reseller},
						'192.168.0.1',$last_call_ts->epoch,1),
			]) };

			if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
				ok(Utils::Rateomat::check_cdrs('',
					$cdr_ids[0] => {
						id => $cdr_ids[0],
						rating_status => 'ok',
					},
					$cdr_ids[1] => {
						id => $cdr_ids[1],
						rating_status => 'ok',
					},
				 ),'cdrs were all processed');
				my $costs = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
							   $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0;
				my $label = $start_mode . '/' . $unit . ': ';
				$stats{$label} = ($t2 - $t1) . ' secs';
				my $balance_intervals = Utils::Api::get_interval_history($label,$caller->{customer}->{id});
				if ('topup' eq $start_mode) {
					is(scalar @$balance_intervals,2,'number of balance intervals ' . (scalar @$balance_intervals) . ' = 2');
				} elsif ('topup_interval' eq $start_mode) {
					is(scalar @$balance_intervals,2 + $num_intervals,'number of balance intervals ' . (scalar @$balance_intervals) . ' = ' . (2 + $num_intervals));
				} elsif ('1st' eq $start_mode) {
					ok((scalar @$balance_intervals) >= 1 + $num_intervals,'number of balance intervals ' . (scalar @$balance_intervals) . ' >= ' . (2 + $num_intervals));
				} elsif ('create' eq $start_mode) {
					if ('hours' eq $unit) {
						ok((scalar @$balance_intervals) >= 1 + $num_intervals,'number of balance intervals ' . (scalar @$balance_intervals) . ' >= ' . (2 + $num_intervals));
					} else {
						is(scalar @$balance_intervals,1 + $num_intervals,'number of balance intervals ' . (scalar @$balance_intervals) . ' = ' . (1 + $num_intervals));
					}
				}
				for (my $i = 0; $i < (scalar @$balance_intervals); $i++) {
					my $cnt = $i + 1;
					my $interval = $balance_intervals->[$i];
					my $cash_balance = $interval->{cash_balance};
					if ($i == (scalar @$balance_intervals) - 1) {
						is($cash_balance,$amount - 2*$costs,$label . "last interval cash balance $cash_balance = " . ($amount - 2*$costs));
						if ('topup' eq $start_mode) {
							ok(Utils::Api::is_infinite_future(Utils::Api::datetime_from_string($interval->{stop})),$label . "last balance interval end is infinite future");
						} else {
							ok(Utils::Api::datetime_from_string($interval->{stop}) > $last_call_ts,$label . "last balance interval end is in future");
						}
					} else {
						if ($is_topup_start_mode) {
							if ($i == 0) {
								is($cash_balance,0,$label . "first interval cash balance $cash_balance = 0");
							} elsif ($i == 1) {
								is($cash_balance,$amount - $costs,$label . "second interval cash balance $cash_balance = " . ($amount - $costs));
							} else {
								if ($first_call_ts > Utils::Api::datetime_from_string($interval->{stop})) {
									is($cash_balance,$amount,$label . "$cnt. interval cash balance $cash_balance = " . $amount);
								} else {
									is($cash_balance,$amount - $costs,$label . "$cnt. interval cash balance $cash_balance = " . ($amount - $costs));
								}
							}
						} else {
							if ($begin > Utils::Api::datetime_from_string($interval->{stop})) {
								is($cash_balance,0,$label . "$cnt. interval cash balance $cash_balance = 0");
							} elsif ($first_call_ts > Utils::Api::datetime_from_string($interval->{stop})) {
								is($cash_balance,$amount,$label . "$cnt. interval cash balance $cash_balance = " . $amount);
							} else {
								is($cash_balance,$amount - $costs,$label . "$cnt. interval cash balance $cash_balance = " . ($amount - $costs));
							}
						}
					}
				}
			}
		}
	}
	diag(Dumper({ subscriber_creation_time => \%stats }));
}

{
	my $begin = Utils::Api::get_now->subtract(minutes => (3*60+30));

	Utils::Api::set_time($begin);

	my $rate_interval = 60;
	#provider contract needs to be created in the past as well:
	my $provider = create_provider($rate_interval);

	Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{subscriber_fees}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:

		],
		balance_interval_start_mode => 'topup_interval',
		balance_interval_value => 1,
		balance_interval_unit => 'hour',
		carry_over_mode => 'carry_over',
	);

	my $call_minutes = 3*60;
	my $amount = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval} +
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_rate} *
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_interval} * ($call_minutes - 1))/100.0;
	my $profiles_setup = $provider->{packages}->[0]->{package};
	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });
	my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '2<n>', sn => '<t>' });

	Utils::Api::perform_topup($caller->{subscriber},$amount);

	my $call_ts = Utils::Api::get_now()->add(minutes => 15);

	Utils::Api::set_time();

	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
				$callee->{subscriber},undef,$callee->{reseller},
				'192.168.0.1',$call_ts->epoch,$call_minutes*60),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			$cdr_ids[0] => {
				id => $cdr_ids[0],
				rating_status => 'ok',
			},
		 ),'cdrs were all processed');
		my $label = 'long call: ';
		my $balance_intervals = Utils::Api::get_interval_history($label,$caller->{customer}->{id});
		shift(@$balance_intervals);
		is(scalar @$balance_intervals,4,$label."number of balance intervals ".(scalar @$balance_intervals)." = 4");
		my @minutes_per_interval = (44,60,60,15);
		my $costs = $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
				$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval};
		for (my $i = 0; $i < (scalar @$balance_intervals); $i++) {
			my $interval = $balance_intervals->[$i];
			$costs += $provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_rate} *
				$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_interval} * $minutes_per_interval[$i];
			if ($i < ((scalar @$balance_intervals) - 1)) {
				ok(is_approx($interval->{cash_balance},($amount*100-$costs)/100,1.2),$label."interval cash balance $interval->{cash_balance} ~ ".($amount*100-$costs)/100);
			} else {
				is($interval->{cash_balance},0,$label."last interval cash balance is 0");
			}
		}
	}
}
#SKIP:
{
	my $begin = Utils::Api::get_now->subtract(minutes => 5);

	Utils::Api::set_time($begin);

	my $rate_interval = 60;
	#provider contract needs to be created in the past as well:
	my $provider = create_provider($rate_interval);

	Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{subscriber_fees}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:

		],
		balance_interval_start_mode => 'topup_interval',
		balance_interval_value => 1,
		balance_interval_unit => 'minute',
		carry_over_mode => 'carry_over',
	);

	my $call_minutes = 3;
	my $amount = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval} +
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_rate} *
		$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_interval} * ($call_minutes*60 - 1))/100.0;
	my $profiles_setup = $provider->{packages}->[0]->{package};
	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });
	my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '2<n>', sn => '<t>' });

	Utils::Api::perform_topup($caller->{subscriber},$amount);

	my $call_ts = Utils::Api::get_now()->add(seconds => 5);

	Utils::Api::set_time();

	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
				$callee->{subscriber},undef,$callee->{reseller},
				'192.168.0.1',$call_ts->epoch,$call_minutes*60),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			$cdr_ids[0] => {
				id => $cdr_ids[0],
				rating_status => 'ok',
			},
		 ),'cdrs were all processed');
		my $label = 'call spanning multiple intervals/cost propagation: ';
		my $balance_intervals = Utils::Api::get_interval_history($label,$caller->{customer}->{id});
		shift(@$balance_intervals);
		my $total_costs = 0;
		for (my $i = 0; $i < scalar @$balance_intervals; $i++) {
			my $interval = $balance_intervals->[$i];
			my $costs = ($i > 0 ? ($i < $call_minutes ? $provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_rate} *
				$provider->{subscriber_fees}->[0]->{fee}->{onpeak_follow_interval} : 0) :
				$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
				$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval});
			$total_costs += $costs;
			is($interval->{cash_balance},($amount*100-$total_costs)/100, $label."interval cash balance $interval->{cash_balance} is " . ($amount*100-$total_costs)/100);
		}
	}
}

done_testing();
exit;

sub is_approx {
	my ($val1,$val2,$epsilon) = @_;
	return abs($val1-$val2) <= $epsilon;
}

sub create_provider {
	my $rate_interval = shift;
	$rate_interval //= 60;
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{ #any
				onpeak_init_rate        => 2,
				onpeak_init_interval    => $rate_interval,
				onpeak_follow_rate      => 1,
				onpeak_follow_interval  => $rate_interval,
				offpeak_init_rate        => 2,
				offpeak_init_interval    => $rate_interval,
				offpeak_follow_rate      => 1,
				offpeak_follow_interval  => $rate_interval,
			},
		],
		[ #billing networks:
		]
	);
}
