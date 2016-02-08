
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

Utils::Api::set_time(Utils::Api::get_now->subtract(months => 5));
#provider contract needs to be created in the past as well:
my $provider = create_provider();
my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},undef,{ cc => 888, ac => '2<n>', sn => '<t>' });
Utils::Api::set_time();

my $amount = 5;
my $costs_initial = ($provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0;
my $costs_underrun = ($provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_interval})/100.0;
my $lock_level = 4;

{
	my $label = 'underrun because of discard during catchup: ';
	my $begin = Utils::Api::get_now->subtract(days => (30 + 1));

	my $profiles_setup = Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{subscriber_fees}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:
			$provider->{subscriber_fees}->[2]->{profile}
		],
		balance_interval_start_mode => 'create',
		balance_interval_value => 30,
		balance_interval_unit => 'day',
		carry_over_mode => 'discard',
		initial_balance => $amount*100,
		underrun_profile_threshold => 1,
		underrun_lock_threshold => 1,
		underrun_lock_level => $lock_level,
	)->{package};

	Utils::Api::set_time($begin);
	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });
	is(Utils::Api::get_subscriber_preferences($caller->{subscriber})->{lock},undef,$label.'subscriber is not locked initially');
	is(Utils::Rateomat::get_usr_preferences($caller->{subscriber},'prepaid')->[0],undef,$label.'subscriber is not prepaid initially');

	Utils::Api::set_time();
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
		Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
			{ start => Utils::Api::datetime_to_string($begin->truncate(to => 'day')),
			  stop => Utils::Api::datetime_to_string($begin->add(days => 30)->clone->subtract(seconds => 1)),
			  cash => $amount,
			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
			{ start => Utils::Api::datetime_to_string($begin),
			  stop => Utils::Api::datetime_to_string($begin->add(days => 30)->clone->subtract(seconds => 1)),
			  cash => 0,
			  profile => $provider->{subscriber_fees}->[2]->{profile}->{id}},
		]);
		is(Utils::Api::get_subscriber_preferences($caller->{subscriber})->{lock},$lock_level,$label.'subscriber is locked now');
		is(Utils::Rateomat::get_usr_preferences($caller->{subscriber},'prepaid')->[0]->{value},1,$label.'subscriber is prepaid now');
	}
}

{
	my $label = 'underrun because of rating: ';
	my $begin = Utils::Api::get_now->subtract(days => (30 - 1));

	my $profiles_setup = Utils::Api::setup_package($provider,
		[ #initial:
			$provider->{subscriber_fees}->[0]->{profile}
		],
		[ #topup:

		],
		[ #underrun:
			$provider->{subscriber_fees}->[1]->{profile}
		],
		balance_interval_start_mode => 'topup',
		balance_interval_value => 30,
		balance_interval_unit => 'day',
		carry_over_mode => 'carry_over',
		initial_balance => $amount*100,
		underrun_profile_threshold => ($amount - $costs_initial)*100+1,
		underrun_lock_threshold => ($amount - $costs_initial - $costs_underrun)*100+1,
		underrun_lock_level => $lock_level,
	)->{package};

	Utils::Api::set_time($begin);
	my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,undef,{ cc => 888, ac => '1<n>', sn => '<t>' });
	is(Utils::Api::get_subscriber_preferences($caller->{subscriber})->{lock},undef,$label.'subscriber is not locked initially');

	Utils::Api::set_time();
	my $now = Utils::Api::get_now();

	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$now->clone->subtract(seconds => 1)->epoch,1),
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$now->epoch,1),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
		),'cdrs were all processed');
		Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
			{ start => '~'.Utils::Api::datetime_to_string($begin),
			  stop => Utils::Api::datetime_to_string(Utils::Api::infinite_future()),
			  cash => $amount - $costs_initial - $costs_underrun,
			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
		]);
		is(Utils::Api::get_subscriber_preferences($caller->{subscriber})->{lock},$lock_level,$label.'subscriber is locked now');
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
			{ #underrun:
				onpeak_init_rate        => 3,
				onpeak_init_interval    => 60,
				onpeak_follow_rate      => 2,
				onpeak_follow_interval  => 30,
				offpeak_init_rate        => 3,
				offpeak_init_interval    => 60,
				offpeak_follow_rate      => 2,
				offpeak_follow_interval  => 30,
			},
			{ #underrun prepaid:
				prepaid                 => 1,
				onpeak_init_rate        => 3,
				onpeak_init_interval    => 60,
				onpeak_follow_rate      => 2,
				onpeak_follow_interval  => 30,
				offpeak_init_rate        => 3,
				offpeak_init_interval    => 60,
				offpeak_follow_rate      => 2,
				offpeak_follow_interval  => 30,
			},
		],
		[ #billing networks:
		]
	);
}
