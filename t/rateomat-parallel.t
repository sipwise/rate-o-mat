
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

{

    Utils::Rateomat::run_rateomat_threads(3,6);

}

#{
#    my $provider = create_provider();
#    my $profile = $provider->{subscriber_fees}->[0]->{profile};
#    my $balance = 5;
#    my $caller = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
#    my $callee = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
#    my $caller_costs = ($provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_init_rate} *
#	$provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_init_interval} +
#    $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} *
#	$provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_interval})/100.0;
#    my $callee_costs = ($provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_init_rate} *
#	$provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_init_interval} +
#    $provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_follow_rate} *
#	$provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_follow_interval})/100.0; #negative!
#
#    my $now = Utils::Api::get_now();
#	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
#		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
#			$callee->{subscriber},undef,$callee->{reseller},
#			'192.168.0.1',$now->epoch,61),
#	]) };
#
#	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
#		ok(Utils::Rateomat::check_cdrs('',
#			map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
#		),'cdrs were all processed');
#		Utils::Api::check_interval_history('negative fees - caller',$caller->{customer}->{id},[
#			{ cash => $balance - $caller_costs,
#			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
#		]);
#		Utils::Api::check_interval_history('negative fees - callee',$callee->{customer}->{id},[
#			{ cash => $balance - $callee_costs,
#			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
#		]);
#	}
#
#}

done_testing();
exit;

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
