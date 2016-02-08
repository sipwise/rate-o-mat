use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

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

my $balance = 5;
my $profiles_setup = $provider->{subscriber_fees}->[0]->{profile};
my $caller = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
#my $caller2 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
#my $caller3 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

my @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([
	#call from any
	Utils::Rateomat::prepare_prepaid_costs_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',Utils::Api::current_unix(),1,0,0),

]) };

if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
	#ok(Utils::Rateomat::check_cdrs('',
	#	$cdr_ids[0] => {
	#		id => $cdr_ids[0],
	#		rating_status => 'ok',
	#	},
	#	$cdr_ids[1] => {
	#		id => $cdr_ids[1],
	#		rating_status => 'ok',
	#	},
	#	$cdr_ids[2] => {
	#		id => $cdr_ids[2],
	#		rating_status => 'ok',
	#	},
	#),'cdrs were all processed');
	#Utils::Api::check_interval_history('',$caller1->{customer}->{id},[
	#	{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
	#			   $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0 },
	#]);
	#Utils::Api::check_interval_history('',$caller2->{customer}->{id},[
	#	{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_rate} *
	#			   $provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_interval})/100.0 },
	#]);
	#Utils::Api::check_interval_history('',$caller3->{customer}->{id},[
	#	{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[2]->{fee}->{onpeak_init_rate} *
	#			   $provider->{subscriber_fees}->[2]->{fee}->{onpeak_init_interval})/100.0 },
	#]);
}

done_testing();
exit;
