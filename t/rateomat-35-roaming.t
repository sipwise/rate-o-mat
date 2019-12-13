
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet calls of callers with profile + billing
### network billing mappings
###
### this tests verify that rates are correctly chosen
### depending on the caller (source) ip.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

my $provider = Utils::Api::setup_provider('test.com',
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
		{ #network1
			onpeak_init_rate        => 4,
			onpeak_init_interval    => 60,
			onpeak_follow_rate      => 2,
			onpeak_follow_interval  => 30,
			offpeak_init_rate        => 4,
			offpeak_init_interval    => 60,
			offpeak_follow_rate      => 2,
			offpeak_follow_interval  => 30,
		},
		{ #network2
			onpeak_init_rate        => 6,
			onpeak_init_interval    => 60,
			onpeak_follow_rate      => 3,
			onpeak_follow_interval  => 30,
			offpeak_init_rate        => 6,
			offpeak_init_interval    => 60,
			offpeak_follow_rate      => 3,
			offpeak_follow_interval  => 30,
		},
	],
	[ #billing networks:
		[ #network1
			{ip=>'10.0.0.1',mask=>26}, #0..63
			{ip=>'10.0.0.133',mask=>26}, #128..
		],
		[ #network2
			{ip=>'10.0.0.99',mask=>26}, #64..127
		],
	]
);

my $balance = 5;
my $profiles_setup = [
	$provider->{subscriber_fees}->[0]->{profile},
	[ $provider->{subscriber_fees}->[1]->{profile}, $provider->{networks}->[0] ],
	[ $provider->{subscriber_fees}->[2]->{profile}, $provider->{networks}->[1] ],
];
my $caller1 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
my $caller2 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
my $caller3 = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
my $callee = Utils::Api::setup_subscriber($provider,$profiles_setup,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
	#call from any
	Utils::Rateomat::prepare_cdr($caller1->{subscriber},undef,$caller1->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',Utils::Api::current_unix(),1),
	#call from network1
	Utils::Rateomat::prepare_cdr($caller2->{subscriber},undef,$caller2->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'10.0.0.129',Utils::Api::current_unix(),1),
	#call from network2
	Utils::Rateomat::prepare_cdr($caller3->{subscriber},undef,$caller3->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'10.0.0.97',Utils::Api::current_unix(),1),
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
		$cdr_ids[2] => {
			id => $cdr_ids[2],
			rating_status => 'ok',
		},
	),'cdrs were all processed');
	Utils::Api::check_interval_history('',$caller1->{customer}->{id},[
		{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_rate} *
				   $provider->{subscriber_fees}->[0]->{fee}->{onpeak_init_interval})/100.0 },
	]);
	Utils::Api::check_interval_history('',$caller2->{customer}->{id},[
		{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_rate} *
				   $provider->{subscriber_fees}->[1]->{fee}->{onpeak_init_interval})/100.0 },
	]);
	Utils::Api::check_interval_history('',$caller3->{customer}->{id},[
		{ cash => (100.0 * $balance - $provider->{subscriber_fees}->[2]->{fee}->{onpeak_init_rate} *
				   $provider->{subscriber_fees}->[2]->{fee}->{onpeak_init_interval})/100.0 },
	]);
}

done_testing();
exit;
