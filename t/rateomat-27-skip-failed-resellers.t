use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### missing outbound billing fees used to abort the whole rate-o-mat
### process after retries. with RATEOMAT_SKIP_FAILED_RESELLERS, the
### failed reseller is skipped and other resellers keep being rated.

my $init_secs = 60;
my $follow_secs = 30;

local $ENV{RATEOMAT_MAX_RETRIES} = 0;
local $ENV{RATEOMAT_RETRY_DELAY} = 0;
local $ENV{RATEOMAT_LOOP_INTERVAL} = 1;
$Utils::Rateomat::rateomat_timeout = 10;

my $offnet = Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld');

{
	my $broken = create_provider('^000.+');
	my $healthy = create_provider('^999.+');
	my $caller_a = Utils::Api::setup_subscriber($broken,$broken->{subscriber_fees}->[0]->{profile},0.0,{ cc => 888, ac => '1<n>', sn => '<t>' });
	my $caller_b = Utils::Api::setup_subscriber($healthy,$healthy->{subscriber_fees}->[0]->{profile},0.0,{ cc => 888, ac => '1<n>', sn => '<t>' });

	my $start_time = Utils::Api::current_unix() - 5;
	my $cdr_a = Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller_a->{subscriber},undef,$caller_a->{reseller},
			undef, $offnet, undef,
			'192.168.0.1',$start_time, $init_secs + $follow_secs),
	])->[0];
	my $cdr_b = Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller_b->{subscriber},undef,$caller_b->{reseller},
			undef, $offnet, undef,
			'192.168.0.1',$start_time + 1, $init_secs + $follow_secs),
	])->[0];

	ok($cdr_a && $cdr_b,'test CDRs created');

	{
		local $ENV{RATEOMAT_SKIP_FAILED_RESELLERS} = 1;
		if (ok($cdr_a && $cdr_b && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat kept running after missing fee when skip is enabled')) {
			ok(Utils::Rateomat::check_cdrs('skip enabled, broken reseller: ',
				$cdr_a->{id} => {
					id => $cdr_a->{id},
					rating_status => 'unrated',
				},
			),'broken reseller CDR stayed unrated');
			ok(Utils::Rateomat::check_cdrs('skip enabled, healthy reseller: ',
				$cdr_b->{id} => {
					id => $cdr_b->{id},
					rating_status => 'ok',
				},
			),'healthy reseller CDR was rated');
		}
	}
}

{
	my $broken = create_provider('^000.+');
	my $healthy = create_provider('^999.+');
	my $caller_a = Utils::Api::setup_subscriber($broken,$broken->{subscriber_fees}->[0]->{profile},0.0,{ cc => 888, ac => '1<n>', sn => '<t>' });
	my $caller_b = Utils::Api::setup_subscriber($healthy,$healthy->{subscriber_fees}->[0]->{profile},0.0,{ cc => 888, ac => '1<n>', sn => '<t>' });

	my $start_time = Utils::Api::current_unix() - 5;
	my $cdr_a = Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller_a->{subscriber},undef,$caller_a->{reseller},
			undef, $offnet, undef,
			'192.168.0.1',$start_time, $init_secs + $follow_secs),
	])->[0];
	my $cdr_b = Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller_b->{subscriber},undef,$caller_b->{reseller},
			undef, $offnet, undef,
			'192.168.0.1',$start_time + 1, $init_secs + $follow_secs),
	])->[0];

	ok($cdr_a && $cdr_b,'abort-mode test CDRs created');

	{
		local $ENV{RATEOMAT_SKIP_FAILED_RESELLERS} = 0;
		if (ok($cdr_a && $cdr_b && !Utils::Rateomat::run_rateomat_threads(),'rate-o-mat aborted after missing fee when skip is disabled')) {
			ok(Utils::Rateomat::check_cdrs('skip disabled, broken reseller: ',
				$cdr_a->{id} => {
					id => $cdr_a->{id},
					rating_status => 'unrated',
				},
			),'broken reseller CDR stayed unrated');
			ok(Utils::Rateomat::check_cdrs('skip disabled, healthy reseller: ',
				$cdr_b->{id} => {
					id => $cdr_b->{id},
					rating_status => 'unrated',
				},
			),'healthy reseller CDR was not fully processed after abort');
		}
	}
}

done_testing();
exit;

sub create_provider {
	my $destination = shift;
	return Utils::Api::setup_provider('test<n>.com',
		[{
			prepaid => 0,
			fees => [{
				direction => 'out',
				destination => $destination,
				onpeak_init_rate        => 6,
				onpeak_init_interval    => $init_secs,
				onpeak_follow_rate      => 1,
				onpeak_follow_interval  => $follow_secs,
				offpeak_init_rate        => 6,
				offpeak_init_interval    => $init_secs,
				offpeak_follow_rate      => 1,
				offpeak_follow_interval  => $follow_secs,
			}],
		}],
		undef,
		{
			prepaid => 0,
			fees => [{
				direction => 'out',
				destination => $destination,
				onpeak_init_rate        => 2,
				onpeak_init_interval    => $init_secs,
				onpeak_follow_rate      => 1,
				onpeak_follow_interval  => $follow_secs,
				offpeak_init_rate        => 2,
				offpeak_init_interval    => $init_secs,
				offpeak_follow_rate      => 1,
				offpeak_follow_interval  => $follow_secs,
			}],
		},
	);
}
