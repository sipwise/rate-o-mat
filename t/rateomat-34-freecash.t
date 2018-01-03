
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet calls that consume profile's freecash
###
### this tests verify that rating correctly
### consumes up free time before cash balance.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

my $init_secs = 50;
my $follow_secs = 20;

{
    my $now = Utils::Api::get_now();
    my $begin = $now->clone->subtract(months => 2);
    Utils::Api::set_time($begin);
    my $provider = create_provider(60 * 100,50 * 100);
    my $balance = 5;
    my $caller = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[1]->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $caller_costs = 2 * ($provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} *
	$provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_interval})/100.0;
    my $callee_costs = 2 * ($provider->{subscriber_fees}->[1]->{fees}->[0]->{onpeak_follow_rate} *
	$provider->{subscriber_fees}->[1]->{fees}->[0]->{onpeak_follow_interval})/100.0;

    my $call_duration = $follow_secs + $follow_secs;
    Utils::Api::set_time($now->clone->truncate(to => 'month'));
    my $start_time = Utils::Api::current_unix() - $call_duration - 1;
    Utils::Api::set_time();
    my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
        Utils::Rateomat::prepare_cdr($caller->{subscriber}, undef,
            $caller->{reseller}, $callee->{subscriber}, undef,
            $callee->{reseller}, '192.168.0.1', $start_time, $call_duration),
    ]) };

    if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
        ok(Utils::Rateomat::check_cdrs('',
            map { $_ => {
                    id => $_,
                    rating_status => 'ok',
                    source_customer_free_time => $out_free_time,
                    destination_customer_free_time => $in_free_time,
                };
            } @cdr_ids),'cdrs were all processed');
        my $label = 'freetime - caller: ';
        my $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'source','customer','contract_balance_id');
        Utils::Api::check_interval_history($label,$caller->{customer}->{id}, [ {
            profile => $provider->{subscriber_fees}->[0]->{profile}->{id},
            id => $balance_id,
            $in_free_time < $out_free_time ? (cash => $balance - $caller_costs) : (),
        }]);
        Utils::Rateomat::check_cdr_time_balance_data($label,$cdr_ids[0],'source','customer','free_time_balance',
                    { before => $out_free_time, after => 0 });
        $label = 'freetime - callee: ';
        $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination','customer','contract_balance_id');
        Utils::Api::check_interval_history($label,$callee->{customer}->{id}, [ {
            profile => $provider->{subscriber_fees}->[1]->{profile}->{id},
            id => $balance_id,
            $in_free_time > $out_free_time ? (cash => $balance - $callee_costs) : (),
        }]);
        Utils::Rateomat::check_cdr_time_balance_data($label,$cdr_ids[0],'destination','customer','free_time_balance',
                    { before => $in_free_time, after => 0 });
    }
}

done_testing();
exit;

sub create_provider {
    my ($free_cash_in,$free_cash_out) = @_;

    return Utils::Api::setup_provider('test<n>.com', [
        # rates:
        {
            interval_free_cash       => $free_cash_out,
            fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 3,
                    onpeak_init_interval    => $init_secs,
                    onpeak_follow_rate      => 3,
                    onpeak_follow_interval  => $follow_secs,
                    offpeak_init_rate        => 2,
                    offpeak_init_interval    => $init_secs,
                    offpeak_follow_rate      => 2,
                    offpeak_follow_interval  => $follow_secs,
                },
            ]
        }, {
            interval_free_cash       => $free_cash_in,
            fees => [
                {
                    direction => 'in',
                    destination => '.',
                    source => '.',
                    onpeak_init_rate        => 4,
                    onpeak_init_interval    => $init_secs,
                    onpeak_follow_rate      => 4,
                    onpeak_follow_interval  => $follow_secs,
                    offpeak_init_rate        => 2,
                    offpeak_init_interval    => $init_secs,
                    offpeak_follow_rate      => 2,
                    offpeak_follow_interval  => $follow_secs,
                },
            ]
        },
    ], [
        # billing networks:
    ]);
}
