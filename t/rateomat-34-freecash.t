
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
### this tests verify the free cash refill.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

my $init_secs = 50;
my $follow_secs = 20;

{
    my $now = Utils::Api::get_now();
    Utils::Api::set_time($now->clone->subtract(months => 2));
    my $free_cash_in = 60;
    my $free_cash_out = 50;
    my $provider = create_provider($free_cash_in,$free_cash_out);
    my $balance = undef; #5;
    my $caller = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[0]->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    Utils::Api::set_time($now->clone->subtract(months => 2)->add(days => 10));
    my $callee = Utils::Api::setup_subscriber($provider,$provider->{subscriber_fees}->[1]->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $caller_costs = (($provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_init_rate} *
	$init_secs) + ($provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} * $follow_secs))/100.0;
    my $callee_costs = (($provider->{subscriber_fees}->[1]->{fees}->[0]->{onpeak_init_rate} *
	$init_secs) + ($provider->{subscriber_fees}->[1]->{fees}->[0]->{onpeak_follow_rate} * $follow_secs))/100.0;

    my $caller_ratio = _get_free_ratio(Utils::Api::datetime_from_string($caller->{customer}->{create_timestamp}),
        Utils::Api::datetime_from_string($caller->{first_interval}->{start}),
        Utils::Api::datetime_from_string($caller->{first_interval}->{stop}));
    is(Utils::Api::get_cash_balance($caller->{customer})->{ratio},$caller_ratio,"caller ratio $caller_ratio");
    my $callee_ratio = _get_free_ratio(Utils::Api::datetime_from_string($callee->{customer}->{create_timestamp}),
        Utils::Api::datetime_from_string($callee->{first_interval}->{start}),
        Utils::Api::datetime_from_string($callee->{first_interval}->{stop}));
    is(Utils::Api::get_cash_balance($callee->{customer})->{ratio},$callee_ratio,"caller ratio $callee_ratio");

    my $call_duration = $init_secs + $follow_secs - 1;
    Utils::Api::set_time($now->clone->subtract(months => 1));
    my $start_time = Utils::Api::current_unix() - $call_duration;
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
                    source_customer_cost => Utils::Rateomat::decimal_to_string(0),
                    destination_customer_cost => Utils::Rateomat::decimal_to_string(0),
                };
            } @cdr_ids),'cdrs were all processed');
        my $label = 'freecash - caller: ';
        my $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'source','customer','contract_balance_id');
        Utils::Api::check_interval_history($label,$caller->{customer}->{id}, [ {
            profile => $provider->{subscriber_fees}->[0]->{profile}->{id},
            cash => '~' . ($free_cash_out * $caller_ratio),
        },{
            profile => $provider->{subscriber_fees}->[0]->{profile}->{id},
            id => $balance_id,
            cash => '~' . ($free_cash_out - $caller_costs),
        },{
            profile => $provider->{subscriber_fees}->[0]->{profile}->{id},
            cash => '~' . $free_cash_out,
        }]);
        Utils::Rateomat::check_cdr_cash_balance_data($label,$cdr_ids[0],'source','customer','cash_balance',
                    { before => $free_cash_out * 100, after => ($free_cash_out - $caller_costs) * 100 });

        $label = 'freecash - callee: ';
        $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination','customer','contract_balance_id');
        Utils::Api::check_interval_history($label,$callee->{customer}->{id}, [ {
            profile => $provider->{subscriber_fees}->[1]->{profile}->{id},
            cash => '~' . ($free_cash_in * $callee_ratio),
        },{
            profile => $provider->{subscriber_fees}->[1]->{profile}->{id},
            id => $balance_id,
            cash => '~' . ($free_cash_in - $callee_costs),
        },{
            profile => $provider->{subscriber_fees}->[1]->{profile}->{id},
            cash => '~' . $free_cash_in,
        }]);
        Utils::Rateomat::check_cdr_cash_balance_data($label,$cdr_ids[0],'destination','customer','cash_balance',
                    { before => $free_cash_in * 100, after => ($free_cash_in - $callee_costs) * 100 });
    }
}

done_testing();
exit;

sub create_provider {
    my ($free_cash_in,$free_cash_out) = @_;

    return Utils::Api::setup_provider('test<n>.com', [
        # rates:
        {
            interval_free_cash       => $free_cash_out * 100,
            fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 0, #3,
                    onpeak_init_interval    => $init_secs,
                    onpeak_follow_rate      => 0, #3,
                    onpeak_follow_interval  => $follow_secs,
                    offpeak_init_rate        => 0, #2,
                    offpeak_init_interval    => $init_secs,
                    offpeak_follow_rate      => 0, #2,
                    offpeak_follow_interval  => $follow_secs,
                },
            ]
        }, {
            interval_free_cash       => $free_cash_in * 100,
            fees => [
                {
                    direction => 'in',
                    destination => '.',
                    source => '.',
                    onpeak_init_rate        => 0, #4,
                    onpeak_init_interval    => $init_secs,
                    onpeak_follow_rate      => 0, #4,
                    onpeak_follow_interval  => $follow_secs,
                    offpeak_init_rate        => 0, #2,
                    offpeak_init_interval    => $init_secs,
                    offpeak_follow_rate      => 0, #2,
                    offpeak_follow_interval  => $follow_secs,
                },
            ]
        },
    ], [
        # billing networks:
    ]);
}

sub _get_free_ratio {
    my ($ctime,$stime,$etime) = @_;
    my $start_of_next_interval = _add_second($etime->clone,1);
    $ctime = $ctime->clone->truncate(to => 'day') > $stime ? $ctime->clone->truncate(to => 'day') : $ctime;
    return ($start_of_next_interval->epoch - $ctime->epoch) / ($start_of_next_interval->epoch - $stime->epoch);
}

sub _add_second {

    my ($dt,$skip_leap_seconds) = @_;
    $dt->add(seconds => 1);
    while ($skip_leap_seconds and $dt->second() >= 60) {
        $dt->add(seconds => 1);
    }
    return $dt;

}