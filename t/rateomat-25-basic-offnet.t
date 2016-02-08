use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### prepaid/postpaid calls of onnet callers to offnet callees and
### offnet callers to onnet callees
###
### this tests verify all combinations of prepaid/postpaid subscriber customers with
### balance > 0.0/no balance produce correct customer/reseller call cost, cash balance
### and cash balance interval values.

my $init_secs = 60;
my $follow_secs = 30;
my $provider = Utils::Api::setup_provider('test.com',
    [ #subscriber rates:
        { prepaid => 0,
          fees => [{ #outgoing:
            direction => 'out',
            destination => '^999.+',
            onpeak_init_rate        => 6,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 6,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        },
        { #incoming:
            direction => 'in',
            destination => '.',
            source => '^999.+',
            onpeak_init_rate        => 5,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 5,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        }]},
        { prepaid => 1,
          fees => [{ #outgoing:
            direction => 'out',
            destination => '^999.+',
            onpeak_init_rate        => 4,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 4,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        },
        { #incoming:
            direction => 'in',
            destination => '.',
            source => '^999.+',
            onpeak_init_rate        => 3,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 3,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        }]},
    ],
    undef, # no billing networks in this test suite
    # provider rate:
    { prepaid => 0,
          fees => [{ #outgoing:
            direction => 'out',
            destination => '^999.+',
            onpeak_init_rate        => 2,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 2,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        },
        { #incoming:
            direction => 'in',
            destination => '.',
            source => '^999.+',
            onpeak_init_rate        => 1,
            onpeak_init_interval    => $init_secs,
            onpeak_follow_rate      => 1,
            onpeak_follow_interval  => $follow_secs,
            offpeak_init_rate        => 1,
            offpeak_init_interval    => $init_secs,
            offpeak_follow_rate      => 1,
            offpeak_follow_interval  => $follow_secs,
        }]},

);
my @offnet_subscribers = (Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'),
                   Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'),
                   Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'));

my $total_reseller_call_costs = 0.0;

#goto SKIP;
{

    my $prefix = 'onnet caller calls offnet callee - ';
    foreach my $prepaid ((0,1)) { # prepaid, postpaid
        foreach my $balance ((0.0,30.0)) { # zero balance, enough balance
            my $caller_fee = ($prepaid ? $provider->{subscriber_fees}->[1] : $provider->{subscriber_fees}->[0]);
            my $caller = Utils::Api::setup_subscriber($provider,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });

            my $caller_call_costs = $caller_fee->{fees}->[0]->{onpeak_init_rate} *
                   $caller_fee->{fees}->[0]->{onpeak_init_interval} +
                   $caller_fee->{fees}->[0]->{onpeak_follow_rate} *
                   $caller_fee->{fees}->[0]->{onpeak_follow_interval};
            my $caller_reseller_call_costs = $provider->{provider_fee}->{fees}->[0]->{onpeak_init_rate} *
                               $provider->{provider_fee}->{fees}->[0]->{onpeak_init_interval} +
                               $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} *
                               $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_interval};

            my $callee_call_costs = 0;
            my $callee_reseller_call_costs = 0;

            my @cdr_ids;
            if ($prepaid) {
                @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
                    Utils::Rateomat::prepare_prepaid_costs_cdr($caller->{subscriber},undef,$caller->{reseller},
                            undef, $_ ,undef,
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs,
                            $caller_call_costs,0);
                } @offnet_subscribers ]) };
            } else {
                @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
                            undef, $_ ,undef,
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs);
                } @offnet_subscribers ]) };
            }

            if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
                my $no_balance = ($balance <= 0.0);
                my $prepaid_label = ($prepaid ? 'prepaid, ' : 'postpaid, ');
                my $no_balance_label = ($no_balance ? 'no balance' : 'balance');

                ok(Utils::Rateomat::check_cdrs($prefix.$prepaid_label.$no_balance_label.': ',
                    map { $_ => {
                        id => $_,
                        rating_status => 'ok',
                        source_customer_cost => Utils::Rateomat::decimal_to_string((($prepaid || $no_balance) ? $caller_call_costs : 0.0)),
                        destination_customer_cost => Utils::Rateomat::decimal_to_string((($prepaid || $no_balance) ? $callee_call_costs : 0.0)),
                        source_reseller_cost => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                        destination_reseller_cost => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                    }; } @cdr_ids
                ),'cdrs were all processed');

                Utils::Api::check_interval_history($prefix.$prepaid_label.$no_balance_label.', caller: ',$caller->{customer}->{id},[
                    { cash => (100.0 * $balance - (($no_balance || $prepaid) ? 0.0 : 3 * $caller_call_costs))/100.0,
                      debit => (($no_balance && !$prepaid) ? 3 * $caller_call_costs : 0.0)/100.0,
                    },
                ]);

                $total_reseller_call_costs += 3 * $caller_reseller_call_costs;
                Utils::Api::check_interval_history($prefix.$prepaid_label.$no_balance_label.', callers\' provider: ',$provider->{contract}->{id},[
                    { debit => $total_reseller_call_costs/100.0,
                    },
                ]);

            }
        }
    }

}

#SKIP:
{
    my $prefix = 'offnet caller calls onnet callee - ';
    foreach my $prepaid ((0,1)) { # prepaid, postpaid
        foreach my $balance ((0.0,30.0)) { # zero balance, enough balance

            my $callee_fee = ($prepaid ? $provider->{subscriber_fees}->[1] : $provider->{subscriber_fees}->[0]);
            my $callee = Utils::Api::setup_subscriber($provider,$callee_fee->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

            my $caller_call_costs = 0;
            my $caller_reseller_call_costs = 0;

            my $callee_call_costs = $callee_fee->{fees}->[1]->{onpeak_init_rate} *
                $callee_fee->{fees}->[1]->{onpeak_init_interval} +
                $callee_fee->{fees}->[1]->{onpeak_follow_rate} *
                $callee_fee->{fees}->[1]->{onpeak_follow_interval};
            my $callee_reseller_call_costs = $provider->{provider_fee}->{fees}->[1]->{onpeak_init_rate} *
                               $provider->{provider_fee}->{fees}->[1]->{onpeak_init_interval} +
                               $provider->{provider_fee}->{fees}->[1]->{onpeak_follow_rate} *
                               $provider->{provider_fee}->{fees}->[1]->{onpeak_follow_interval};

            my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr(undef, $_ ,undef,
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs);
                } @offnet_subscribers ]) };

            if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
                my $no_balance = ($balance <= 0.0);
                my $prepaid_label = ($prepaid ? 'prepaid, ' : 'postpaid, ');
                my $no_balance_label = ($no_balance ? 'no balance' : 'balance');

                ok(Utils::Rateomat::check_cdrs($prefix.$prepaid_label.$no_balance_label.': ',
                    map { $_ => {
                        id => $_,
                        rating_status => 'ok',
                        source_customer_cost => Utils::Rateomat::decimal_to_string($caller_call_costs),
                        destination_customer_cost => Utils::Rateomat::decimal_to_string((($prepaid || $no_balance) ? $callee_call_costs : 0.0)),
                        source_reseller_cost => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                        destination_reseller_cost => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                    }; } @cdr_ids
                ),'cdrs were all processed');

                Utils::Api::check_interval_history($prefix.$prepaid_label.$no_balance_label.', callee: ',$callee->{customer}->{id},[
                    { cash => (100.0 * $balance - ($no_balance ? 0.0 : 3 * $callee_call_costs))/100.0,
                      debit => ($no_balance ? 3 * $callee_call_costs : 0.0)/100.0,
                    },
                ]);

                $total_reseller_call_costs += 3 * $callee_reseller_call_costs;
                Utils::Api::check_interval_history($prefix.$prepaid_label.$no_balance_label.', callee\'s provider: ',$callee->{reseller}->{contract_id},[
                    { debit => $total_reseller_call_costs/100.0,
                    },
                ]);

            }

        }
    }

}

done_testing();
exit;
