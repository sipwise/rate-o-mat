use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

my $init_secs = 60;
my $follow_secs = 30;
my $provider_a = create_provider('testa.com');
my $provider_b = create_provider('testb.com');

my $total_caller_reseller_call_costs = 0.0;
my $total_callee_reseller_call_costs = 0.0;

# full matrix:
foreach my $prepaid ((0,1)) { # prepaid, postpaid
    foreach my $balance ((0.0,10.0)) { # zero balance, enough balance
        foreach my $prepaid_costs (0..1) { # prepaid: with prepaid_costs records and without (swrate down)
            next if (!$prepaid && $prepaid_costs);
            my $caller_fee = ($prepaid ? $provider_a->{subscriber_fees}->[1] : $provider_a->{subscriber_fees}->[0]);
            my $caller1 = Utils::Api::setup_subscriber($provider_a,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
            my $caller2 = Utils::Api::setup_subscriber($provider_a,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
            my $caller3 = Utils::Api::setup_subscriber($provider_a,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
            my $callee_fee = ($prepaid ? $provider_b->{subscriber_fees}->[1] : $provider_b->{subscriber_fees}->[0]);
            my $callee = Utils::Api::setup_subscriber($provider_b,$callee_fee->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

            my $caller_call_costs = $caller_fee->{fees}->[0]->{onpeak_init_rate} *
                               $caller_fee->{fees}->[0]->{onpeak_init_interval} +
                               $caller_fee->{fees}->[0]->{onpeak_follow_rate} *
                               $caller_fee->{fees}->[0]->{onpeak_follow_interval};
            my $caller_reseller_call_costs = $provider_a->{provider_fee}->{fees}->[0]->{onpeak_init_rate} *
                               $provider_a->{provider_fee}->{fees}->[0]->{onpeak_init_interval} +
                               $provider_a->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} *
                               $provider_a->{provider_fee}->{fees}->[0]->{onpeak_follow_interval};

            my $callee_call_costs = $callee_fee->{fees}->[1]->{onpeak_init_rate} *
                               $callee_fee->{fees}->[1]->{onpeak_init_interval} +
                               $callee_fee->{fees}->[1]->{onpeak_follow_rate} *
                               $callee_fee->{fees}->[1]->{onpeak_follow_interval};
            my $callee_reseller_call_costs = $provider_b->{provider_fee}->{fees}->[1]->{onpeak_init_rate} *
                               $provider_b->{provider_fee}->{fees}->[1]->{onpeak_init_interval} +
                               $provider_b->{provider_fee}->{fees}->[1]->{onpeak_follow_rate} *
                               $provider_b->{provider_fee}->{fees}->[1]->{onpeak_follow_interval};

            my @cdr_ids;
            if ($prepaid_costs) {
                @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
                    Utils::Rateomat::prepare_prepaid_costs_cdr($_->{subscriber},undef,$_->{reseller},
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs,
                            $caller_call_costs,0);
                } ($caller1,$caller2,$caller3) ]) };
            } else {
                @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr($_->{subscriber},undef,$_->{reseller},
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs);
                } ($caller1,$caller2,$caller3) ]) };
            }

            if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
                my $no_balance = ($balance <= 0.0);
                my $prepaid_label = ($prepaid ? ($prepaid_costs ? 'prepaid w prepaid costs, ' : 'prepaid w/o prepaid costs, ') : 'postpaid, ');
                my $no_balance_label = ($no_balance ? 'no balance' : 'balance');

                ok(Utils::Rateomat::check_cdrs($prepaid_label.$no_balance_label.': ',
                    map { $_ => {
                        id => $_,
                        rating_status => 'ok',
                        source_customer_cost => sprintf('%6f',(($prepaid || $no_balance) ? $caller_call_costs : 0.0)),
                        destination_customer_cost => sprintf('%6f',(($prepaid || $no_balance) ? $callee_call_costs : 0.0)),
                        source_reseller_cost => sprintf('%6f',$caller_reseller_call_costs),
                        destination_reseller_cost => sprintf('%6f',$callee_reseller_call_costs),
                    }; } @cdr_ids
                ),'cdrs were all processed');



                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', caller 1: ',$caller1->{customer}->{id},[
                    { cash => (100.0 * $balance - (($no_balance || $prepaid_costs) ? 0.0 : $caller_call_costs))/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                    },
                ]);
                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', caller 2: ',$caller2->{customer}->{id},[
                    { cash => (100.0 * $balance - (($no_balance || $prepaid_costs) ? 0.0 : $caller_call_costs))/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                    },
                ]);
                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', caller 3: ',$caller3->{customer}->{id},[
                    { cash => (100.0 * $balance - (($no_balance || $prepaid_costs) ? 0.0 : $caller_call_costs))/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                    },
                ]);

                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', callee: ',$callee->{customer}->{id},[
                    { cash => (100.0 * $balance - ($no_balance ? 0.0 : 3 * $callee_call_costs))/100.0,
                      debit => ($no_balance ? 3 * $callee_call_costs : 0.0)/100.0,
                    },
                ]);

                $total_caller_reseller_call_costs += 3 * $caller_reseller_call_costs;
                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', callers\' provider: ',$provider_a->{contract}->{id},[
                    { debit => $total_caller_reseller_call_costs/100.0,
                    },
                ]);

                $total_callee_reseller_call_costs += 3 * $callee_reseller_call_costs;
                Utils::Api::check_interval_history($prepaid_label.$no_balance_label.', callee\'s provider: ',$callee->{reseller}->{contract_id},[
                    { debit => $total_callee_reseller_call_costs/100.0,
                    },
                ]);

            }
        }
    }
}

done_testing();
exit;

sub create_provider {
    my $domain = shift;
    return Utils::Api::setup_provider($domain,
        [ #subscriber rates:
            { prepaid => 0,
              fees => [{ #outgoing:
                direction => 'out',
                destination => '^8882.+',
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
                source => '^8881.+',
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
                destination => '^8882.+',
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
                source => '^8881.+',
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
                destination => '^888.+',
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
                source => '^888.+',
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
}
