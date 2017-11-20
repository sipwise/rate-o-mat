use strict;
use warnings;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet prepaid/postpaid calls of callers to callees with both using
### dedicated reseller fees.
###
### this tests verify all combinations of prepaid/postpaid subscriber customers with
### balance > 0.0/no balance produce correct customer/reseller call cost, cash balance
### and cash balance interval values.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

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
            my $start_time = Utils::Api::current_unix() - 5;
            if ($prepaid_costs) {
                @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
                    Utils::Rateomat::prepare_prepaid_costs_cdr($_->{subscriber},undef,$_->{reseller},
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',$start_time += 1,$init_secs + $follow_secs,
                            $caller_call_costs,0);
                } ($caller1,$caller2,$caller3) ]) };
            } else {
                @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr($_->{subscriber},undef,$_->{reseller},
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',$start_time += 1,$init_secs + $follow_secs);
                } ($caller1,$caller2,$caller3) ]) };
            }

            if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
                my $no_balance = ($balance <= 0.0);
                my $prepaid_label = ($prepaid ? ($prepaid_costs ? 'prepaid w prepaid costs, ' : 'prepaid w/o prepaid costs, ') : 'postpaid, ');
                my $no_balance_label = ($no_balance ? 'no balance' : 'balance');

                my $caller_cdr_map = {};

                ok(Utils::Rateomat::check_cdrs($prepaid_label.$no_balance_label.': ',
                    map {
                        my $cdr = Utils::Rateomat::get_cdrs($_);
                        $caller_cdr_map->{$cdr->{source_account_id}} = $_;
                        $_ => { id => $_,
                            rating_status => 'ok',
                            source_customer_cost => Utils::Rateomat::decimal_to_string((($prepaid || $no_balance) ? $caller_call_costs : 0.0)),
                            destination_customer_cost => Utils::Rateomat::decimal_to_string((($prepaid || $no_balance) ? $callee_call_costs : 0.0)),
                            source_reseller_cost => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                            destination_reseller_cost => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                        };
                    } @cdr_ids
                ),'cdrs were all processed');

                my $label = $prepaid_label.$no_balance_label.', caller 1: ';
                my $contract_id = $caller1->{customer}->{id};
                my $cash_balance = 100.0 * $balance - (($no_balance || $prepaid_costs) ? 0.0 : $caller_call_costs);
                my $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$caller_cdr_map->{$contract_id},'source','customer','contract_balance_id');
                Utils::Api::check_interval_history($label,$contract_id,[
                    { cash => $cash_balance/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                      id => $balance_id,
                    },
                ]);
                Utils::Rateomat::check_cdr_cash_balance_data($label,$caller_cdr_map->{$contract_id},'source','customer','cash_balance',
                    { before => ($prepaid_costs ? $cash_balance + $caller_call_costs : 100.0 * $balance), after => $cash_balance });

                $label = $prepaid_label.$no_balance_label.', caller 2: ';
                $contract_id = $caller2->{customer}->{id};
                $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$caller_cdr_map->{$contract_id},'source','customer','contract_balance_id');
                Utils::Api::check_interval_history($label,$contract_id,[
                    { cash => $cash_balance/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                      id => $balance_id,
                    },
                ]);
                Utils::Rateomat::check_cdr_cash_balance_data($label,$caller_cdr_map->{$contract_id},'source','customer','cash_balance',
                    { before => ($prepaid_costs ? $cash_balance + $caller_call_costs : 100.0 * $balance), after => $cash_balance });

                $label = $prepaid_label.$no_balance_label.', caller 3: ';
                $contract_id = $caller3->{customer}->{id};
                $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$caller_cdr_map->{$contract_id},'source','customer','contract_balance_id');
                Utils::Api::check_interval_history($label,$contract_id,[
                    { cash => $cash_balance/100.0,
                      debit => (($no_balance && !$prepaid_costs) ? $caller_call_costs : 0.0)/100.0,
                      id => $balance_id,
                    },
                ]);
                Utils::Rateomat::check_cdr_cash_balance_data($label,$caller_cdr_map->{$contract_id},'source','customer','cash_balance',
                    { before => ($prepaid_costs ? $cash_balance + $caller_call_costs : 100.0 * $balance), after => $cash_balance });

                $label = $prepaid_label.$no_balance_label.', callee: ';
                $contract_id = $callee->{customer}->{id};
                $cash_balance = 100.0 * $balance - ($no_balance ? 0.0 : 3 * $callee_call_costs);
                $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination','customer','contract_balance_id');
                Utils::Api::check_interval_history($label,$contract_id,[
                    { cash => $cash_balance/100.0,
                      debit => ($no_balance ? 3 * $callee_call_costs : 0.0)/100.0,
                      id => $balance_id,
                    },
                ]);
                my $bal = 100.0 * $balance;
                my $bal_decrease = ($no_balance ? 0.0 : $callee_call_costs);
                foreach (@cdr_ids) { # ensure id also orders cdrs by start_time
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','customer','contract_balance_id',$balance_id);
                    Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','customer','cash_balance',
                    { before => $bal, after => $bal - $bal_decrease });
                    $bal -= $bal_decrease;
                }

                $label = $prepaid_label.$no_balance_label.', callers\' provider: ';
                $contract_id = $provider_a->{contract}->{id};
                $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'source','reseller','contract_balance_id');
                $total_caller_reseller_call_costs += 3 * $caller_reseller_call_costs;
                Utils::Api::check_interval_history($label,$contract_id,[
                    { debit => $total_caller_reseller_call_costs/100.0,
                      id => $balance_id,
                    },
                ]);
                foreach (@cdr_ids) {
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'source','carrier','contract_balance_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'source','reseller','contract_balance_id',$balance_id);
                }

                $label = $prepaid_label.$no_balance_label.', callee\'s provider: ';
                $contract_id = $callee->{reseller}->{contract_id};
                $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination','reseller','contract_balance_id');
                $total_callee_reseller_call_costs += 3 * $callee_reseller_call_costs;
                Utils::Api::check_interval_history($label,$contract_id,[
                    { debit => $total_callee_reseller_call_costs/100.0,
                      id => $balance_id,
                    },
                ]);
                foreach (@cdr_ids) {
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','carrier','contract_balance_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','reseller','contract_balance_id',$balance_id);
                }

                $label = $prepaid_label.$no_balance_label.' providers and subscriber must not have a profile package: ';
                foreach (@cdr_ids) {
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'source','carrier','profile_package_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'source','reseller','profile_package_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'source','customer','profile_package_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','carrier','profile_package_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','reseller','profile_package_id',undef);
                    Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','customer','profile_package_id',undef);
                }
                $label = $prepaid_label.$no_balance_label.' providers and subscriber must have zero free time: ';
                my $free_time_balance_before_after = { before => 0.0, after => 0.0 };
                foreach (@cdr_ids) {
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','carrier','free_time_balance',undef);
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','reseller','free_time_balance',$free_time_balance_before_after);
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','customer','free_time_balance',$free_time_balance_before_after);
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','carrier','free_time_balance',undef);
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','reseller','free_time_balance',$free_time_balance_before_after);
                    Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','customer','free_time_balance',$free_time_balance_before_after);
                }
                $label = $prepaid_label.$no_balance_label.' providers must have zero balance: ';
                my $cash_balance_before_after = { before => 0.0, after => 0.0 };
                foreach (@cdr_ids) {
                    Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','carrier','cash_balance',undef);
                    Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','reseller','cash_balance',$cash_balance_before_after);
                    Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','carrier','cash_balance',undef);
                    Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','reseller','cash_balance',$cash_balance_before_after);
                }
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
