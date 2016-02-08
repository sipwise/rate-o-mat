use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

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

{
    foreach my $prepaid ((0,1)) { # prepaid, postpaid
        foreach my $balance ((0.0,10.0)) { # zero balance, enough balance
            my $caller_fee = ($prepaid ? $provider->{subscriber_fees}->[1] : $provider->{subscriber_fees}->[0]);
            my $caller = Utils::Api::setup_subscriber($provider,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });

            my $caller_call_costs = $caller_fee->{fees}->[0]->{onpeak_init_rate} *
                   $caller_fee->{fees}->[0]->{onpeak_init_interval} +
                   $caller_fee->{fees}->[0]->{onpeak_follow_rate} *
                   $caller_fee->{fees}->[0]->{onpeak_follow_interval};

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

                ok(Utils::Rateomat::check_cdrs($prepaid_label.$no_balance_label.': ',
                    map { $_ => {
                        id => $_,
                        rating_status => 'ok',
                        source_customer_cost => sprintf('%6f',(($prepaid || $no_balance) ? $caller_call_costs : 0.0)),
                        #destination_customer_cost => sprintf('%6f',(($prepaid || $no_balance) ? $callee_call_costs : 0.0)),
                        #source_reseller_cost => sprintf('%6f',$caller_reseller_call_costs),
                        #destination_reseller_cost => sprintf('%6f',$callee_reseller_call_costs),
                    }; } @cdr_ids
                ),'cdrs were all processed');
            }
        }
    }

}

done_testing();
exit;
{
    foreach my $prepaid ((0,1)) { # prepaid, postpaid
        foreach my $balance ((0.0,10.0)) { # zero balance, enough balance

            my $callee_fee = ($prepaid ? $provider->{subscriber_fees}->[1] : $provider->{subscriber_fees}->[0]);
            my $callee = Utils::Api::setup_subscriber($provider,$callee_fee->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

            my $caller_call_costs = 0;
                        my $callee_call_costs = $callee_fee->{fees}->[1]->{onpeak_init_rate} *
                               $callee_fee->{fees}->[1]->{onpeak_init_interval} +
                               $callee_fee->{fees}->[1]->{onpeak_follow_rate} *
                               $callee_fee->{fees}->[1]->{onpeak_follow_interval};

            my @cdr_ids;
            if ($prepaid) {
                @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
                    Utils::Rateomat::prepare_prepaid_costs_cdr(undef, $_ ,undef,
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs,
                            $caller_call_costs,0);
                } @offnet_subscribers ]) };
            } else {
                @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr(undef, $_ ,undef,
                            $callee->{subscriber},undef,$callee->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$init_secs + $follow_secs);
                } @offnet_subscribers ]) };
            }

        }
    }

}

