use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

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

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

my $init_secs = 60;
my $follow_secs = 30;
my @offnet_subscribers = (Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'),
                   Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'),
                   Utils::Rateomat::prepare_offnet_subsriber_info({ cc => 999, ac => '2<n>', sn => '<t>' },'somewhere.tld'));

#goto SKIP;
{

    foreach my $ptype (('reseller')) { # 'sippeering'
      my $prefix = "onnet $ptype caller calls offnet callees - ";
      my $provider = create_subscriber($ptype);
      my $is_carrier;
      my $provider_type;
      if ($ptype ne 'reseller') {
        $provider_type = 'carrier';
        $is_carrier = 1;
      } else {
        $provider_type = 'reseller';
        $is_carrier = 0;
      }
      my $total_reseller_call_costs = 0.0;
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

              my $start_time = Utils::Api::current_unix() - 5;
              my @cdr_ids;
              if ($prepaid) {
                  @cdr_ids = map { $_->{cdr}->{id}; } @{ Utils::Rateomat::create_prepaid_costs_cdrs([ map {
                      Utils::Rateomat::prepare_prepaid_costs_cdr($caller->{subscriber},undef,$caller->{reseller},
                              undef, $_ ,undef,
                              '192.168.0.1',$start_time += 1,$init_secs + $follow_secs,
                              $caller_call_costs,0);
                  } @offnet_subscribers ]) };
              } else {
                  @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                      Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
                              undef, $_ ,undef,
                              '192.168.0.1',$start_time += 1,$init_secs + $follow_secs);
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
                          'source_'.$provider_type.'_cost' => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                          'destination_'.$provider_type.'_cost' => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                      }; } @cdr_ids
                  ),'cdrs were all processed');

                  my $label = $prefix.$prepaid_label.$no_balance_label.', caller: ';
                  my $cash_balance = 100.0 * $balance - (($no_balance || $prepaid) ? 0.0 : 3 * $caller_call_costs);
                  my $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'source','customer','contract_balance_id');
                  Utils::Api::check_interval_history($label,$caller->{customer}->{id},[
                      { cash => $cash_balance/100.0,
                        debit => 3 * $caller_call_costs/100.0, #(($no_balance && !$prepaid) ? 3 * $caller_call_costs : 0.0)/100.0,
                        id => $balance_id,
                      },
                  ]);
                  my $bal = ($prepaid ? $cash_balance + $caller_call_costs : 100.0 * $balance);
                  my $bal_decrease = (($no_balance && !$prepaid) ? 0.0 : $caller_call_costs);
                  foreach (@cdr_ids) { # ensure id also orders cdrs by start_time
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','customer','contract_balance_id',$balance_id);
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','customer','cash_balance',
                      { before => $bal, after => $bal - $bal_decrease });
                      $bal -= $bal_decrease if !$prepaid;
                  }

                  $label = $prefix.$prepaid_label.$no_balance_label.', callers\' provider: ';
                  $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'source',$provider_type,'contract_balance_id');
                  $total_reseller_call_costs += 3 * $caller_reseller_call_costs;
                  Utils::Api::check_interval_history($label,$provider->{contract}->{id},[
                      { debit => $total_reseller_call_costs/100.0,
                        id => $balance_id,
                      },
                  ]);

                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and subscriber must not have a profile package: ';
                  foreach (@cdr_ids) {
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','carrier','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','reseller','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','customer','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','carrier','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','reseller','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','customer','profile_package_id',undef);
                  }
                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and subscriber must have zero free time: ';
                  my $free_time_balance_before_after = { before => 0.0, after => 0.0 };
                  foreach (@cdr_ids) {
                      if ($is_carrier) {
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','carrier','free_time_balance',$free_time_balance_before_after);
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','reseller','free_time_balance',undef);
                      } else {
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','carrier','free_time_balance',undef);
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','reseller','free_time_balance',$free_time_balance_before_after);
                      }
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','customer','free_time_balance',$free_time_balance_before_after);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','carrier','free_time_balance',undef);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','reseller','free_time_balance',undef);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','customer','free_time_balance',undef);
                  }
                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and destination customers must have zero balance: ';
                  my $cash_balance_before_after = { before => 0.0, after => 0.0 };
                  foreach (@cdr_ids) {
                      if ($is_carrier) {
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','carrier','cash_balance',$cash_balance_before_after);
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','reseller','cash_balance',undef);
                      } else {
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','carrier','cash_balance',undef);
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','reseller','cash_balance',$cash_balance_before_after);
                      }
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','customer','cash_balance',undef);
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','carrier','cash_balance',undef);
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','reseller','cash_balance',undef);
                  }

              }
          }
      }
    }
}

#SKIP:
{
    foreach my $ptype (('reseller')) { # 'sippeering'
      my $prefix = "offnet callers call onnet $ptype callee - ";
      my $provider = create_subscriber($ptype);
      my $is_carrier;
      my $provider_type;
      if ($ptype ne 'reseller') {
        $provider_type = 'carrier';
        $is_carrier = 1;
      } else {
        $provider_type = 'reseller';
        $is_carrier = 0;
      }
      my $total_reseller_call_costs = 0.0;
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

              my $start_time = Utils::Api::current_unix() - 5;
              my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                      Utils::Rateomat::prepare_cdr(undef, $_ ,undef,
                              $callee->{subscriber},undef,$callee->{reseller},
                              '192.168.0.1',$start_time += 1,$init_secs + $follow_secs);
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
                          'source_'.$provider_type.'_cost' => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                          'destination_'.$provider_type.'_cost' => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                      }; } @cdr_ids
                  ),'cdrs were all processed');

                  my $label = $prefix.$prepaid_label.$no_balance_label.', callee: ';
                  my $cash_balance = 100.0 * $balance - ($no_balance ? 0.0 : 3 * $callee_call_costs);
                  my $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination','customer','contract_balance_id');
                  Utils::Api::check_interval_history($label,$callee->{customer}->{id},[
                      { cash => $cash_balance/100.0,
                        debit => 3 * $callee_call_costs/100.0, #($no_balance ? 3 * $callee_call_costs : 0.0)/100.0,
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

                  $label = $prefix.$prepaid_label.$no_balance_label.', callee\'s provider: ';
                  $balance_id = Utils::Rateomat::get_cdr_relation_data($label,$cdr_ids[0],'destination',$provider_type,'contract_balance_id');
                  $total_reseller_call_costs += 3 * $callee_reseller_call_costs;
                  Utils::Api::check_interval_history($label,$callee->{reseller}->{contract_id},[
                      { debit => $total_reseller_call_costs/100.0,
                        id => $balance_id,
                      },
                  ]);

                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and subscriber must not have a profile package: ';
                  foreach (@cdr_ids) {
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','carrier','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','reseller','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'source','customer','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','carrier','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','reseller','profile_package_id',undef);
                      Utils::Rateomat::check_cdr_relation_data($label,$_,'destination','customer','profile_package_id',undef);
                  }
                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and subscriber must have zero free time: ';
                  my $free_time_balance_before_after = { before => 0.0, after => 0.0 };
                  foreach (@cdr_ids) {
                      if ($is_carrier) {
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','carrier','free_time_balance',$free_time_balance_before_after);
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','reseller','free_time_balance',undef);
                      } else {
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','carrier','free_time_balance',undef);
                        Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','reseller','free_time_balance',$free_time_balance_before_after);
                      }
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'destination','customer','free_time_balance',$free_time_balance_before_after);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','carrier','free_time_balance',undef);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','reseller','free_time_balance',undef);
                      Utils::Rateomat::check_cdr_time_balance_data($label,$_,'source','customer','free_time_balance',undef);
                  }
                  $label = $prefix.$prepaid_label.$no_balance_label.' providers and destination customers must have zero balance: ';
                  my $cash_balance_before_after = { before => 0.0, after => 0.0 };
                  foreach (@cdr_ids) {
                      if ($is_carrier) {
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','carrier','cash_balance',$cash_balance_before_after);
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','reseller','cash_balance',undef);
                      } else {
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','carrier','cash_balance',undef);
                        Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'destination','reseller','cash_balance',$cash_balance_before_after);
                      }
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','customer','cash_balance',undef);
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','carrier','cash_balance',undef);
                      Utils::Rateomat::check_cdr_cash_balance_data($label,$_,'source','reseller','cash_balance',undef);
                  }

              }

          }
      }
    }
}

done_testing();
exit;

sub create_subscriber {
  my $type = shift;
  return Utils::Api::setup_provider('test<n>.com',
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
    $type
  );
}
