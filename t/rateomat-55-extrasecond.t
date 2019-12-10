use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet postpaid calls of callers using fees with "extra_second" rate.
###
### this tests verify charging the extra_second.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;
local $ENV{RATEOMAT_PREPAID_UPDATE_BALANCE} = 1;

my $init_secs = 60;
my $follow_secs = 30;
my $extra_rate = 100;

foreach my $extra_second (undef, 0, 60, $init_secs + $follow_secs - 1,$init_secs + $follow_secs) {
    my $provider_a = create_provider('testa<n>.com',$extra_second);
    my $provider_b = create_provider('testb<n>.com');
    my $balance = 0;
    my $caller_fee = $provider_a->{subscriber_fees}->[0];
    my $caller = Utils::Api::setup_subscriber($provider_a,$caller_fee->{profile},$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $callee_fee = $provider_b->{subscriber_fees}->[0];
    my $callee = Utils::Api::setup_subscriber($provider_b,$callee_fee->{profile},$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });

    my $duration = $init_secs + $follow_secs;
    my $is_extra_second = (defined $extra_second and $extra_second < $duration);
    my $_extra_rate = ($is_extra_second ? $extra_rate : 0);
    my $caller_call_costs = $caller_fee->{fees}->[0]->{onpeak_init_rate} *
                            $caller_fee->{fees}->[0]->{onpeak_init_interval} +
                            $caller_fee->{fees}->[0]->{onpeak_follow_rate} *
                            $caller_fee->{fees}->[0]->{onpeak_follow_interval} + $_extra_rate;
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

    my $start_time = Utils::Api::current_unix() - 5;
    my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
        Utils::Rateomat::prepare_cdr($_->{subscriber},undef,$_->{reseller},
        $callee->{subscriber},undef,$callee->{reseller},
        '192.168.0.1',$start_time += 1,$duration);
    } ($caller,$caller) ]) };

    if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
        my $caller_cdr_map = {};

        my $label = 'extra_second ' . $extra_second . 'extra_rate ' . $extra_second . ': ';
        ok(Utils::Rateomat::check_cdrs($label,
            map {
                my $cdr = Utils::Rateomat::get_cdrs($_);
                $caller_cdr_map->{$cdr->{source_account_id}} = $_;
                $_ => { id => $_,
                    rating_status => 'ok',
                    source_customer_cost => Utils::Rateomat::decimal_to_string($caller_call_costs),
                    destination_customer_cost => Utils::Rateomat::decimal_to_string($callee_call_costs),
                    source_reseller_cost => Utils::Rateomat::decimal_to_string($caller_reseller_call_costs),
                    destination_reseller_cost => Utils::Rateomat::decimal_to_string($callee_reseller_call_costs),
                };
            } @cdr_ids
        ),'cdrs were all processed');
        if ($is_extra_second) {
            foreach (@cdr_ids) {
                Utils::Rateomat::check_cdr_tag_data($label,$_,'source','carrier','extra_rate',undef);
                Utils::Rateomat::check_cdr_tag_data($label,$_,'source','reseller','extra_rate',undef);
                Utils::Rateomat::check_cdr_tag_data($label,$_,'source','customer','extra_rate',($is_extra_second ? $_extra_rate : undef));
                Utils::Rateomat::check_cdr_tag_data($label,$_,'destination','carrier','extra_rate',undef);
                Utils::Rateomat::check_cdr_tag_data($label,$_,'destination','reseller','extra_rate',undef);
                Utils::Rateomat::check_cdr_tag_data($label,$_,'destination','customer','extra_rate',undef);
            }
        }

    }
}

done_testing();
exit;

sub create_provider {
    my ($domain,$extra_second) = @_;
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

                onpeak_extra_second     => $extra_second,
                onpeak_extra_rate       => $extra_rate,
                offpeak_extra_second     => $extra_second,
                offpeak_extra_rate       => $extra_rate,
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
