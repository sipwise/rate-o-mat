
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### onnet calls between subscribers of multiple resellers
### are rated by multiple rateomat instances running
### concurrently
###
### this tests verify that ratomat can be run safely against
### one and the same accounting.cdr table.

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;

{

    my $number_of_rateomat_threads = 3;
    my $rateomat_timeout = 120;
    my $number_of_providers = 3;
    my $number_of_subscribers_per_provider = 3;
    my $balance = 0.0;
    my %subscribers = ();
    foreach (1..$number_of_providers) {
        my $rate_interval = 30 + int(rand(31));
        my $provider = create_provider($rate_interval);

        my $caller_fee = $provider->{subscriber_fees}->[0];

        foreach (1..$number_of_subscribers_per_provider) {
            my $subscriber = Utils::Api::setup_subscriber($provider,$caller_fee->{profile},$balance,{ cc => 888, ac => '3<n>', sn => '<t>' });
            $subscriber->{provider} = $provider;
            $subscriber->{fee} = $caller_fee;
            $subscriber->{rate_interval} = $rate_interval;
            $subscribers{$subscriber->{customer}->{id}} = $subscriber;
        }
    }

    my @caller_callee_matrix = ();
    # add calls from each subscriber to each subscriber except itself:
    Utils::Api::cartesian_product(sub {
        my ($caller,$callee) = @_;
        push(@caller_callee_matrix,{ caller => $caller, callee => $callee }) unless $caller->{customer}->{id} == $callee->{customer}->{id};
    },[ values %subscribers ],[ values %subscribers ]);
    ## add calls from each subscriber to itself:
    #Utils::Api::cartesian_product(sub {
    #    my ($caller,$callee) = @_;
    #    push(@caller_callee_matrix,{ caller => $caller, callee => $callee }) if $caller->{customer}->{id} == $callee->{customer}->{id};
    #},[ values %subscribers ],[ values %subscribers ]);

    # place calls by generating cdrs:
    my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([ map {
                    Utils::Rateomat::prepare_cdr($_->{caller}->{subscriber},undef,$_->{caller}->{reseller},
                            $_->{callee}->{subscriber},undef,$_->{callee}->{reseller},
                            '192.168.0.1',Utils::Api::current_unix(),$_->{caller}->{rate_interval} + 1);
                } @caller_callee_matrix ]) };

    my $number_of_calls = ($number_of_providers * $number_of_subscribers_per_provider) * ($number_of_providers * $number_of_subscribers_per_provider - 1);
    # Ensure all rateomats grab all cdrs at once.
    local $ENV{RATEOMAT_BATCH_SIZE} = $number_of_calls;
    # Enable and see the speedup.
    local $ENV{RATEOMAT_SHUFFLE_BATCH} = 1;

    if (ok((scalar @cdr_ids) == $number_of_calls,'there are '.$number_of_calls.' calls to rate')
        && ok(Utils::Rateomat::run_rateomat_threads($number_of_rateomat_threads, $rateomat_timeout),'rate-o-mat threads executed')) {

        ok(Utils::Rateomat::check_cdrs('',
            map {
                    my $cdr = Utils::Rateomat::get_cdrs($_);
                    my $caller = $subscribers{$cdr->{source_account_id}};
                    my $call_costs = $caller->{fee}->{fees}->[0]->{onpeak_init_rate} *
                               $caller->{fee}->{fees}->[0]->{onpeak_init_interval} +
                               $caller->{fee}->{fees}->[0]->{onpeak_follow_rate} *
                               $caller->{fee}->{fees}->[0]->{onpeak_follow_interval};
                    $caller->{call_costs} += $call_costs;
                    $_ => {
                        id => $_,
                        rating_status => 'ok',
                        source_customer_cost => Utils::Rateomat::decimal_to_string($call_costs),
                        destination_customer_cost => Utils::Rateomat::decimal_to_string(0.0),
                        source_reseller_cost => Utils::Rateomat::decimal_to_string(0.0),
                        destination_reseller_cost => Utils::Rateomat::decimal_to_string(0.0),
                    };
                } @cdr_ids
        ),'cdrs were all processed');

        foreach (keys %subscribers) {
            my $caller = $subscribers{$_};
            Utils::Api::check_interval_history("caller $_: ",$_,[
                { #cash => (100.0 * $balance - $caller->{call_costs})/100.0,
                  debit => $caller->{call_costs}/100.0,
                },
            ]);
        }

    }

}


done_testing();
exit;

sub create_provider {
	my $rate_interval = shift;
	$rate_interval //= 60;
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{ #any
				onpeak_init_rate        => 2,
				onpeak_init_interval    => $rate_interval,
				onpeak_follow_rate      => 1,
				onpeak_follow_interval  => $rate_interval,
				offpeak_init_rate        => 2,
				offpeak_init_interval    => $rate_interval,
				offpeak_follow_rate      => 1,
				offpeak_follow_interval  => $rate_interval,
			},
		],
		[ #billing networks:
		]
	);
}
