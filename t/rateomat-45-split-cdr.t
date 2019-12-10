
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;
use List::Util qw();

### testcase outline:
### onnet calls of callers with profiles using different
### onpeak/offpeak rates
###
### this tests verify that offpeak/onpeak rates are correctly
### chosen depending call start time. for alternating offpeak/onpeak
### phases during a single call, another new cdr has to be created
### per peaktime fragment with each rateomat loop ("split peak parts").

local $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} = 1;
local $ENV{RATEOMAT_SPLIT_PEAK_PARTS} = 1;
local $ENV{RATEOMAT_PREPAID_UPDATE_BALANCE} = 1;
local $ENV{RATEOMAT_BATCH_SIZE} = 1;

#use Text::Table;
#use Text::Wrap;
#use Storable;
#use DateTime::Format::Strptime;
#my $tb = Text::Table->new("request", "response");
#*Utils::Api::log_request = sub {
#    my ($req,$res) = @_;
#    if ($tb) {
#        my $dtf = DateTime::Format::Strptime->new(
#            pattern => '%F %T',
#        );
#        #$tb->add(wrap('',"\t",$tb_cnt . ".\t" . $label . ":"),'');
#        my $http_cmd = $req->method . " " . $req->uri;
#        $http_cmd =~ s/\?/?\n/;
#        $tb->add($http_cmd,' ... at ' . $dtf->format_datetime(Utils::Api::get_now()));
#        $tb->add("Request","Response");
#        my $req_data;
#        eval {
#            $req_data = JSON::from_json($req->decoded_content);
#        };
#        my $res_data;
#        eval {
#            $res_data = JSON::from_json($res->decoded_content);
#        };
#        if ($res_data) {
#            $res_data = Storable::dclone($res_data);
#            delete $res_data->{"_links"};
#            $tb->add($req_data ? Utils::Api::to_pretty_json($req_data) : '', Utils::Api::to_pretty_json($res_data));
#        } else {
#            $tb->add($req_data ? Utils::Api::to_pretty_json($req_data) : '', '');
#        }
#        #$tb_cnt++;
#    };
#};

my $extra_rate = 100;

{

    my $call_minutes = 10; #divisible by 2
    my $date = Utils::Api::get_now()->ymd();
    my $t = Utils::Api::datetime_from_string($date . ' 07:30:00');
    Utils::Api::set_time($t);
    my $provider = create_provider($date,$call_minutes);
    my $profile = $provider->{subscriber_fees}->[0]->{profile};
    my $balance = 0; #no balances, for correct source_customer_cost
    my $caller = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $callee = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
    #my $caller_costs = $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_init_rate} * 1 +
    #                   $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2 + 0.99) +
    #                    $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2);
    #my $caller_provider_costs = $provider->{provider_fee}->{fees}->[0]->{offpeak_init_rate} * 60 +
    #                   $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2 + 0.99) +
    #                    $provider->{provider_fee}->{fees}->[0]->{offpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2);
    my @caller_costs = (
        $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_init_rate} * 60, #07:59:50 .. 08:00:50

        $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} * 10 + $extra_rate,

        (map { ($provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_follow_rate} * 60,
        $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} * 60); } (1..(int(($call_minutes - 1) / 2 + 0.99) - 1))), #4x on

        $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_follow_rate} * 50,
    );
    my @caller_provider_costs = (
        $provider->{provider_fee}->{fees}->[0]->{offpeak_init_rate} * 1 + #07:59:50 .. 08:00:50
        $provider->{provider_fee}->{fees}->[0]->{offpeak_follow_rate} * 9 +
        $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} * 50,

        $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} * 10,

        (map { ($provider->{provider_fee}->{fees}->[0]->{offpeak_follow_rate} * 60,
        $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} * 60); } (1..(int(($call_minutes - 1) / 2 + 0.99) - 1))), #4x on

        $provider->{provider_fee}->{fees}->[0]->{offpeak_follow_rate} * 50,
    );
    $t = Utils::Api::datetime_from_string($date . ' 07:59:50');
    Utils::Api::set_time();
    my $cdr = Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$t->epoch,$call_minutes*60),
	])->[0];

    my %cdr_id_map = ();
    my $onpeak = 0; #call starts offpeak
    my $i = 1;
    my $remaining = $call_minutes * 60;
    while (defined $cdr && ok(Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
        # rated fragment:
        $cdr = Utils::Rateomat::get_cdrs($cdr->{id});
        $cdr_id_map{$cdr->{id}} = $cdr;
        Utils::Rateomat::check_cdr('cdr was processed: ',$cdr->{id},{ rating_status => 'ok' });
        Utils::Rateomat::check_cdr('cdr is fragmented: ',$cdr->{id},{ is_fragmented => '1' });
        Utils::Rateomat::check_cdr('cdr is reseller onpeak: ',$cdr->{id},{ frag_reseller_onpeak => "$onpeak" });
        Utils::Rateomat::check_cdr('cdr is customer onpeak: ',$cdr->{id},{ frag_customer_onpeak => "$onpeak" });
        Utils::Rateomat::check_cdr_tag_data("extra rate: ",$cdr->{id},'source','customer','extra_rate',($i == 2 ? 100 : undef));
        Utils::Api::is_float_approx($cdr->{source_customer_cost},$caller_costs[$i-1],'caller costs: ');
        Utils::Api::check_interval_history("caller ",$caller->{customer}->{id},[{
                debit => '~'.List::Util::sum(@caller_costs[0..$i-1])/100.0,
        },]);
        Utils::Api::is_float_approx($cdr->{source_reseller_cost},$caller_provider_costs[$i-1],'caller provider costs: ');
        Utils::Api::check_interval_history("caller provider ",$caller->{reseller}->{contract_id},[{
                debit => '~'.List::Util::sum(@caller_provider_costs[0..$i-1])/100.0,
        },]);
        my $duration = (($remaining < 60) ? $remaining : 60);
        $duration = 10 if $i == 2;
        Utils::Rateomat::check_cdr('cdr duration: ',$cdr->{id},{ duration => $duration.'.000' });
        my @split_cdrs = grep { !exists $cdr_id_map{$_->{id}}; } @{ Utils::Rateomat::get_cdrs_by_call_id($cdr->{call_id}) };
        if ((scalar @split_cdrs) > 0) {
            is(scalar @split_cdrs,1,'exactly one new split cdr');
            $cdr = $split_cdrs[0];
            Utils::Rateomat::check_cdr('split cdr is unrated: ',$cdr->{id},{ rating_status => 'unrated' });
            Utils::Rateomat::check_cdr('split cdr is fragmented: ',$cdr->{id},{ is_fragmented => '1' });
            $remaining -= $duration;
            Utils::Rateomat::check_cdr('split cdr duration: ',$cdr->{id},{ duration => $remaining . '.000' });
            $i++;
            $onpeak = ($onpeak ? 0 : 1);
        } else {
            undef $cdr;
        }
    }
    is(scalar keys %cdr_id_map,$call_minutes + 1,"call was split into " . ($call_minutes + 1) . " cdrs");
    my $duration_sum = 0;
    my $caller_costs_sum = 0;
    my $caller_provider_costs_sum = 0;
    foreach my $cdr_id (keys %cdr_id_map) {
        $cdr = $cdr_id_map{$cdr_id};
        $duration_sum += $cdr->{duration};
        $caller_costs_sum += $cdr->{source_customer_cost};
        $caller_provider_costs_sum += $cdr->{source_reseller_cost};
    }
    ok($duration_sum == $call_minutes * 60,'sum of rated duration is ' . $call_minutes * 60 . ' secs');
    Utils::Api::is_float_approx($caller_costs_sum,List::Util::sum(@caller_costs),'caller costs: ');
    Utils::Api::is_float_approx($caller_provider_costs_sum,List::Util::sum(@caller_provider_costs),'caller provider costs: ');

}

#print $tb->stringify;

done_testing();
exit;

sub create_provider {
    my ($peaktime_special_date,$call_minutes) = @_;
    my $start = Utils::Api::datetime_from_string($peaktime_special_date . ' 08:00:00');
    my @special_peaktimes = ();
    foreach my $i (1..$call_minutes/2) {
        ($start,my $special_peaktime) = get_interleaved_special_peaktimes($start);
        push(@special_peaktimes,$special_peaktime);
    }
    my @peaktimes = (
                peaktime_weekdays => [ map { #offpeak times:
                        ({ weekday => $_,
                           stop => '07:59:59',
                         },
                         { weekday => $_,
                           start => '16:59:59',
                         }); } (0..6)
                    ],
                peaktime_special => \@special_peaktimes,
    );
    return Utils::Api::setup_provider('test<n>.com', [
        # rates:
        {
            fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 30,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => sprintf("%.10f",10/60),
                    onpeak_follow_interval  => 1, #60,
                    offpeak_init_rate        => 3,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => sprintf("%.10f",1/60),
                    offpeak_follow_interval  => 1, #60,

                    onpeak_extra_second     => 900,
                    onpeak_extra_rate       => 200,
                    offpeak_extra_second     => 60,
                    offpeak_extra_rate       => $extra_rate,
                },
            ],
            @peaktimes,
        },
    ], [
        # billing networks:
    ], {
        # provider rate
        fees => [
            {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 40,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => sprintf("%.10f",20/60),
                    onpeak_follow_interval  => 1, #60,
                    offpeak_init_rate        => sprintf("%.10f",4/60),
                    offpeak_init_interval    => 1, #60,
                    offpeak_follow_rate      => sprintf("%.10f",2/60),
                    offpeak_follow_interval  => 1, #60,
            },
        ],
        @peaktimes,
    });
}

sub get_interleaved_special_peaktimes {
    my $start = shift;
    $start->add(seconds => 60);
    my $end = $start->clone->add(seconds => 59);
    my $next_start = $end->clone->add(seconds => 1);
    return ($next_start,
            { start => Utils::Api::datetime_to_string($start),
              stop => Utils::Api::datetime_to_string($end),
        });
}
