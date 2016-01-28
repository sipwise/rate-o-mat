
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

$ENV{RATEOMAT_SPLIT_PEAK_PARTS} = 1;

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
    my $caller_costs = $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_init_rate} * 60 +
                       $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2 + 0.99) +
                        $provider->{subscriber_fees}->[0]->{fees}->[0]->{offpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2);
    my $caller_provider_costs = $provider->{provider_fee}->{fees}->[0]->{offpeak_init_rate} * 60 +
                       $provider->{provider_fee}->{fees}->[0]->{onpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2 + 0.99) +
                        $provider->{provider_fee}->{fees}->[0]->{offpeak_follow_rate} * 60 * int(($call_minutes - 1) / 2);
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
    while (defined $cdr && ok(Utils::Rateomat::run_rateomat(),'rate-o-mat executed')) {
        $cdr = Utils::Rateomat::get_cdrs($cdr->{id});
        $cdr_id_map{$cdr->{id}} = $cdr;
        Utils::Rateomat::check_cdr('cdr was processed: ',$cdr->{id},{ rating_status => 'ok' });
        Utils::Rateomat::check_cdr('cdr is fragmented: ',$cdr->{id},{ is_fragmented => '1' });
        Utils::Rateomat::check_cdr('cdr is reseller onpeak: ',$cdr->{id},{ frag_reseller_onpeak => "$onpeak" });
        Utils::Rateomat::check_cdr('cdr is customer onpeak: ',$cdr->{id},{ frag_customer_onpeak => "$onpeak" });
        Utils::Rateomat::check_cdr('cdr duration: ',$cdr->{id},{ duration => '60.000' });
        my @split_cdrs = grep { !exists $cdr_id_map{$_->{id}}; } @{ Utils::Rateomat::get_cdrs_by_call_id($cdr->{call_id}) };
        if ((scalar @split_cdrs) > 0) {
            is(scalar @split_cdrs,1,'exactly one new split cdr');
            $cdr = $split_cdrs[0];
            Utils::Rateomat::check_cdr('split cdr is unrated: ',$cdr->{id},{ rating_status => 'unrated' });
            Utils::Rateomat::check_cdr('split cdr is fragmented: ',$cdr->{id},{ is_fragmented => '1' });
            Utils::Rateomat::check_cdr('split cdr duration: ',$cdr->{id},{ duration => ($call_minutes-$i)*60 . '.000' });
            $i++;
            $onpeak = ($onpeak ? 0 : 1);
        } else {
            undef $cdr;
        }
    }
    is(scalar keys %cdr_id_map,$call_minutes,"call was split into $call_minutes cdrs");
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
    ok($caller_costs_sum == $caller_costs,'caller costs is ' . $caller_costs);
    ok($caller_provider_costs_sum == $caller_provider_costs,'caller provider costs is ' . $caller_provider_costs);

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
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{
                @peaktimes,
                fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 30,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 10,
                    onpeak_follow_interval  => 60,
                    offpeak_init_rate        => 3,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => 1,
                    offpeak_follow_interval  => 60,
                },
			]},
		],
		[ #billing networks:
		],
        #provider rate
        {
                @peaktimes,
                fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 40,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 20,
                    onpeak_follow_interval  => 60,
                    offpeak_init_rate        => 4,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => 2,
                    offpeak_follow_interval  => 60,
                },
			]}
	);
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
