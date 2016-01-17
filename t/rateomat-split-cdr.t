
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

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
    my $date = Utils::Api::get_now()->ymd();
    my $t = Utils::Api::datetime_from_string($date . ' 07:30:00');
    Utils::Api::set_time($t);
    my $provider = create_provider($date);
    my $profile = $provider->{profiles}->[0]->{profile};
    my $balance = 5;
    my $caller = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $callee = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
    #my $caller_costs = ($provider->{profiles}->[0]->{fees}->[0]->{onpeak_init_rate} *
	#$provider->{profiles}->[0]->{fees}->[0]->{onpeak_init_interval} +
    #$provider->{profiles}->[0]->{fees}->[0]->{onpeak_follow_rate} *
	#$provider->{profiles}->[0]->{fees}->[0]->{onpeak_follow_interval})/100.0;
    #my $callee_costs = ($provider->{profiles}->[0]->{fees}->[1]->{onpeak_init_rate} *
	#$provider->{profiles}->[0]->{fees}->[1]->{onpeak_init_interval} +
    #$provider->{profiles}->[0]->{fees}->[1]->{onpeak_follow_rate} *
	#$provider->{profiles}->[0]->{fees}->[1]->{onpeak_follow_interval})/100.0; #negative!


    $t = Utils::Api::datetime_from_string($date . ' 07:59:50');
    Utils::Api::set_time();
	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$t->epoch,5*60),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
		),'cdrs were all processed');
		#todo...
	}

}

#print $tb->stringify;

done_testing();
exit;

sub create_provider {
    my $peaktime_special_date = shift;
    my @peaktimes = (
                peaktime_weekdays => [ map {
                        ({ weekday => $_,
                           stop => '07:59:59',
                         },
                         { weekday => $_,
                           start => '16:59:59',
                         }); } (0..6)
                    ],
                peaktime_special => [
                        { start => $peaktime_special_date . ' 08:01:00',
                          stop => $peaktime_special_date . ' 08:01:59',
                        },
                        { start => $peaktime_special_date . ' 08:04:00',
                          stop => $peaktime_special_date . ' 08:04:59',
                        },
                    ],
    );
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{
                @peaktimes,
                fees => [
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 20,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 10,
                    onpeak_follow_interval  => 60,
                    offpeak_init_rate        => 2,
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
                    direction => 'in',
                    destination => '.',
                    onpeak_init_rate        => 20,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 10,
                    onpeak_follow_interval  => 60,
                    offpeak_init_rate        => 2,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => 1,
                    offpeak_follow_interval  => 60,
                },
                {
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 20,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 10,
                    onpeak_follow_interval  => 60,
                    offpeak_init_rate        => 2,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => 1,
                    offpeak_follow_interval  => 60,
                },
			]}
	);
}
