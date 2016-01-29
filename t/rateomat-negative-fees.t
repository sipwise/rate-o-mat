
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

use Text::Table;
use Text::Wrap;
use Storable;
use DateTime::Format::Strptime;
my $tb = Text::Table->new("request", "response");
*Utils::Api::log_request = sub {
    my ($req,$res) = @_;
    if ($tb) {
        my $dtf = DateTime::Format::Strptime->new(
            pattern => '%F %T',
        );
        #$tb->add(wrap('',"\t",$tb_cnt . ".\t" . $label . ":"),'');
        my $http_cmd = $req->method . " " . $req->uri;
        $http_cmd =~ s/\?/?\n/;
        $tb->add($http_cmd,' ... at ' . $dtf->format_datetime(Utils::Api::get_now()));
        $tb->add("Request","Response");
        my $req_data;
        eval {
            $req_data = JSON::from_json($req->decoded_content);
        };
        my $res_data;
        eval {
            $res_data = JSON::from_json($res->decoded_content);
        };
        if ($res_data) {
            $res_data = Storable::dclone($res_data);
            delete $res_data->{"_links"};
            $tb->add($req_data ? Utils::Api::to_pretty_json($req_data) : '', Utils::Api::to_pretty_json($res_data));
        } else {
            $tb->add($req_data ? Utils::Api::to_pretty_json($req_data) : '', '');
        }
        #$tb_cnt++;
    };
};

{
    my $provider = create_provider();
    my $profile = $provider->{subscriber_fees}->[0]->{profile};
    my $balance = 5;
    my $caller = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '1<n>', sn => '<t>' });
    my $callee = Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => '2<n>', sn => '<t>' });
    my $caller_costs = ($provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_init_interval} +
    $provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_rate} *
	$provider->{subscriber_fees}->[0]->{fees}->[0]->{onpeak_follow_interval})/100.0;
    my $callee_costs = ($provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_init_rate} *
	$provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_init_interval} +
    $provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_follow_rate} *
	$provider->{subscriber_fees}->[0]->{fees}->[1]->{onpeak_follow_interval})/100.0; #negative!

    my $now = Utils::Api::get_now();
	my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
		Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$now->epoch,61),
	]) };

	if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat_threads(),'rate-o-mat executed')) {
		ok(Utils::Rateomat::check_cdrs('',
			map { $_ => { id => $_, rating_status => 'ok', }; } @cdr_ids
		),'cdrs were all processed');
		Utils::Api::check_interval_history('negative fees - caller',$caller->{customer}->{id},[
			{ cash => $balance - $caller_costs,
			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
		]);
		Utils::Api::check_interval_history('negative fees - callee',$callee->{customer}->{id},[
			{ cash => $balance - $callee_costs,
			  profile => $provider->{subscriber_fees}->[0]->{profile}->{id} },
		]);
	}

}

print $tb->stringify;

done_testing();
exit;

sub create_provider {
	return Utils::Api::setup_provider('test<n>.com',
		[ #rates:
			{ fees => [
                { #regular:
                    direction => 'out',
                    destination => '.',
                    onpeak_init_rate        => 2,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => 1,
                    onpeak_follow_interval  => 30,
                    offpeak_init_rate        => 2,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => 1,
                    offpeak_follow_interval  => 30,
                },
                { #negative:
                    direction => 'in',
                    destination => '.',
                    onpeak_init_rate        => -1*2,
                    onpeak_init_interval    => 60,
                    onpeak_follow_rate      => -1*1,
                    onpeak_follow_interval  => 30,
                    offpeak_init_rate        => -1*2,
                    offpeak_init_interval    => 60,
                    offpeak_follow_rate      => -1*1,
                    offpeak_follow_interval  => 30,
                },
			]},
		],
		[ #billing networks:
		]
	);
}
