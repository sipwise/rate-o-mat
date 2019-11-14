package Utils::Api;

use strict;
use warnings;

use LWP::UserAgent qw();
use JSON::PP qw();
use Test::More;
use Time::HiRes qw(); #prevent warning from Time::Warp
use Time::Warp qw();
use DateTime::TimeZone qw();
use DateTime::Format::Strptime qw();
use DateTime::Format::ISO8601 qw();
use Data::Rmap qw();
use Data::Dumper;

use Utils::Env qw();

my $uri = $ENV{CATALYST_SERVER} // 'https://127.0.0.1:443';
my $user = $ENV{API_USER} // 'administrator';
my $pass = $ENV{API_PASS} // 'administrator';

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
	create_customercontact
	create_systemcontact
	create_reseller
	create_contract
	create_domain
	create_billing_profile
	create_billing_network
	create_billing_zone
	create_billing_fee
	create_package
	create_customer
	create_subscriber
	update_item
	set_cash_balance
	get_cash_balance
	perform_topup
	is_infinite_future
	get_subscriber_preferences
	set_time
	datetime_to_string
	datetime_from_string
	get_now
	current_unix
	check_interval_history
	get_interval_history
	setup_provider
	setup_subscriber
	setup_package
	to_pretty_json
	cartesian_product
	is_float_approx
);

my ($netloc) = ($uri =~ m!^https?://(.*)/?.*$!);

my ($ua, $req, $res);
$ua = LWP::UserAgent->new;
$ua->ssl_opts(
		verify_hostname => 0,
		SSL_verify_mode => 0,
	);
$ua->credentials($netloc, "api_admin_http", $user, $pass);

#my $default_reseller_id = 1;
my $t = time;
my $is_fake_time = 0;
my %entity_maps = ();
my %resource_map = ();

#my $dtf = DateTime::Format::Strptime->new(
#    pattern => '%F %T',
#);

sub create_customercontact {
	return _create_item('customercontacts',
		firstname => "cust_contact_<n>_first",
		lastname  => "cust_contact_<n>_last",
		email     => "cust_contact<n>\@custcontact.invalid",
		#reseller_id => $default_reseller_id,
		@_,
	);
}

sub create_systemcontact {
	return _create_item('systemcontacts',
		firstname => "syst_contact_<n>_first",
		lastname  => "syst_contact_<n>_last",
		email     => "syst_contact<n>\@custcontact.invalid",
		@_,
	);
}

sub create_reseller {
	return _create_item('resellers',
		name => "test <t> <n>",
		status => "active",
		@_,
	);
}

sub create_domain {
	return _create_item('domains',
		domain => 'test_<t>_<n>.example.org',
		#reseller_id => $default_reseller_id,
		@_,
	);
}

sub create_billing_profile {
	return _create_item('billingprofiles',
		name => "test <t> <n>",
		handle  => "test_<t>_<n>",
		#reseller_id => $default_reseller_id,
		@_,
	);
}

sub create_billing_network {
	return _create_item('billingnetworks',
		name => "test <t> <n>",
		description  => "test <t> <n>",
		#reseller_id => $default_reseller_id,
		@_,
	);
}

sub create_package {
	return _create_item('profilepackages',
		name => "test <t> <n>",
		description  => "test <t> <n>",
		#reseller_id => $default_reseller_id,
		@_,
	);
}

sub create_customer {
	return _create_item('customers',
		status => "active",
		type => "sipaccount",
		max_subscribers => undef,
		external_id => undef,
		@_,
	);
}

sub create_contract {
	return _create_item('contracts',
		status => "active",
		type => "reseller",
		@_,
	);
}

sub create_subscriber {
	return _create_item('subscribers',
		username => 'subscriber_<t>_<n>',
		password => 'password',
		@_,
	);
}

sub create_billing_zone {
	return _create_item('billingzones',
		zone => 'test<n>',
		detail => 'test <n>',
		@_,
	);
}

sub create_billing_fee {
	return _create_item('billingfees',
		@_,
	);
}

sub _get_entity_map {
	my $resource = shift;
	if (!exists $entity_maps{$resource}) {
		$entity_maps{$resource} = {};
	}
	return $entity_maps{$resource};
}

sub _ua_request {
	my $req = shift;
	my $res = $ua->request($req);
	log_request($req,$res);
	return $res;
}

sub log_request {
	my ($req,$res) = @_;
}

sub _create_item {
	my ($resource,@params) = @_;
	my $map = _get_entity_map($resource);
	my $n = 1 + scalar keys %$map;
	Data::Rmap::rmap { $_ =~ s/<n>/$n/ if defined $_; $_ =~ s/<i>/$n/ if defined $_; $_ =~ s/<t>/$t/ if defined $_; } @params;
	$req = HTTP::Request->new('POST', $uri.'/api/'.$resource.'/');
	$req->header('Content-Type' => 'application/json');
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$req->content(_to_json({
		@params
	}));
	$res = _ua_request($req);
	if (is($res->code, 201, "create $resource $n")) {
		$req = HTTP::Request->new('GET', $uri.$res->header('Location'));
		$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
		$res = _ua_request($req);
		my $entity = _from_json($res->decoded_content);
		$map->{$entity->{id}} = $entity;
		$resource_map{$entity->{_links}->{self}->{href}} = $resource;
		return $entity;
	} else {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}
	return;
}

sub update_item {
	my ($entity,%params) = @_;
	my $self_href = $entity->{_links}->{self}->{href};
	my $resource = $resource_map{$self_href};
	my $map = _get_entity_map($resource);
	Data::Rmap::rmap { $_ =~ s/<t>/$t/ if defined $_; } %params;
	$req = HTTP::Request->new('PATCH', $uri.$self_href);
	$req->header('Prefer' => 'return=representation');
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$req->header('Content-Type' => 'application/json-patch+json');
	$req->content(_to_json(
		[ map { { op => 'replace', path => '/'.$_ , value => $params{$_} }; } keys %params ]
	));
	$res = _ua_request($req);
	if (is($res->code, 200, "patch $resource id ".$entity->{id})) {
		$entity = _from_json($res->decoded_content);
		$map->{$entity->{id}} = $entity;
		return $entity;
	} else {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}
	return $entity;
}

sub set_time {
	my ($o) = @_;
	if (defined $o) {
		_set_fake_time($o);
		my $now = _current_local();
		diag("applying fake time offset '$o' - current time: " . datetime_to_string($now));
	} else {
		_set_fake_time();
		my $now = _current_local();
		diag("resetting fake time - current time: " . datetime_to_string($now));
	}
}

sub _get_fake_clienttime_now {
	return datetime_to_string(_current_local());
}

sub get_now {
	return _current_local();
}

sub current_unix {
	if ($is_fake_time) {
		return Time::Warp::time;
	} else {
		time;
	}
}

sub _current_local {
	if ($is_fake_time) {
		return DateTime->from_epoch(epoch => Time::Warp::time,
			time_zone => DateTime::TimeZone->new(name => 'local' )
		);
	} else {
		return DateTime->now(
			time_zone => DateTime::TimeZone->new(name => 'local' )
		);
	}
}

sub infinite_future {
	#... to '9999-12-31 23:59:59'
	return DateTime->new(year => 9999, month => 12, day => 31, hour => 23, minute => 59, second => 59,
		#applying the 'local' timezone takes too long -> "The current implementation of DateTime::TimeZone
		#will use a huge amount of memory calculating all the DST changes from now until the future date.
		#Use UTC or the floating time zone and you will be safe."
		time_zone => DateTime::TimeZone->new(name => 'UTC')
		#- with floating timezones, the long conversion takes place when comparing with a 'local' dt
		#- the error due to leap years/seconds is not relevant in comparisons
	);
}

sub is_infinite_future {
	my $dt = shift;
	return $dt->year >= 9999;
}

sub datetime_to_string {
	my ($dt) = @_;
	return unless defined ($dt);
	my $s = $dt->ymd('-') . ' ' . $dt->hms(':');
	$s .= '.'.$dt->millisecond if $dt->millisecond > 0.0;
	return $s;
}

sub datetime_from_string {
	my $s = shift;
	$s =~ s/^(\d{4}\-\d{2}\-\d{2})\s+(\d.+)$/$1T$2/;
	my $ts = DateTime::Format::ISO8601->parse_datetime($s);
	$ts->set_time_zone( DateTime::TimeZone->new(name => 'local') );
	return $ts;
}

sub _set_fake_time {
	my ($o) = @_;
	$is_fake_time = 1;
	if (defined $o) {
		if (ref $o eq 'DateTime') {
			$o = $o->epoch;
		} else {
			my %mult = (
				s => 1,
				m => 60,
				h => 60*60,
				d => 60*60*24,
				M => 60*60*24*30,
				y => 60*60*24*365,
			);

			if (!$o) {
				$o = time;
			} elsif ($o =~ m/^([+-]\d+)([smhdMy]?)$/) {
				$o = time + $1 * $mult{ $2 || "s" };
			} elsif ($o !~ m/\D/) {

			} else {
				die("Invalid time offset: '$o'");
			}
		}
		Time::Warp::to($o);
	} else {
		Time::Warp::reset();
		$is_fake_time = 0;
	}
}

sub set_cash_balance {

	my ($customer,$new_cash_balance) = @_;
	$req = HTTP::Request->new('PATCH', $uri.'/api/customerbalances/' . $customer->{id});
	$req->header('Prefer' => 'return=representation');
	$req->header('Content-Type' => 'application/json-patch+json');
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$req->content(_to_json(
		[ { op => 'replace', path => '/cash_balance', value => $new_cash_balance } ]
	));
	$res = _ua_request($req);
	if (!is($res->code, 200, "setting customer id " . $customer->{id} . " cash_balance to " . $new_cash_balance * 100.0 . ' cents')) {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}

}

sub get_cash_balance {
	my ($customer) = @_;
	$req = HTTP::Request->new('GET', $uri.'/api/customerbalances/' . $customer->{id});
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$res = _ua_request($req);
	if (is($res->code, 200, "fetch customer id " . $customer->{id} . " customerbalance")) {
		return _from_json($res->decoded_content);
	} else {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}
}

sub get_subscriber_preferences {
	my ($subscriber) = @_;
	$req = HTTP::Request->new('GET', $uri.'/api/subscriberpreferences/'.$subscriber->{id});
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$res = _ua_request($req);
	if (is($res->code, 200, "fetch subscriber id " . $subscriber->{id} . " preferences")) {
		return _from_json($res->decoded_content);
	} else {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}
}

sub get_interval_history {
	my ($label,$customer_id) = @_;
	my $intervals = [];
	check_interval_history($label,$customer_id,undef,undef,undef,undef,0,$intervals);
	return $intervals;
}

sub check_interval_history {

	my ($label,$customer_id,$expected_interval_history,$limit_dt,$page,$rows,$first_only,$r_intervals) = @_;
	my $total_count = (defined $expected_interval_history ? (scalar @$expected_interval_history) : undef);
	my $i = 0;
	my $limit = '';
	my $ok = 1;
	$page //= 1;
	$rows //= 10;
	my @intervals;
	$limit = '&start=' . DateTime::Format::ISO8601->parse_datetime($limit_dt) if defined $limit_dt;
	my $nexturi = $uri.'/api/balanceintervals/'.$customer_id.'/?page='.$page.'&rows='.$rows.'&order_by_direction=asc&order_by=start'.$limit;
	do {
		$req = HTTP::Request->new('GET',$nexturi);
		$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
		$res = _ua_request($req);
		is($res->code, 200, $label . "fetch customer id " . $customer_id . " balance intervals collection page");
		my $collection;
		eval {
			$collection = _from_json($res->decoded_content);
		};
		if ($@) {
			print $@;
		}

		if (!$first_only && defined $total_count) {
			$ok = ok($collection->{total_count} == $total_count, $label . "check 'total_count' of collection") && $ok;
		}

		if($collection->{_links}->{next}->{href}) {
			$nexturi = $uri . $collection->{_links}->{next}->{href};
		} else {
			$nexturi = undef;
		}

		my $page_items = {};

		foreach my $interval (@{ $collection->{_embedded}->{'ngcp:balanceintervals'} }) {
			push(@$r_intervals,$interval) if defined $r_intervals;
			$ok = _compare_interval($interval,$expected_interval_history->[$i],$label) && $ok;
			delete $interval->{'_links'};
			push(@intervals,$interval);
			$i++
		}

	} while($nexturi && !$first_only);

	ok($i == $total_count,$label . "check if all expected items are listed") if defined $total_count;
	diag(Dumper({result_intervals => \@intervals})) if !$ok;
	return $ok;
}

sub _compare_interval {
	my ($got,$expected,$label) = @_;

	my $ok = 1;
	if ($expected->{start}) {
		if (substr($expected->{start},0,1) eq '~') {
			$ok = _is_ts_approx($got->{start},$expected->{start},$label . "check interval " . $got->{id} . " start timestamp") && $ok;
		} else {
			$ok = is($got->{start},$expected->{start},$label . "check interval " . $got->{id} . " start timestamp timestamp $got->{start} = $expected->{start}") && $ok;
		}
	}
	if ($expected->{stop}) {
		if (substr($expected->{stop},0,1) eq '~') {
			$ok = _is_ts_approx($got->{stop},substr($expected->{stop},1),$label . "check interval " . $got->{id} . " stop timestamp") && $ok;
		} else {
			$ok = is($got->{stop},$expected->{stop},$label . "check interval " . $got->{id} . " stop timestamp $got->{stop} = $expected->{stop}") && $ok;
		}
	}

	if (defined $expected->{cash}) {
		if (substr($expected->{cash},0,1) eq '~') {
			$ok = is_float_approx($got->{cash_balance},substr($expected->{cash},1),$label . "check interval " . $got->{id} . " cash balance") && $ok;
		} else {
			$ok = is($got->{cash_balance},$expected->{cash},$label . "check interval " . $got->{id} . " cash balance $got->{cash_balance} = $expected->{cash}") && $ok;
		}
	}

	if (defined $expected->{debit}) {
		if (substr($expected->{debit},0,1) eq '~') {
			$ok = is_float_approx($got->{cash_debit},substr($expected->{debit},1),$label . "check interval " . $got->{id} . " cash balance interval") && $ok;
		} else {
			$ok = is($got->{cash_debit},$expected->{debit},$label . "check interval " . $got->{id} . " cash balance interval $got->{cash_debit} = $expected->{debit}") && $ok;
		}
	}

	if ($expected->{profile}) {
		$ok = is($got->{billing_profile_id},$expected->{profile},$label . "check interval " . $got->{id} . " billing profile id $got->{billing_profile_id} = $expected->{profile}") && $ok;
	}

	if (defined $expected->{topups}) {
		$ok = is($got->{topup_count},$expected->{topups},$label . "check interval " . $got->{id} . " topup count $got->{topup_count} = $expected->{topups}") && $ok;
	}

	if (defined $expected->{timely_topups}) {
		$ok = is($got->{timely_topup_count},$expected->{timely_topups},$label . "check interval " . $got->{id} . " timely topup count $got->{timely_topup_count} = $expected->{timely_topups}") && $ok;
	}

	if (defined $expected->{id}) {
		$ok = is($got->{id},$expected->{id},$label . "check interval " . $got->{id} . " id = $expected->{id}") && $ok;
	}

	return $ok;

}

sub _is_ts_approx {
	my ($got,$expected,$label) = @_;
	$got = datetime_from_string($got);
	$expected = datetime_from_string(substr($expected,1));
	my $epsilon = 10;
	my $lower = $expected->clone->subtract(seconds => $epsilon);
	my $upper = $expected->clone->add(seconds => $epsilon);
	return ok($got >= $lower && $got <= $upper,$label . ' ' . datetime_to_string($expected) . ' ~ ' . datetime_to_string($got));
}

sub is_float_approx {
	my ($got,$expected,$label) = @_;
	my $epsilon = 1e-6;
	my $lower = $expected - $epsilon;
	my $upper = $expected + $epsilon;
	return ok($got >= $lower && $got <= $upper,$label . ' ' . $expected . ' ~ ' . $got);
}

sub perform_topup {

	my ($subscriber,$amount,$package) = @_;
	$req = HTTP::Request->new('POST', $uri.'/api/topupcash/');
	$req->header('Content-Type' => 'application/json');
	$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	my $req_data = {
		amount => $amount * 100.0,
		package_id => ($package ? $package->{id} : undef),
		subscriber_id => $subscriber->{id},
	};
	$req->content(_to_json($req_data));
	$res = _ua_request($req);
	if (!is($res->code, 204, "perform topup with amount " . $amount * 100.0 . " cents, " . ($package ? 'package id ' . $package->{id} : 'no package'))) {
		eval {
			diag(_from_json($res->decoded_content)->{message});
		};
	}

}

sub setup_package {
	my ($provider,$initial_profile_networks,$topup_profile_networks,$underrun_profile_networks,@params) = @_;

	my $package = {
		initial_profiles => $initial_profile_networks,
		topup_profiles => $topup_profile_networks,
		underrun_profiles => $underrun_profile_networks,
	};

	$package->{package} = create_package(
		reseller_id => $provider->{reseller}->{id},
		((scalar @$initial_profile_networks) > 0 ? (initial_profiles => [ map { _get_profile_network_mapping({},$_); } @$initial_profile_networks ]) : ()),
		((scalar @$topup_profile_networks) > 0 ? (topup_profiles => [ map { _get_profile_network_mapping({},$_); } @$topup_profile_networks ]) : ()),
		((scalar @$underrun_profile_networks) > 0 ? (underrun_profiles => [ map { _get_profile_network_mapping({},$_); } @$underrun_profile_networks ]) : ()),
		@params,
	);

	push(@{$provider->{packages}},$package);
	return $package;
}

sub _get_profile_network_mapping {
	my ($mapping,$profile_network) = @_;
	if ('ARRAY' eq ref $profile_network) {
		$mapping->{profile_id} = $profile_network->[0]->{id};
		$mapping->{network_id} = $profile_network->[1]->{id};
	} else {
		$mapping->{profile_id} = $profile_network->{id};
	}
	return $mapping;
}

sub _get_interval_start {
	my ($ctime,$start_mode) = @_;
	if ('create' eq $start_mode) {
		return $ctime->clone->truncate(to => 'day');
	} elsif ('1st' eq $start_mode) {
		return $ctime->clone->truncate(to => 'month');
	} elsif ('topup' eq $start_mode) {
		return $ctime->clone; #->truncate(to => 'day');
	} elsif ('topup_interval' eq $start_mode) {
		return $ctime->clone; #->truncate(to => 'day');
	}
	return;
}

sub _add_interval {
	my ($from,$interval_unit,$interval_value,$align_eom_dt) = @_;
	if ('minute' eq $interval_unit) {
		return $from->clone->add(minutes => $interval_value);
	} elsif ('hour' eq $interval_unit) {
		return $from->clone->add(hours => $interval_value);
	} elsif ('day' eq $interval_unit) {
		return $from->clone->add(days => $interval_value);
	} elsif ('week' eq $interval_unit) {
		return $from->clone->add(weeks => $interval_value);
	} elsif ('month' eq $interval_unit) {
		my $to = $from->clone->add(months => $interval_value, end_of_month => 'preserve');
		#DateTime's "preserve" mode would get from 30.Jan to 30.Mar, when adding 2 months
		#When adding 1 month two times, we get 28.Mar or 29.Mar, so we adjust:
		if (defined $align_eom_dt
			&& $to->day > $align_eom_dt->day
			&& $from->day == _last_day_of_month($from)) {
			my $delta = _last_day_of_month($align_eom_dt) - $align_eom_dt->day;
			$to->set(day => _last_day_of_month($to) - $delta);
		}
		return $to;
	}
	return;
}

sub _last_day_of_month {
	my $dt = shift;
	return DateTime->last_day_of_month(year => $dt->year, month => $dt->month,
									   time_zone => DateTime::TimeZone->new(name => 'local'))->day;
}

sub setup_subscriber {
	my ($provider,$profile_config,$cash_balance,$primary_number) = @_;
	my $customer = {};
	$customer->{reseller} = $provider->{reseller};
	$customer->{contact} = create_customercontact(
		reseller_id => $provider->{reseller}->{id}
	);
	my $now = Utils::Api::get_now();
	my $first_interval_start;
	my $first_interval_stop;
	my $first_interval_over;
	my %customer_data = ( contact_id => $customer->{contact}->{id} );
	my $initial_profile_id = undef;
	if ('HASH' eq ref $profile_config) {
		my $self_href = $profile_config->{_links}->{self}->{href};
		my $resource = $resource_map{$self_href};
		if ('billingprofiles' eq $resource) {
			$customer_data{billing_profile_definition} = 'id';
			$customer_data{billing_profile_id} = $profile_config->{id};
			$initial_profile_id = $profile_config->{id};
			$first_interval_start = _get_interval_start($now,'1st');
			$first_interval_stop = _add_interval($first_interval_start,'month',1)->subtract(seconds => 1);
			$first_interval_start = datetime_to_string($first_interval_start);
			$first_interval_stop = datetime_to_string($first_interval_stop);
			$first_interval_over = 0;
		} elsif ('profilepackages' eq $resource) {
			$customer_data{billing_profile_definition} = 'package';
			$customer_data{profile_package_id} = $profile_config->{id};
			$initial_profile_id = $profile_config->{initial_profiles}->[0]->{profile_id};
			$first_interval_start = _get_interval_start($now,$profile_config->{balance_interval_start_mode});
			if ('topup' eq $profile_config->{balance_interval_start_mode}
				|| 'topup_interval' eq $profile_config->{balance_interval_start_mode}) {
				$first_interval_start = '~'.datetime_to_string($first_interval_start);
				$first_interval_stop = datetime_to_string(infinite_future());
			} else {
				$first_interval_stop = _add_interval($first_interval_start,$profile_config->{balance_interval_unit},$profile_config->{balance_interval_value},$now)->subtract(seconds => 1);
				$first_interval_start = datetime_to_string($first_interval_start);
				$first_interval_over = $first_interval_stop < $now;
				$first_interval_stop = datetime_to_string($first_interval_stop);
			}
		}
		$customer->{customer} = create_customer(%customer_data);
	} elsif ('ARRAY' eq ref $profile_config) {
		my @profile_networks = ();
		my $first = 1;
		my $dt = 5;
		my $profile_start = $now->clone->add(seconds => $dt);
		foreach my $profile_network (@$profile_config) {
			my %mapping = (!$first ? ( start => datetime_to_string($profile_start) ) : ());
			_get_profile_network_mapping(\%mapping,$profile_network);
			$initial_profile_id = $mapping{profile_id} if $first;
			push(@profile_networks,\%mapping);
			$first = 0;
		}
		$customer_data{billing_profile_definition} = 'profiles';
		$customer_data{billing_profiles} = \@profile_networks;
		$first_interval_start = _get_interval_start($now,'1st');
		$first_interval_stop = _add_interval($first_interval_start,'month',1)->subtract(seconds => 1);
		$first_interval_start = datetime_to_string($first_interval_start);
		$first_interval_stop = datetime_to_string($first_interval_stop);
		$first_interval_over = 0;
		$customer->{customer} = create_customer(%customer_data);
		sleep($dt) if (scalar @profile_networks) > 0; #wait so profiles become active
	}
	if ($customer->{customer}) {
		set_cash_balance($customer->{customer},$cash_balance) if defined $cash_balance;
		$customer->{subscriber} = create_subscriber(
			customer_id => $customer->{customer}->{id},
			domain_id => $provider->{domain}->{id},
			username => $primary_number->{cc} . $primary_number->{ac} . $primary_number->{sn},
			primary_number => $primary_number,
		) if defined $primary_number;
		my @intervals = ();
		check_interval_history('very first balance interval: ',$customer->{customer}->{id},[
			{ start => $first_interval_start, stop => $first_interval_stop, (!$first_interval_over && defined $cash_balance ? (cash => $cash_balance) : ()), profile => $initial_profile_id },
		],undef,1,1,1,\@intervals);
		$customer->{first_interval} = $intervals[0];
	}
	push(@{$provider->{customers}},$customer);
	return $customer;
}

sub setup_provider {
	my ($domain_name,$rates,$networks,$provider_rate,$type) = @_;
	my $provider = {};
	$provider->{contact} = create_systemcontact();
	$provider->{contract} = create_contract(
		contact_id => $provider->{contact}->{id},
		billing_profile_id => 1, #default profile id
		type => $type // 'reseller',
	);
	$provider->{reseller} = create_reseller(
		contract_id => $provider->{contract}->{id},
	);
	if (defined $provider_rate) {
		my $profile_fee = {};
		($profile_fee->{profile},
		 $profile_fee->{zone},
		 $profile_fee->{fee},
		 $profile_fee->{fees}) = _setup_fees($provider->{reseller},
			%$provider_rate
		);
		$provider->{profile} = $profile_fee->{profile};
		$provider->{provider_fee} = $profile_fee;
		$provider->{contract} = update_item($provider->{contract},
			billing_profile_id => $provider->{profile}->{id},
		);
	#} else {
	#	ok(!$split_peak_parts,'split_peak_parts disabled');
		#use default billing profile id, which already comes with fees.
		#$provider->{profile} = create_billing_profile(
		#	reseller_id => $provider->{reseller}->{id},
		#);
	}

	$provider->{domain} = create_domain(
		reseller_id => $provider->{reseller}->{id},
		domain => $domain_name.'.<t>',
	);
	$provider->{subscriber_fees} = [];
	foreach my $rate (@$rates) {
		my $profile_fee = {};
		($profile_fee->{profile},
		 $profile_fee->{zone},
		 $profile_fee->{fee},
		 $profile_fee->{fees}) = _setup_fees($provider->{reseller},
			%$rate
		);
		push(@{$provider->{subscriber_fees}},$profile_fee);
	}
	$provider->{networks} = [];
	if (defined $networks) {
		foreach my $network_blocks (@$networks) {
			push(@{$provider->{networks}},create_billing_network(
				blocks => $network_blocks,
			));
		}
	}
	$provider->{customers} = [];
	$provider->{packages} = [];
	return $provider;
}

sub _setup_fees {
	my ($reseller,%params) = @_;
	my $prepaid = delete $params{prepaid};
	my $peaktime_weekdays = delete $params{peaktime_weekdays};
	my $peaktime_specials = delete $params{peaktime_special};
	my $interval_free_time = delete $params{interval_free_time};
	my $interval_free_cash = delete $params{interval_free_cash};
	my @fraud_params = map { $_ => delete $params{$_} } grep { rindex($_,'fraud') == 0 ; } keys %params;
	my $profile = create_billing_profile(
		reseller_id => $reseller->{id},
		(defined $prepaid ? (prepaid => $prepaid) : ()),
		(defined $peaktime_weekdays ? (peaktime_weekdays => $peaktime_weekdays) : ()),
		(defined $peaktime_specials ? (peaktime_special => $peaktime_specials) : ()),
		(defined $interval_free_time ? (interval_free_time => $interval_free_time) : ()),
		(defined $interval_free_cash ? (interval_free_cash => $interval_free_cash) : ()),
		@fraud_params,
	);
	my $zone = create_billing_zone(
		billing_profile_id => $profile->{id},
	);
	my @fees = ();
	if (exists $params{fees}) {
		foreach my $fee (@{ $params{fees} }) {
			push(@fees,create_billing_fee(
				billing_profile_id => $profile->{id},
				billing_zone_id => $zone->{id},
				%$fee,
			));
		}
	} else {
		push(@fees,create_billing_fee(
			billing_profile_id => $profile->{id},
			billing_zone_id => $zone->{id},
			direction               => "out",
			destination             => ".",
			%params,
		));
	}
	return ($profile,$zone,$fees[0],\@fees);
}

sub to_pretty_json {
	my $json = JSON::PP->new;
	return $json->pretty->encode(shift);
    #return _to_json(shift, {pretty => 1}); # =~ s/(^\s*{\s*)|(\s*}\s*$)//rg =~ s/\n   /\n/rg;
}

sub _from_json {
    return JSON::PP::decode_json(shift);
}

sub _to_json {
    return JSON::PP::encode_json(shift);
}

sub cartesian_product {

	#Copyright (c) 2009 Philip R Brenan.
	#This module is free software. It may be used, redistributed and/or
	#modified under the same terms as Perl itself.

    my $s = shift;       # Subroutine to call to process each element of the product
    my @C = @_;          # Lists to be multiplied

    my @c = ();          # Current element of cartesian product
    my @P = ();          # Cartesian product
    my $n = 0;           # Number of elements in product

    return 0 if @C == 0; # Empty product

    @C == grep {ref eq 'ARRAY'} @C or die("Arrays of things required by cartesian");

    ## no critic (ClassHierarchies::ProhibitOneArgBless)

    # Generate each cartesian product when there are no prior cartesian products.

    my $p; $p = sub {
        if (@c < @C) {
            for (@{$C[@c]}) {
                push @c, $_;
                &$p();
                pop @c;
            }
        } else {
            my $p = [ @c ];
            push @P, bless $p if &$s(@$p);
        }
    };

    # Generate each cartesian product allowing for prior cartesian products.

    my $q; $q = sub {
        if (@c < @C) {
            for (@{$C[@c]}) {
                push @c, $_;
                &$q();
                pop @c;
            }
        } else {
            my $p = [ map {ref $_ eq __PACKAGE__ ? @$_ : $_} @c ];
            push @P, bless $p if &$s(@$p);
        }
    };

    # Determine optimal method of forming cartesian products for this call

    if (grep { grep {ref $_ eq __PACKAGE__ } @$_ } @C) {
        &$q();
    } else {
        &$p();
    }

    @P;
}

1;
