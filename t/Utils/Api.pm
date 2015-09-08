package Utils::Api;

use strict;
use LWP::UserAgent qw();
use JSON qw();
use Test::More;
use Time::HiRes qw(); #prevent warning from Time::Warp
use Time::Warp qw();
use DateTime::TimeZone qw();
use DateTime::Format::Strptime qw();
use DateTime::Format::ISO8601 qw();
use Data::Rmap qw();
use Data::Dumper;

my $uri = $ENV{CATALYST_SERVER} // 'https://127.0.0.1:443';
my $user = $ENV{API_USER} // 'administrator';
my $pass = $ENV{API_USER} // 'administrator';

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
    get_subscriber_preferences
    set_time
    datetime_to_string
    datetime_from_string
    get_now
    current_unix
    check_interval_history
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

sub _create_item {
    my ($resource,@params) = @_;
    my $map = _get_entity_map($resource);
    my $n = 1 + scalar keys %$map;
    Data::Rmap::rmap { $_ =~ s/<n>/$n/; $_ =~ s/<i>/$n/; $_ =~ s/<t>/$t/; } @params;
    $req = HTTP::Request->new('POST', $uri.'/api/'.$resource.'/');
    $req->header('Content-Type' => 'application/json');
    $req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
    $req->content(JSON::to_json({
        @params
    }));
    $res = $ua->request($req);
    is($res->code, 201, "create $resource $n");
    $req = HTTP::Request->new('GET', $uri.$res->header('Location'));
    $res = $ua->request($req);
    if (is($res->code, 200, "fetch $resource $n")) {
        my $entity = JSON::from_json($res->decoded_content);
        $map->{$entity->{id}} = $entity;
        $resource_map{$entity->{_links}->{self}->{href}} = $resource;
        return $entity;
    } else {
        eval {
            diag(JSON::from_json($res->decoded_content)->{message});
        };
    }
    return undef;
}

sub update_item {
    my ($entity,%params) = @_;
    my $self_href = $entity->{_links}->{self}->{href};
    my $resource = $resource_map{$self_href};
    my $map = _get_entity_map($resource);
    Data::Rmap::rmap { $_ =~ s/<t>/$t/; } %params;    
    $req = HTTP::Request->new('PATCH', $uri.$self_href);
    $req->header('Prefer' => 'return=representation');
    $req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
    $req->header('Content-Type' => 'application/json-patch+json');
    $req->content(JSON::to_json(
        [ map { { op => 'replace', path => '/'.$_ , value => $params{$_} }; } keys %params ]
    ));
    $res = $ua->request($req);
    if (is($res->code, 200, "patch $resource id ".$entity->{id})) {
        $entity = JSON::from_json($res->decoded_content);
        $map->{$entity->{id}} = $entity;
        return $entity;
    } else {
        eval {
            diag(JSON::from_json($res->decoded_content)->{message});
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
            time_zone => DateTime::TimeZone->new(name => 'local')
        );
    } else {
        return DateTime->now(
            time_zone => DateTime::TimeZone->new(name => 'local')
        );
    }
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
    $req->content(JSON::to_json(
        [ { op => 'replace', path => '/cash_balance', value => $new_cash_balance } ]
    ));
    $res = $ua->request($req);
    if (!is($res->code, 200, "setting customer id " . $customer->{id} . " cash_balance to " . $new_cash_balance * 100.0 . ' cents')) {
        eval {
            diag(JSON::from_json($res->decoded_content)->{message});
        };        
    }
    
}

sub get_subscriber_preferences {
    my ($subscriber) = @_;
    $req = HTTP::Request->new('GET', $uri.'/api/subscriberpreferences/'.$subscriber->{id});
    $req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
    $res = $ua->request($req);
    if (is($res->code, 200, "fetch subscriber id " . $subscriber->{id} . " preferences")) {
        return JSON::from_json($res->decoded_content);
    } else {
        eval {
            diag(JSON::from_json($res->decoded_content)->{message});
        };        
    }
}

sub check_interval_history {
    
    my ($label,$customer_id,$expected_interval_history,$limit_dt) = @_;
    my $total_count = (scalar @$expected_interval_history);
    my $i = 0;
    my $limit = '';
    my $ok = 1;
    my @intervals;
    $limit = '&start=' . DateTime::Format::ISO8601->parse_datetime($limit_dt) if defined $limit_dt;
    my $nexturi = $uri.'/api/balanceintervals/'.$customer_id.'/?page=1&rows=10&order_by_direction=asc&order_by=start'.$limit;
    do {
        $req = HTTP::Request->new('GET',$nexturi);
        $req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
        $res = $ua->request($req);        
        is($res->code, 200, $label . "fetch customer id " . $customer_id . " balance intervals collection page");
        my $collection = JSON::from_json($res->decoded_content);

        $ok = ok($collection->{total_count} == $total_count, $label . "check 'total_count' of collection") && $ok;

        if($collection->{_links}->{next}->{href}) {
            $nexturi = $uri . $collection->{_links}->{next}->{href};
        } else {
            $nexturi = undef;
        }
        
        my $page_items = {};

        foreach my $interval (@{ $collection->{_embedded}->{'ngcp:balanceintervals'} }) {
            $ok = _compare_interval($interval,$expected_interval_history->[$i],$label) && $ok;
            delete $interval->{'_links'};
            push(@intervals,$interval);
            $i++
        }
             
    } while($nexturi);
    
    ok($i == $total_count,$label . "check if all expected items are listed");
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
            $ok = is($got->{start},$expected->{start},$label . "check interval " . $got->{id} . " start timestmp") && $ok;
        }
    }
    if ($expected->{stop}) {
        if (substr($expected->{stop},0,1) eq '~') {
            $ok = _is_ts_approx($got->{stop},$expected->{stop},$label . "check interval " . $got->{id} . " stop timestamp") && $ok;
        } else {
            $ok = is($got->{stop},$expected->{stop},$label . "check interval " . $got->{id} . " stop timestmp") && $ok;
        }
    }
    
    if ($expected->{cash}) {
        $ok = is($got->{cash_balance},$expected->{cash},$label . "check interval " . $got->{id} . " cash balance") && $ok;
    }

    if ($expected->{profile}) {
        $ok = is($got->{billing_profile_id},$expected->{profile},$label . "check interval " . $got->{id} . " billing profile") && $ok;
    }
    
    if ($expected->{topups}) {
        $ok = is($got->{topup_count},$expected->{topups},$label . "check interval " . $got->{id} . " topup count") && $ok;
    }
    
    if ($expected->{timely_topups}) {
        $ok = is($got->{timely_topup_count},$expected->{timely_topups},$label . "check interval " . $got->{id} . " timely topup count") && $ok;
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
    return ok($got >= $lower && $got <= $upper,$label . ' approximately (' . $got . ')');
}

1;