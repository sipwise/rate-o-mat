
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

#$ENV{CATALYST_SERVER} = https://127.0.0.1:4443
#$ENV{RATEOMAT_PL} = /home/rkrenn/sipwise/git/rate-o-mat/rate-o-mat.pl

#$ENV{CATALYST_SERVER}
#$ENV{API_USER}
#$ENV{API_USER}
#$ENV{RATEOMAT_PROVISIONING_DB_HOST}
#$ENV{RATEOMAT_PROVISIONING_DB_PORT}
#$ENV{RATEOMAT_PROVISIONING_DB_USER}
#$ENV{RATEOMAT_PROVISIONING_DB_PASS}
#$ENV{RATEOMAT_BILLING_DB_HOST}
#$ENV{RATEOMAT_BILLING_DB_PORT}
#$ENV{RATEOMAT_BILLING_DB_USER}
#$ENV{RATEOMAT_BILLING_DB_PASS}
#$ENV{RATEOMAT_ACCOUNTING_DB_HOST}
#$ENV{RATEOMAT_ACCOUNTING_DB_PORT}
#$ENV{RATEOMAT_ACCOUNTING_DB_USER}
#$ENV{RATEOMAT_ACCOUNTING_DB_PASS}
#$ENV{RATEOMAT_PL}

my $provider = setup_provider('test.com',
{ #any
    onpeak_init_rate        => 2,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 1,
    onpeak_follow_interval  => 30,
    offpeak_init_rate        => 2,
    offpeak_init_interval    => 60,
    offpeak_follow_rate      => 1,
    offpeak_follow_interval  => 30,
},
{ #network1
    onpeak_init_rate        => 4,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 2,
    onpeak_follow_interval  => 30,
    offpeak_init_rate        => 4,
    offpeak_init_interval    => 60,
    offpeak_follow_rate      => 2,
    offpeak_follow_interval  => 30,
},
{ #network2
    onpeak_init_rate        => 6,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 3,
    onpeak_follow_interval  => 30,
    offpeak_init_rate        => 6,
    offpeak_init_interval    => 60,
    offpeak_follow_rate      => 3,
    offpeak_follow_interval  => 30,
},
[ #network1
    {ip=>'10.0.0.1',mask=>26}, #0..63
    {ip=>'10.0.0.133',mask=>26}, #128..
],
[ #network2
    {ip=>'10.0.0.99',mask=>26}, #64..127
],
);

my $balance = 5;
my $caller1 = setup_customer($provider,{ cc => 888, ac => '1<n>', sn => '<t>' },$balance);
my $caller2 = setup_customer($provider,{ cc => 888, ac => '1<n>', sn => '<t>' },$balance);
my $caller3 = setup_customer($provider,{ cc => 888, ac => '1<n>', sn => '<t>' },$balance);
my $callee = setup_customer($provider,{ cc => 888, ac => '2<n>', sn => '<t>' });

my @cdr_ids = map { $_->{id}; } @{ Utils::Rateomat::create_cdrs([
    #call from any
    Utils::Rateomat::prepare_cdr($caller1->{subscriber},undef,$caller1->{reseller},
            $callee->{subscriber},undef,$callee->{reseller},
            '192.168.0.1',Utils::Api::current_unix(),1), 
    #call from network1
    Utils::Rateomat::prepare_cdr($caller2->{subscriber},undef,$caller2->{reseller},
            $callee->{subscriber},undef,$callee->{reseller},
            '10.0.0.129',Utils::Api::current_unix(),1),
    #call from network2
    Utils::Rateomat::prepare_cdr($caller3->{subscriber},undef,$caller3->{reseller},
            $callee->{subscriber},undef,$callee->{reseller},
            '10.0.0.97',Utils::Api::current_unix(),1),
]) };

if (ok((scalar @cdr_ids) > 0 && Utils::Rateomat::run_rateomat(),'rate-o-mat executed')) {
    ok(Utils::Rateomat::check_cdrs('',
        $cdr_ids[0] => {
            id => $cdr_ids[0],
            rating_status => 'ok',
        },
        $cdr_ids[1] => {
            id => $cdr_ids[1],
            rating_status => 'ok',
        },
        $cdr_ids[2] => {
            id => $cdr_ids[2],
            rating_status => 'ok',
        },    
    ),'cdrs were all processed');
    Utils::Api::check_interval_history('',$caller1->{customer}->{id},[
        { cash => (100.0 * $balance - $provider->{fee_1}->{onpeak_init_rate} *
                   $provider->{fee_1}->{onpeak_init_interval})/100.0, profile => $provider->{profile_1}->{id} },
    ]);
    Utils::Api::check_interval_history('',$caller2->{customer}->{id},[
        { cash => (100.0 * $balance - $provider->{fee_2}->{onpeak_init_rate} *
                   $provider->{fee_2}->{onpeak_init_interval})/100.0, profile => $provider->{profile_1}->{id} },
    ]);
    Utils::Api::check_interval_history('',$caller3->{customer}->{id},[
        { cash => (100.0 * $balance - $provider->{fee_3}->{onpeak_init_rate} *
                   $provider->{fee_3}->{onpeak_init_interval})/100.0, profile => $provider->{profile_1}->{id} },
    ]);
}

done_testing();
exit;

sub setup_customer {
    my ($provider,$pn,$cash_balance) = @_;
    my $customer = {};
    $customer->{reseller} = $provider->{reseller};
    $customer->{contact} = Utils::Api::create_customercontact(
        reseller_id => $provider->{reseller}->{id}
    );
    my $dt = 5;
    my $now = Utils::Api::get_now();
    my $bom = $now->clone->truncate(to => 'month');
    my $eom = $bom->clone->add(months => 1)->subtract(seconds => 1);
    my $profile_start = $now->clone->add(seconds => $dt);
    $customer->{customer} = Utils::Api::create_customer(
        contact_id => $customer->{contact}->{id},
        billing_profile_definition => 'profiles',
        billing_profiles => [
            {
                profile_id => $provider->{profile_1}->{id},
            },
            {
                profile_id => $provider->{profile_2}->{id},
                network_id => $provider->{network_1}->{id},
                start => Utils::Api::datetime_to_string($profile_start),
            },
            {
                profile_id => $provider->{profile_3}->{id},
                network_id => $provider->{network_2}->{id},
                start => Utils::Api::datetime_to_string($profile_start),
            }],
    );
    sleep($dt); #wait so profiles become active
    $customer->{subscriber} = Utils::Api::create_subscriber(
        customer_id => $customer->{customer}->{id},
        domain_id => $provider->{domain}->{id},
        username => $pn->{cc} . $pn->{ac} . $pn->{sn},
        primary_number => $pn,
    );
    Utils::Api::set_cash_balance($customer->{customer},$cash_balance) if defined $cash_balance;
    Utils::Api::check_interval_history('',$customer->{customer}->{id},[
        { start => Utils::Api::datetime_to_string($bom), stop => Utils::Api::datetime_to_string($eom), cash => (defined $cash_balance ? $cash_balance : 0), profile => $provider->{profile_1}->{id} },
    ]);
    return $customer;
}

sub setup_provider {
    my ($domain_name,$rates_1,$rates_2,$rates_3,$network_1_blocks,$network_2_blocks) = @_;
    my $provider = {};
    $provider->{contact} = Utils::Api::create_systemcontact();    
    $provider->{contract} = Utils::Api::create_contract(
        contact_id => $provider->{contact}->{id},
        billing_profile_id => 1, #default profile id
    );
    $provider->{reseller} = Utils::Api::create_reseller(
        contract_id => $provider->{contract}->{id},
    );
    $provider->{profile} = Utils::Api::create_billing_profile(
        reseller_id => $provider->{reseller}->{id},
    );
    $provider->{contract} = Utils::Api::update_item($provider->{contract},
        billing_profile_id => $provider->{profile}->{id},
    );
    $provider->{domain} = Utils::Api::create_domain(
        reseller_id => $provider->{reseller}->{id},
        domain => $domain_name.'.<t>',
    );
    ($provider->{profile_1},$provider->{fee_1}) = setup_customer_fees($provider->{reseller},
        %$rates_1
    ) if defined $rates_1;
    ($provider->{profile_2},$provider->{fee_2}) = setup_customer_fees($provider->{reseller},
        %$rates_2
    ) if defined $rates_2;
    ($provider->{profile_3},$provider->{fee_3}) = setup_customer_fees($provider->{reseller},
        %$rates_3
    ) if defined $rates_3;   
    $provider->{network_1} = Utils::Api::create_billing_network(
        blocks => $network_1_blocks,
    ) if defined $network_1_blocks;
    $provider->{network_2} = Utils::Api::create_billing_network(
        blocks => $network_2_blocks,
    ) if defined $network_2_blocks;   
    return $provider;
}

sub setup_customer_fees {
    my ($reseller,@params) = @_;
    my $profile = Utils::Api::create_billing_profile(
        reseller_id => $reseller->{id},
    );
    my $zone = Utils::Api::create_billing_zone(
        billing_profile_id => $profile->{id},
    );    
    my $fee = Utils::Api::create_billing_fee(
        billing_profile_id => $profile->{id},
        billing_zone_id => $zone->{id},
        direction               => "out",
        destination             => ".",
        @params,   
    );
    return ($profile,$fee);
}