
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

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
{
    onpeak_init_rate        => 1,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 1,
    onpeak_follow_interval  => 30,
    offpeak_init_rate       => 0.5,
    offpeak_init_interval   => 60,
    offpeak_follow_rate     => 0.5,
    offpeak_follow_interval => 30, 
},
{
    onpeak_init_rate        => 1,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 1,
    onpeak_follow_interval  => 30,
    offpeak_init_rate       => 0.5,
    offpeak_init_interval   => 60,
    offpeak_follow_rate     => 0.5,
    offpeak_follow_interval => 30, 
},
{
    onpeak_init_rate        => 1,
    onpeak_init_interval    => 60,
    onpeak_follow_rate      => 1,
    onpeak_follow_interval  => 30,
    offpeak_init_rate       => 0.5,
    offpeak_init_interval   => 60,
    offpeak_follow_rate     => 0.5,
    offpeak_follow_interval => 30, 
},
[
    {ip=>'10.0.4.7',mask=>26}, #0..63
    {ip=>'10.0.4.99',mask=>26}, #64..127
    {ip=>'10.0.5.9',mask=>24},
    {ip=>'10.0.6.9',mask=>24},
],
[
    {ip=>'10.0.4.7',mask=>26}, #0..63
    {ip=>'10.0.4.99',mask=>26}, #64..127
    {ip=>'10.0.5.9',mask=>24},
    {ip=>'10.0.6.9',mask=>24},
],
);

my $customer = setup_customer($provider,5);

#set_cash_balance($customer,3);

done_testing();
exit;

sub setup_customer {
    my ($provider,$subscriber_name,$primary_number,$cash_balance) = @_;
    my $customer = {};
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
        $subscriber_name
        primary_number => $primary_number,
    );
    Utils::Api::set_cash_balance($customer->{customer},$cash_balance);
    Utils::Api::check_interval_history('',$customer->{customer}->{id},[
        { start => Utils::Api::datetime_to_string($bom), stop => Utils::Api::datetime_to_string($eom), cash => $cash_balance, profile => $provider->{profile_1}->{id} },
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
    $provider->{profile_1} = setup_customer_fees($provider->{reseller},
        %$rates_1
    ) if defined $rates_1;
    $provider->{profile_2} = setup_customer_fees($provider->{reseller},
        %$rates_2
    ) if defined $rates_2;
    $provider->{profile_3} = setup_customer_fees($provider->{reseller},
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
    return $profile;
}

#my $package = Utils::Api::create_package(
#    initial_profiles => [{ profile_id => $profile->{id}, }, ],
#
#    );
#my $customer = Utils::Api::create_customer(
#    contact_id => $contact->{id},
#    billing_profile_definition => 'package',
#    profile_package_id => $package->{id}
#);
#my $subscriber = Utils::Api::create_subscriber(
#    customer_id => $customer->{id},
#    domain_id => $domain->{id},
#);
#set_cash_balance($customer,3);
#my $preferences = get_subscriber_preferences($subscriber);


my $cdr = Utils::Rateomat::create_cdrs(

);
my $cdrs = Utils::Rateomat::create_cdrs([{

}]);

Utils::Rateomat::get_cdrs($cdr->{id});
Utils::Rateomat::get_cdrs([ $cdr->{id} ]);

Utils::Rateomat::run_rateomat();