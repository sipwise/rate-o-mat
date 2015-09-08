#!/usr/bin/perl -w
use lib '/usr/share/ngcp-rate-o-mat';
use strict;
use DBI;
use POSIX qw(setsid mktime);
use Fcntl qw(LOCK_EX LOCK_NB SEEK_SET);
use IO::Handle;
use Sys::Syslog;
use NetAddr::IP;
use Data::Dumper;
use DateTime;

$0 = 'rate-o-mat';
my $fork = $ENV{RATEOMAT_DAEMONIZE} // 1;
my $PID;
my $pidfile = '/var/run/rate-o-mat.pid';
my $type = 'call';
my $loop_interval = $ENV{RATEOMAT_LOOP_INTERVAL} ? int $ENV{RATEOMAT_LOOP_INTERVAL} : 10;
my $debug = $ENV{RATEOMAT_DEBUG} ? int $ENV{RATEOMAT_DEBUG} : 0;

my $log_ident = 'rate-o-mat';
my $log_facility = 'daemon';
my $log_opts = 'ndely,cons,pid,nowait';

# if the LNP database is used not just for LNP, but also for on-net
# billing, special routing or similar things, this should be set to
# better guess the correct LNP provider ID when selecting ported numbers
# e.g.:
# my @lnp_order_by = ("lnp_provider_id ASC");
my @lnp_order_by = ();

# billing database
my $BillDB_Name = $ENV{RATEOMAT_BILLING_DB_NAME} || 'billing';
my $BillDB_Host = $ENV{RATEOMAT_BILLING_DB_HOST} || 'localhost';
my $BillDB_Port = $ENV{RATEOMAT_BILLING_DB_PORT} ? int $ENV{RATEOMAT_BILLING_DB_PORT} : 3306;
my $BillDB_User = $ENV{RATEOMAT_BILLING_DB_USER} || die "Missing billing DB user setting.";
my $BillDB_Pass = $ENV{RATEOMAT_BILLING_DB_PASS}; # || die "Missing billing DB password setting.";
# accounting database
my $AcctDB_Name = $ENV{RATEOMAT_ACCOUNTING_DB_NAME} || 'accounting';
my $AcctDB_Host = $ENV{RATEOMAT_ACCOUNTING_DB_HOST} || 'localhost';
my $AcctDB_Port = $ENV{RATEOMAT_ACCOUNTING_DB_PORT} ? int $ENV{RATEOMAT_ACCOUNTING_DB_PORT} : 3306;
my $AcctDB_User = $ENV{RATEOMAT_ACCOUNTING_DB_USER} || die "Missing accounting DB user setting.";
my $AcctDB_Pass = $ENV{RATEOMAT_ACCOUNTING_DB_PASS}; # || die "Missing accounting DB password setting.";
# provisioning database
my $ProvDB_Name = $ENV{RATEOMAT_PROVISIONING_DB_NAME} || 'provisioning';
my $ProvDB_Host = $ENV{RATEOMAT_PROVISIONING_DB_HOST} || 'localhost';
my $ProvDB_Port = $ENV{RATEOMAT_PROVISIONING_DB_PORT} ? int $ENV{RATEOMAT_PROVISIONING_DB_PORT} : 3306;
my $ProvDB_User = $ENV{RATEOMAT_PROVISIONING_DB_USER} || die "Missing provisioning DB user setting.";
my $ProvDB_Pass = $ENV{RATEOMAT_PROVISIONING_DB_PASS}; # || die "Missing provisioning DB password setting.";
# duplication database
my $DupDB_Name = $ENV{RATEOMAT_DUPLICATE_DB_NAME} || 'accounting';
my $DupDB_Host = $ENV{RATEOMAT_DUPLICATE_DB_HOST} || 'localhost';
my $DupDB_Port = $ENV{RATEOMAT_DUPLICATE_DB_PORT} ? int $ENV{RATEOMAT_DUPLICATE_DB_PORT} : 3306;
my $DupDB_User = $ENV{RATEOMAT_DUPLICATE_DB_USER};
my $DupDB_Pass = $ENV{RATEOMAT_DUPLICATE_DB_PASS};

########################################################################

sub main;

my $shutdown = 0;
my $prepaid_costs;

my $billdbh;
my $acctdbh;
my $provdbh;
my $dupdbh;
my $sth_get_contract_info;
my $sth_get_subscriber_contract_id;
my $sth_billing_info_v4;
my $sth_billing_info_v6;
my $sth_billing_info_panel;
my $sth_profile_info;
my $sth_offpeak_weekdays;
my $sth_offpeak_special;
my $sth_unrated_cdrs;
my $sth_update_cdr;
my $sth_provider_info;
my $sth_reseller_info;
my $sth_get_cbalances;
my $sth_update_cbalance_w_underrun_profiles_lock;
my $sth_update_cbalance_w_underrun_lock;
my $sth_update_cbalance_w_underrun_profiles;
my $sth_update_cbalance;
my $sth_new_cbalance;
my $sth_new_cbalance_infinite_future;
my $sth_get_last_cbalance;
my $sth_get_first_cbalance;
my $sth_get_last_topup_cbalance,
my $sth_lnp_number;
my $sth_lnp_profile_info;
my $sth_prepaid_costs;
my $sth_delete_prepaid_cost;
my $sth_delete_old_prepaid;
my $sth_get_billing_voip_subscribers;
my $sth_get_package_profile_sets;
my $sth_create_billing_mappings;
my $sth_get_provisioning_voip_subscribers;
my $sth_get_usr_preference_attribute;
my $sth_get_usr_preference_value;
my $sth_create_usr_preference_value;
my $sth_update_usr_preference_value;
my $sth_delete_usr_preference_value;
my $sth_duplicate_cdr;

my $connect_interval = 3;

my @cdr_fields = qw(source_user_id source_provider_id source_external_subscriber_id source_external_contract_id source_account_id source_user source_domain source_cli source_clir source_ip destination_user_id destination_provider_id destination_external_subscriber_id destination_external_contract_id destination_account_id destination_user destination_domain destination_user_dialed destination_user_in destination_domain_in peer_auth_user peer_auth_realm call_type call_status call_code init_time start_time duration call_id source_carrier_cost source_reseller_cost source_customer_cost source_carrier_free_time source_reseller_free_time source_customer_free_time source_carrier_billing_fee_id source_reseller_billing_fee_id source_customer_billing_fee_id source_carrier_billing_zone_id source_reseller_billing_zone_id source_customer_billing_zone_id destination_carrier_cost destination_reseller_cost destination_customer_cost destination_carrier_free_time destination_reseller_free_time destination_customer_free_time destination_carrier_billing_fee_id destination_reseller_billing_fee_id destination_customer_billing_fee_id destination_carrier_billing_zone_id destination_reseller_billing_zone_id destination_customer_billing_zone_id frag_carrier_onpeak frag_reseller_onpeak frag_customer_onpeak is_fragmented split rated_at rating_status exported_at export_status);
foreach my $gpp_idx(0 .. 9) {
	push @cdr_fields, ("source_gpp$gpp_idx", "destination_gpp$gpp_idx");
}

main;
exit 0;

########################################################################

sub FATAL
{
	my $msg = shift;
	chomp $msg;
	print "FATAL: $msg\n" if($fork != 1);
	#unless(defined $DBI::err and $DBI::err == 2006)
	#{
	#	# we manually start the transaction and call commit,
	#	# so no need to rollback here
	#	#$billdbh->rollback if defined $billdbh;
	#	#$acctdbh->rollback if defined $acctdbh;
	#}
	syslog('crit', $msg);
	die "$msg\n";
}

sub DEBUG
{
	return unless $debug;
	my $msg = shift;
	chomp $msg;
	$msg =~ s/#012 +/ /g;
	print "DEBUG: $msg\n" if($fork != 1);
	syslog('debug', $msg);
}

sub INFO
{
	my $msg = shift;
	chomp $msg;
	print "INFO: $msg\n" if($fork != 1);
	syslog('info', $msg);
}

sub WARNING
{
	my $msg = shift;
	chomp $msg;
	print "WARNING: $msg\n" if($fork != 1);
	syslog('warning', $msg);
}

sub sql_time {
	my ($time) = @_;

	my ($y, $m, $d, $H, $M, $S) = (localtime($time))[5,4,3,2,1,0];
	$y += 1900;
	$m += 1;
	return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $y, $m, $d, $H, $M, $S);
}

sub set_start_strtime
{
	my $start = shift;
	my $r_str = shift;

	$$r_str = sql_time($start);
	return 0;
}

sub connect_billdbh
{
	do {
		INFO "Trying to connect to billing db...";
		$billdbh = DBI->connect("dbi:mysql:database=$BillDB_Name;host=$BillDB_Host;port=$BillDB_Port", $BillDB_User, $BillDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $billdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($billdbh);
	INFO "Successfully connected to billing db...";
}

sub connect_acctdbh
{
	do {
		INFO "Trying to connect to accounting db...";
		$acctdbh = DBI->connect("dbi:mysql:database=$AcctDB_Name;host=$AcctDB_Host;port=$AcctDB_Port", $AcctDB_User, $AcctDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $acctdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($acctdbh);
	INFO "Successfully connected to accounting db...";
}

sub connect_provdbh
{
	do {
		INFO "Trying to connect to provisioning db...";
		$provdbh = DBI->connect("dbi:mysql:database=$ProvDB_Name;host=$ProvDB_Host;port=$ProvDB_Port", $ProvDB_User, $ProvDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $provdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($provdbh);
	INFO "Successfully connected to provisioning db...";
}

sub connect_dupdbh
{
	$DupDB_User && $DupDB_Pass or return;

	do {
		INFO "Trying to connect to duplication db...";
		$dupdbh = DBI->connect("dbi:mysql:database=$DupDB_Name;host=$DupDB_Host;port=$DupDB_Port", $DupDB_User, $DupDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $dupdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($dupdbh);
	INFO "Successfully connected to duplication db...";
}

sub begin_transaction {
    my ($dbh,$isolation_level) = @_;
    if ($dbh) {
        if ($isolation_level) {
            $dbh->do('SET TRANSACTION ISOLATION LEVEL '.$isolation_level) or FATAL "Error setting transaction isolation level: ".$dbh->errstr;
        }
        $dbh->begin_work or FATAL "Error starting transaction: ".$dbh->errstr;
    }
}

sub commit_transaction {
    my $dbh = shift;
    if ($dbh) {
        #capture result to force list context and prevent good old komodo perl5db.pl bug:
        my @wa = $dbh->commit or FATAL "Error committing: ".$dbh->errstr;
    }
}

sub rollback_transaction {
    my $dbh = shift;
    if ($dbh) {
        my @wa = $dbh->rollback or FATAL "Error rolling back: ".$dbh->errstr;
    }
}

sub _bigint_to_bytes {
	my ($bigint,$size) = @_;
	return pack('C' x $size, map { hex($_) } (sprintf('%0' . 2 * $size . 's',substr($bigint->as_hex(),2)) =~ /(..)/g));
}

sub _is_infinite_unix {
	my $unix_ts = shift;
	return 1 unless defined $unix_ts; #internally, we use undef for infinite future
	return $unix_ts == 0 ? 1 : 0; #If you pass an out-of-range date to UNIX_TIMESTAMP(), it returns 0
}

sub last_day_of_month {
	my $dt = shift;
	return DateTime->last_day_of_month(year => $dt->year, month => $dt->month,
									   time_zone => DateTime::TimeZone->new(name => 'local'))->day;
}

sub init_db
{
	connect_billdbh;
	connect_provdbh;
	connect_acctdbh;
	connect_dupdbh;

	$sth_get_contract_info = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(c.create_timestamp),".
		" UNIX_TIMESTAMP(c.modify_timestamp),".
		" co.reseller_id,".
		" p.id,".
		" p.balance_interval_unit,".
		" p.balance_interval_value,".
		" p.balance_interval_start_mode,".
		" p.carry_over_mode,".
		" p.notopup_discard_intervals,".
		" p.underrun_profile_threshold,".
		" p.underrun_lock_threshold,".
		" p.underrun_lock_level ".
		"FROM billing.contracts c ".
		"LEFT JOIN billing.profile_packages p on c.profile_package_id = p.id ".
		"LEFT JOIN billing.contacts co on c.contact_id = co.id ".
		"WHERE c.id = ?"
	) or FATAL "Error preparing subscriber contract info statement: ".$billdbh->errstr;

	$sth_get_last_cbalance = $billdbh->prepare(
		"SELECT id, UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end), cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, topup_count, timely_topup_count ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? ".
		"ORDER BY end DESC LIMIT 1"
	) or FATAL "Error preparing get last contract balance statement: ".$billdbh->errstr;
	$sth_get_first_cbalance = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(start) ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? ".
		"ORDER BY start ASC LIMIT 1"
	) or FATAL "Error preparing get first contract balance statement: ".$billdbh->errstr;
	$sth_get_last_topup_cbalance = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(start) ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"topup_count > 0 ".
		"ORDER BY end DESC LIMIT 1"
	) or FATAL "Error preparing get last topup contract balance statement: ".$billdbh->errstr;

	$sth_get_subscriber_contract_id = $billdbh->prepare(
		"SELECT contract_id FROM voip_subscribers WHERE uuid = ?"
	) or FATAL "Error preparing subscriber contract id statement: ".$billdbh->errstr;

	$sth_billing_info_v4 = $billdbh->prepare(<<EOS
		SELECT b.billing_profile_id, b.product_id, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		LEFT JOIN billing.billing_networks n ON n.id = b.network_id
		LEFT JOIN billing.billing_network_blocks nb ON n.id = nb.network_id
		WHERE b.contract_id = ?
		AND ( b.start_date IS NULL OR b.start_date <= FROM_UNIXTIME(?) )
		AND ( b.end_date IS NULL OR b.end_date >= FROM_UNIXTIME(?) )
		AND ( (nb._ipv4_net_from <= ? AND nb._ipv4_net_to >= ?) OR b.network_id IS NULL)
		ORDER BY b.network_id DESC, b.start_date DESC, b.id DESC
		LIMIT 1
EOS
	) or FATAL "Error preparing ipv4 billing info statement: ".$billdbh->errstr;

	$sth_billing_info_v6 = $billdbh->prepare(<<EOS
		SELECT b.billing_profile_id, b.product_id, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		LEFT JOIN billing.billing_networks n ON n.id = b.network_id
		LEFT JOIN billing.billing_network_blocks nb ON n.id = nb.network_id
		WHERE b.contract_id = ?
		AND ( b.start_date IS NULL OR b.start_date <= FROM_UNIXTIME(?) )
		AND ( b.end_date IS NULL OR b.end_date >= FROM_UNIXTIME(?) )
		AND ( (nb._ipv6_net_from <= ? AND nb._ipv6_net_to >= ?) OR b.network_id IS NULL)
		ORDER BY b.network_id DESC, b.start_date DESC, b.id DESC
		LIMIT 1
EOS
	) or FATAL "Error preparing ipv6 billing info statement: ".$billdbh->errstr;

	$sth_billing_info_panel = $billdbh->prepare(<<EOS
		SELECT b.billing_profile_id, b.product_id, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		WHERE b.contract_id = ?
		AND ( b.start_date IS NULL OR b.start_date <= FROM_UNIXTIME(?) )
		AND ( b.end_date IS NULL OR b.end_date >= FROM_UNIXTIME(?) )
		ORDER BY b.start_date DESC, b.id DESC
		LIMIT 1
EOS
	) or FATAL "Error preparing panel billing info statement: ".$billdbh->errstr;

	$sth_lnp_number = $billdbh->prepare("
		SELECT lnp_provider_id
		  FROM lnp_numbers
		 WHERE ? LIKE CONCAT(number,'%')
		   AND (start <= FROM_UNIXTIME(?) OR start IS NULL)
		   AND (end > FROM_UNIXTIME(?) OR end IS NULL)
	".       join(", ", "ORDER BY LENGTH(number) DESC", @lnp_order_by) ."
				 LIMIT 1
	") or FATAL "Error preparing LNP number statement: ".$billdbh->errstr;

	$sth_profile_info = $billdbh->prepare(
		"SELECT id, source, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, use_free_time ".
		"FROM billing.billing_fees_history WHERE billing_profile_id = ? ".
		"AND bf_id IS NOT NULL AND type = ? ".
		"AND direction = ? AND ? REGEXP(source) AND ? REGEXP(destination) ".
		"ORDER BY LENGTH(destination) DESC, LENGTH(source) DESC LIMIT 1"
	) or FATAL "Error preparing profile info statement: ".$billdbh->errstr;

	$sth_lnp_profile_info = $billdbh->prepare(
		"SELECT id, source, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, use_free_time ".
		"FROM billing.billing_fees_history WHERE billing_profile_id = ? ".
		"AND bf_id IS NOT NULL AND type = ? ".
		"AND direction = ? AND destination = ? ".
		"LIMIT 1"
	) or FATAL "Error preparing LNP profile info statement: ".$billdbh->errstr;

	$sth_offpeak_weekdays = $billdbh->prepare( # TODO: optimize lines 4 and 10 below
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing.billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(FROM_UNIXTIME(?)) <= WEEKDAY(FROM_UNIXTIME(? + ?)) ".
		"AND weekday >= WEEKDAY(FROM_UNIXTIME(?)) ".
		"AND weekday <= WEEKDAY(FROM_UNIXTIME(? + ?)) ".
		"UNION ".
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing.billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(FROM_UNIXTIME(?)) > WEEKDAY(FROM_UNIXTIME(? + ?)) ".
		"AND (weekday >= WEEKDAY(FROM_UNIXTIME(?)) ".
		"OR weekday <= WEEKDAY(FROM_UNIXTIME(? + ?)))"
	) or FATAL "Error preparing weekday offpeak statement: ".$billdbh->errstr;

	$sth_offpeak_special = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end) ".
		"FROM billing.billing_peaktime_special ".
		"WHERE billing_profile_id = ? ".
		"AND ( ".
		"(start <= FROM_UNIXTIME(?) AND end >= FROM_UNIXTIME(?)) ".
		"OR (start >= FROM_UNIXTIME(?) AND end <= FROM_UNIXTIME(? + ?)) ".
		"OR (start <= FROM_UNIXTIME(? + ?) AND end >= FROM_UNIXTIME(? + ?)) ".
		")"
	) or FATAL "Error preparing special offpeak statement: ".$billdbh->errstr;

	$sth_unrated_cdrs = $acctdbh->prepare(
		"SELECT * ".
		"FROM accounting.cdr WHERE rating_status = 'unrated' ".
		"ORDER BY start_time ASC LIMIT 100 " # ."FOR UPDATE"
	) or FATAL "Error preparing unrated cdr statement: ".$acctdbh->errstr;

	$sth_update_cdr = $acctdbh->prepare(
		"UPDATE accounting.cdr SET ".
		"source_carrier_cost = ?, source_reseller_cost = ?, source_customer_cost = ?, ".
		"source_carrier_free_time = ?, source_reseller_free_time = ?, source_customer_free_time = ?, ".
		"rated_at = ?, rating_status = ?, ".
		"source_carrier_billing_fee_id = ?, source_reseller_billing_fee_id = ?, source_customer_billing_fee_id = ?, ".
		"source_carrier_billing_zone_id = ?, source_reseller_billing_zone_id = ?, source_customer_billing_zone_id = ?, ".
		"destination_carrier_cost = ?, destination_reseller_cost = ?, destination_customer_cost = ?, ".
		"destination_carrier_free_time = ?, destination_reseller_free_time = ?, destination_customer_free_time = ?, ".
		"destination_carrier_billing_fee_id = ?, destination_reseller_billing_fee_id = ?, destination_customer_billing_fee_id = ?, ".
		"destination_carrier_billing_zone_id = ?, destination_reseller_billing_zone_id = ?, destination_customer_billing_zone_id = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update cdr statement: ".$acctdbh->errstr;

	$sth_provider_info = $billdbh->prepare(
		"SELECT p.class, bm.billing_profile_id ".
		"FROM billing.products p, billing.billing_mappings bm ".
		"WHERE bm.contract_id = ? AND bm.product_id = p.id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= FROM_UNIXTIME(?)) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= FROM_UNIXTIME(?)) ".
		"ORDER BY bm.start_date, bm.id DESC ".
		"LIMIT 1"
	) or FATAL "Error preparing provider info statement: ".$billdbh->errstr;

	$sth_reseller_info = $billdbh->prepare(
		"SELECT bm.billing_profile_id, r.contract_id ".
		"FROM billing.billing_mappings bm, billing.voip_subscribers vs, ".
		"billing.contracts c, billing.contacts ct, billing.resellers r ".
		"WHERE vs.uuid = ? AND vs.contract_id = c.id ".
		"AND c.contact_id = ct.id ".
		"AND ct.reseller_id = r.id ".
		"AND r.contract_id = bm.contract_id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= FROM_UNIXTIME(?)) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= FROM_UNIXTIME(?)) ".
		"ORDER BY bm.start_date, bm.id DESC ".
		"LIMIT 1"
	) or FATAL "Error preparing reseller info statement: ".$billdbh->errstr;

	$sth_get_cbalances = $billdbh->prepare(
		"SELECT id, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, start, ".
		"unix_timestamp(start) start_unix, ".
		"unix_timestamp(end) end_unix ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"end >= FROM_UNIXTIME(?) ORDER BY start ASC"
	) or FATAL "Error preparing get contract balance statement: ".$billdbh->errstr;

	$sth_new_cbalance = $billdbh->prepare(
		"INSERT INTO billing.contract_balances (".
		" contract_id, cash_balance, cash_balance_interval, free_time_balance, free_time_balance_interval, underrun_profiles, underrun_lock, start, end".
		") VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?))"
	) or FATAL "Error preparing create contract balance statement: ".$billdbh->errstr;

	$sth_new_cbalance_infinite_future = $billdbh->prepare(
		"INSERT INTO billing.contract_balances (".
		" contract_id, cash_balance, cash_balance_interval, free_time_balance, free_time_balance_interval, underrun_profiles, underrun_lock, start, end".
		") VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), '9999-12-31 23:59:59')"
	) or FATAL "Error preparing create contract balance statement: ".$billdbh->errstr;

	$sth_update_cbalance_w_underrun_profiles_lock = $billdbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ?, underrun_profiles = FROM_UNIXTIME(?), underrun_lock = FROM_UNIXTIME(?) ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$billdbh->errstr;

	$sth_update_cbalance_w_underrun_lock = $billdbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ?, underrun_lock = FROM_UNIXTIME(?) ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$billdbh->errstr;

	$sth_update_cbalance_w_underrun_profiles = $billdbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ?, underrun_profiles = FROM_UNIXTIME(?) ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$billdbh->errstr;

	$sth_update_cbalance = $billdbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$billdbh->errstr;

	$sth_prepaid_costs = $acctdbh->prepare(
		"SELECT * FROM prepaid_costs order by timestamp asc" # newer entries overwrite older ones
	) or FATAL "Error preparing prepaid costs statement: ".$acctdbh->errstr;

	$sth_delete_prepaid_cost = $acctdbh->prepare(
		"DELETE FROM prepaid_costs WHERE call_id = ?"
	) or FATAL "Error preparing delete prepaid costs statement: ".$acctdbh->errstr;

	$sth_delete_old_prepaid = $acctdbh->prepare(
		"DELETE FROM prepaid_costs WHERE timestamp < DATE_SUB(NOW(), INTERVAL 7 DAY) LIMIT 10000"
	) or FATAL "Error preparing delete old prepaid statement: ".$acctdbh->errstr;

    $sth_get_billing_voip_subscribers = $billdbh->prepare(
        "SELECT uuid FROM billing.voip_subscribers WHERE contract_id = ? AND status != 'terminated'"
    ) or FATAL "Error preparing get billing voip subscribers statement: ".$billdbh->errstr;

    $sth_get_package_profile_sets = $billdbh->prepare(
        "SELECT profile_id, network_id FROM billing.package_profile_sets WHERE package_id = ? AND discriminator = ?"
    ) or FATAL "Error preparing get package profile sets statement: ".$billdbh->errstr;

    $sth_create_billing_mappings = $billdbh->prepare(
        "INSERT INTO billing.billing_mappings (billing_profile_id, network_id, product_id, start_date) VALUES (?, ?, ?, FROM_UNIXTIME(?))"
    ) or FATAL "Error preparing create billing mappings statement: ".$billdbh->errstr;

    $sth_get_provisioning_voip_subscribers = $provdbh->prepare(
        "SELECT id FROM provisioning.voip_subscribers WHERE uuid = ?"
    ) or FATAL "Error preparing get provisioning voip subscribers statement: ".$provdbh->errstr;
    $sth_get_usr_preference_attribute = $provdbh->prepare(
        "SELECT id FROM provisioning.voip_preferences WHERE attribute = ? AND usr_pref = 1"
    ) or FATAL "Error preparing get usr preference attribute statement: ".$provdbh->errstr;
    $sth_get_usr_preference_value = $provdbh->prepare(
        "SELECT id,value FROM provisioning.voip_usr_preferences WHERE attribute_id = ? AND subscriber_id = ?"
    ) or FATAL "Error preparing get usr preference value statement: ".$provdbh->errstr;
    $sth_create_usr_preference_value = $provdbh->prepare(
        "INSERT INTO provisioning.voip_usr_preferences (subscriber_id, attribute_id, value) VALUES (?, ?, ?)"
    ) or FATAL "Error preparing create usr preference value statement: ".$provdbh->errstr;
    $sth_update_usr_preference_value = $provdbh->prepare(
        "UPDATE provisioning.voip_usr_preferences SET value = ? WHERE id = ?"
    ) or FATAL "Error preparing update usr preference value statement: ".$provdbh->errstr;
    $sth_delete_usr_preference_value = $provdbh->prepare(
        "DELETE FROM provisioning.voip_usr_preferences WHERE id = ?"
    ) or FATAL "Error preparing delete usr preference value statement: ".$provdbh->errstr;

	if ($dupdbh) {
    	$sth_duplicate_cdr = $dupdbh->prepare(
    		'insert into cdr ('.
    		join(',', @cdr_fields).
    		') values ('.
    		join(',', (map {'?'} @cdr_fields)).
    		')'
    	) or FATAL "Error preparing duplicate_cdr statement: ".$dupdbh->errstr;
    }

	return 1;
}

sub lock_contracts {
	my $cdr = shift;
	my %provider_cids = ();
	$provider_cids{$cdr->{source_provider_id}} = 1 if $cdr->{source_provider_id} ne "0";
	$provider_cids{$cdr->{destination_provider_id}} = 1 if $cdr->{destination_provider_id} ne "0";
	my @pcids = keys %provider_cids;
	my $pcid_count = scalar @pcids;
	my $sth = undef;
	my %lock_cids = ();
	if ($pcid_count > 0) {
	    $sth = $billdbh->prepare("SELECT c.id from billing.contracts c ".
			"WHERE c.id IN (" . substr(',?' x $pcid_count,1) . ")")
			 or FATAL "Error preparing contract row lock selection statement: ".$billdbh->errstr;
	    $sth->execute(@pcids);
	    while (my @res = $sth->fetchrow_array) {
	        $lock_cids{$res[0]} = 1;
        }
        $sth->finish;
	}
	my %user_ids = ();
	$user_ids{$cdr->{source_user_id}} = 1 if $cdr->{source_user_id} ne "0";
	$user_ids{$cdr->{destination_user_id}} = 1 if $cdr->{destination_user_id} ne "0";
	my @uuids = keys %user_ids;
	my $uuid_count = scalar @uuids;
	if ($uuid_count > 0) {
		$sth = $billdbh->prepare("SELECT DISTINCT c.id from billing.contracts c ".
			" JOIN billing.voip_subscribers s ON c.id = s.contract_id ".
			"WHERE s.uuid IN (" . substr(',?' x $uuid_count,1) . ")")
			 or FATAL "Error preparing subscriber contract row lock selection statement: ".$billdbh->errstr;
		$sth->execute(@uuids);
	    while (my @res = $sth->fetchrow_array) {
	        $lock_cids{$res[0]} = 1;
        }
        $sth->finish;
	}
	my @cids = keys %lock_cids;
	my $lock_count = scalar @cids;
	if ($lock_count > 0) {
	    @cids = sort { $a <=> $b } @cids; #"Access your tables and rows in a fixed order."
	    #the 'for update' statement must avoid joins, otherwise all rows can get locked!
   	    my $sth = $billdbh->prepare("SELECT c.id from billing.contracts c ".
   			"WHERE c.id IN (" . substr(',?' x $lock_count,1) . ") FOR UPDATE")
   			 or FATAL "Error preparing contract row lock statement: ".$billdbh->errstr;
   	    $sth->execute(@cids);
        $sth->finish;
		DEBUG "$lock_count contract(s) locked: ".join(', ',@cids);
	#} else {
	#    FATAL "no contracts to lock";
	}

	return $lock_count;
}

sub _add_interval {
	my ($unit,$count,$from,$align_eom,$src) = @_; #all DateTimes here
	my $to;
	my $delta;
	if($unit eq "day") {
		$to = $from->clone->add(days => $count);
	} elsif($unit eq "hour") {
		$to = $from->clone->add(hours => $count);
	} elsif($unit eq "week") {
		$to = $from->clone->add(weeks => $count);
	} elsif($unit eq "month") {
		$to = $from->clone->add(months => $count, end_of_month => 'preserve');
		#DateTime's "preserve" mode would get from 30.Jan to 30.Mar, when adding 2 months
		#When adding 1 month two times, we get 28.Mar or 29.Mar, so we adjust:
		if (defined $align_eom
			&& $to->day > $align_eom->day
			&& $from->day == last_day_of_month($from)) {
			$delta = last_day_of_month($align_eom) - $align_eom->day;
			$to->set(day => last_day_of_month($to) - $delta);
		}
	} else {
		FATAL "Invalid interval unit '$unit' in $src";
	}
	return $to;
}

sub set_subscriber_first_int_attribute_value {

    my $contract_id = shift;
    my $new_value = shift;
    my $attribute = shift;

    my $changed = 0;
    my $attr_id = undef;
    my $sth;

    $sth_get_billing_voip_subscribers->execute($contract_id)
        or FATAL "Error executing get billing voip subscribers statement: ".
        $sth_get_billing_voip_subscribers->errstr;

	while (my @res = $sth_get_billing_voip_subscribers->fetchrow_array) {
	    my $uuid = $res[0];
	    $sth = $sth_get_provisioning_voip_subscribers;
	    $sth->execute($uuid)
	        or FATAL "Error executing get provisioning voip subscribers statement: ".$sth->errstr;
	    my ($prov_subs_id) = $sth->fetchrow_array();
	    $sth->finish;
        if (defined $prov_subs_id) {
            unless (defined $attr_id) {
                $sth = $sth_get_usr_preference_attribute;
                $sth->execute($attribute)
                    or FATAL "Error executing get '$attribute' usr preference attribute statement: ".$sth->errstr;
                ($attr_id) = $sth->fetchrow_array();
                $sth->finish;
                FATAL "Cannot find '$attribute' usr preference attribute" unless defined $attr_id;
            }
            $sth = $sth_get_usr_preference_value;
    	    $sth->execute($attr_id,$prov_subs_id)
    	        or FATAL "Error executing get '$attribute' usr preference value statement: ".$sth->errstr;
            my ($val_id,$old_value) = $sth->fetchrow_array();
            $sth->finish;
            undef $sth;
            if (defined $val_id) {
                if ($new_value == 0) {
                    $sth = $sth_delete_usr_preference_value;
                    $sth->execute($val_id)
                        or FATAL "Error executing delete '$attribute' usr preference value statement: ".$sth->errstr;
                    $changed++;
                    DEBUG "'$attribute' usr preference value ID $val_id with value '$old_value' deleted";
                } else {
                    $sth = $sth_update_usr_preference_value;
                    $sth->execute($new_value,$val_id)
                        or FATAL "Error executing update usr preference value statement: ".$sth->errstr;
                    $changed++;
                    DEBUG "'$attribute' usr preference value ID $val_id updated from old value '$old_value' to new value '$new_value'";
                }
            } elsif ($new_value > 0) {
                $sth = $sth_create_usr_preference_value;
                $sth->execute($prov_subs_id,$attr_id,$new_value)
                    or FATAL "Error executing create usr preference value statement: ".$sth->errstr;
                $changed++;
                DEBUG "'$attribute' usr preference value ID ".$provdbh->{'mysql_insertid'}." with value '$new_value' created";
            } else {
                DEBUG "'$attribute' usr preference value does not exists and no value is to be set";
            }
            $sth->finish if $sth;
        }
    }

    $sth_get_billing_voip_subscribers->finish;

    return $changed;

}

sub set_subscriber_lock_level {

    my $contract_id = shift;
    my $lock_level = shift; #int

    return set_subscriber_first_int_attribute_value($contract_id,$lock_level // 0,'lock');

}

sub switch_prepaid {

    my $contract_id = shift;
    my $prepaid = shift; #int

    return set_subscriber_first_int_attribute_value($contract_id,($prepaid ? 1 : 0),'prepaid');

}

sub add_profile_mappings {

    my $contract_id = shift;
    my $stime = shift;
    my $package_id = shift;
    my $profiles = shift;

    my $mappings_added = 0;
    my $profile_id;
    my $network_id;
    my $now = time;
    my $profile = undef;

    $sth_get_package_profile_sets->execute($package_id,$profiles)
        or FATAL "Error executing get package profile sets statement: ".$sth_get_package_profile_sets->errstr;

    while (my @res = $sth_get_package_profile_sets->fetchrow_array) {
        ($profile_id,$network_id) = @res;
        unless (defined $profile) {
            $profile = {};
            get_billing_info($now, $contract_id, undef, $profile) or
			    FATAL "Error getting billing info for date '".$now."' and contract_id $contract_id\n";
		}
        $sth_create_billing_mappings->execute($profile_id,$network_id,$profile->{product_id},$stime)
            or FATAL "Error executing create billing mappings statement: ".$sth_create_billing_mappings->errstr;
        $sth_create_billing_mappings->finish;
        $mappings_added++;
    }
    $sth_get_package_profile_sets->finish;
    if ($mappings_added > 0) {
        DEBUG "$mappings_added '$profiles' profile mappings added";
		get_billing_info($now, $contract_id, undef, $profile) or
			FATAL "Error getting billing info for date '".$now."' and contract_id $contract_id\n";
		switch_prepaid($contract_id,$profile->{prepaid});
    }

    return $mappings_added;

}

sub get_notopup_expiration {
    
    my $contract_id = shift;
    my $notopup_discard_intervals = shift;
    my $interval_unit = shift;
    my $package_id = shift;
    
    my $notopup_expiration = undef;
    my $last_topup_start_time;
    my $last_topup_start;
	if ($notopup_discard_intervals) { #get notopup_expiration:
        $sth = $sth_get_last_topup_cbalance;
		$sth->execute($contract_id) or FATAL "Error executing get latest contract balance statement: ".$sth->errstr;
		($last_topup_start_time) = $sth->fetchrow_array();
		$sth->finish;
		if (!$last_topup_start_time) {
			$sth = $sth_get_first_cbalance;
			$sth->execute($contract_id) or FATAL "Error executing get first contract balance statement: ".$sth->errstr;
			($last_topup_start_time) = $sth->fetchrow_array();
			$sth->finish;
		}
		if ($last_topup_start_time) {
			$last_topup_start = DateTime->from_epoch(epoch => $last_topup_start_time,
				time_zone => DateTime::TimeZone->new(name => 'local'));
			$notopup_expiration = _add_interval($interval_unit, $notopup_discard_intervals + 1,
				$last_topup_start, undef, "package id " . $package_id)->epoch;
		}
	}
	return $notopup_expiration;
	
}

sub catchup_contract_balance {

	my $end_time = shift;
	my $contract_id = shift;
	my $r_package_info = shift;

	DEBUG "catching up contract ID $contract_id balance rows";

	my $sth = $sth_get_contract_info;
	$sth->execute($contract_id) or FATAL "Error executing get info statement: ".$sth->errstr;
	my ($create_time,$modify,$contact_reseller_id,$package_id,$interval_unit,$interval_value,
	    $start_mode,$carry_over_mode,$notopup_discard_intervals,$underrun_profile_threshold,
	    $underrun_lock_threshold,$underrun_lock_level) = $sth->fetchrow_array();
	$sth->finish;
	$create_time ||= $modify; #contract create_timestamp might be 0000-00-00 00:00:00
	my $create_time_aligned;
	my $has_package = defined $package_id && defined $contact_reseller_id;

	if (!$has_package) { #backward-defaults
		$start_mode = "1st";
		$carry_over_mode = "carry_over";
	}

	$sth = $sth_get_last_cbalance;
	$sth->execute($contract_id) or FATAL "Error executing get latest contract balance statement: ".$sth->errstr;
	my ($last_id,$last_start,$last_end,$last_cash_balance,$last_cash_balance_int,$last_free_balance,$last_free_balance_int,$last_topups,$last_timely_topups) = $sth->fetchrow_array();
    $sth->finish;

	my $last_profile = undef;
	my $next_start;
	my $profile;
	my ($stime,$etime);
	my ($from,$to);
	my $align_eom;
	if ("create" eq $start_mode && defined $create_time) {
		$align_eom = DateTime->from_epoch(epoch => $create_time,
			time_zone => DateTime::TimeZone->new(name => 'local'));
	} #no eom preserve, since we don't have the begin of the first topup interval
	#} elsif ("topup_interval" eq $start_mode && defined x) {
	#    $align_eom = DateTime->from_epoch(epoch => x,
	#        time_zone => DateTime::TimeZone->new(name => 'local'));
	#}
	my $ratio;
	my $old_free_cash;
	my $cash_balance;
	my $cash_balance_interval;
	my $free_cash;
	my $free_time;
	my $free_time_balance;
	my $free_time_balance_interval;
	my $balances_count = 0;
	my ($underrun_lock_applied,$underrun_profiles_applied) = (0,0);
	my ($underrun_profiles_time,$underrun_lock_time) = (undef,undef);
	my $notopup_expiration = 0;
	my $now = time;

	while (defined $last_id && !_is_infinite_unix($last_end) && $last_end < $end_time) {
		$next_start = $last_end + 1;
		
        if ($balances_count == 0) {
            #we have two queries here, so do it only if really creating contract_balances
            $notopup_expiration = get_notopup_expiration($contract_id,$notopup_discard_intervals,$interval_unit,$package_id);
        }

		#profile of last and next interval:
		unless($last_profile) {
			#no ip here - same as in panel: for now we assume that the profiles in a contracts'
			#profile+network mapping schedule have the same free_time/free cash!
			$last_profile = {};
			get_billing_info($last_start < $create_time ? $create_time : $last_start, $contract_id, undef, $last_profile) or
				FATAL "Error getting billing info for date '".($last_start < $create_time ? $create_time : $last_start)."' and contract_id $contract_id\n";
		}
		($underrun_profiles_time,$underrun_lock_time) = (undef,undef);
PREPARE_BALANCE_CATCHUP:
		$profile = {};
		get_billing_info($next_start, $contract_id, undef, $profile) or
			FATAL "Error getting billing info for date '".$next_start."' and contract_id $contract_id\n";

		#stime, etime:
		$interval_unit = $has_package ? $interval_unit : ($profile->{int_unit} // 'month'); #backward-defaults
		$interval_value = $has_package ? $interval_value : ($profile->{int_count} // 1);

		$from = DateTime->from_epoch(epoch => $next_start,
			time_zone => DateTime::TimeZone->new(name => 'local'));
		if ("topup" eq $start_mode) {
			$stime = $from->epoch;
			$etime = undef;
		} else {
			$to = _add_interval($interval_unit, $interval_value, $from, $align_eom, $has_package ? "package id " . $package_id : "profile id " . $profile->{profile_id});
			$to->subtract(seconds => 1);
			$stime = $from->epoch;
			$etime = $to->epoch;
		}

		#balance values:
		$cash_balance = 0;
		if (("carry_over" eq $carry_over_mode || ("carry_over_timely" eq $carry_over_mode && $last_timely_topups > 0))
			&& (!$notopup_expiration || $stime < $notopup_expiration)) {

			$ratio = 1.0;
			if($create_time > $last_start and $create_time < $last_end) {
				$create_time_aligned = DateTime->from_epoch(epoch => $create_time,
					time_zone => DateTime::TimeZone->new(name => 'local'))->clone->truncate(to => 'day')->epoch;
				$create_time_aligned = $create_time if $create_time_aligned < $stime;
				$ratio = ($last_end + 1 - $create_time_aligned) / ($last_end + 1 - $last_start);
			}
			$last_free_balance_int = $last_profile->{int_free_cash} // 0.0; #backward-defaults
			$old_free_cash = $ratio * $last_free_balance_int;
			$cash_balance = $last_cash_balance;
			if ($last_cash_balance_int < $old_free_cash) {
				$cash_balance = $cash_balance + $last_cash_balance_int - $old_free_cash;
			}
			#free time corrections can take place here once..
			#$last_profile->{int_free_time} ...
		} else {
		    DEBUG "discarding cash balance (mode '$carry_over_mode'".($notopup_expiration ? ", notopup expiration " . $notopup_expiration : "").")";
		}
		$ratio = 1.0;
		$free_cash = $ratio * ($profile->{int_free_cash} // 0.0); #backward-defaults
		$cash_balance += $free_cash;
		$cash_balance_interval = 0.0;

		$free_time = $ratio * ($profile->{int_free_time} // 0);
		$free_time_balance = $free_time;
		$free_time_balance_interval = 0;

		if (!$underrun_lock_applied && defined $underrun_lock_threshold && $last_cash_balance >= $underrun_lock_threshold && $cash_balance < $underrun_lock_threshold) {
		    $underrun_lock_applied = 1;
		    DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun lock threshold $underrun_lock_threshold";
		    if (defined $underrun_lock_level) {
		        set_subscriber_lock_level($contract_id,$underrun_lock_level);
		        $underrun_lock_time = $now;
		    }
		}

		if (!$underrun_profiles_applied && defined $underrun_profile_threshold && $last_cash_balance >= $underrun_profile_threshold && $cash_balance < $underrun_profile_threshold) {
		    $underrun_profiles_applied = 1;
		    DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun profile threshold $underrun_profile_threshold";
		    if (add_profile_mappings($contract_id,$stime,$package_id,'underrun') > 0) {
		        $underrun_profiles_time = $now;
		        goto PREPARE_BALANCE_CATCHUP;
		    }
		}

		#exec create statement:
		$sth = (defined $etime ? $sth_new_cbalance : $sth_new_cbalance_infinite_future);
		($last_cash_balance,$last_cash_balance_int,$last_free_balance,$last_free_balance_int) =
		(sprintf("%.4f",$cash_balance), sprintf("%.4f",$cash_balance_interval),
			sprintf("%.0f",$free_time_balance), sprintf("%.0f",$free_time_balance_interval));
		my @bind_parms = ($contract_id,
			$last_cash_balance,$last_cash_balance_int,$last_free_balance,$last_free_balance_int,
			$underrun_profiles_time,$underrun_lock_time,$stime);
		push(@bind_parms,$etime) if defined $etime;
		$sth->execute(@bind_parms)
            or FATAL "Error executing new contract balance statement: ".$sth->errstr;
        $sth->finish;
		$balances_count++;

		#avoid reloading created balance:
		($last_id       ,$last_start,$last_end,$last_topups,$last_timely_topups) =
		($balances_count,$stime     ,$etime   ,0           ,0); #$billdbh->{'mysql_insertid'}

		DEBUG "contract balance created: ".(Dumper {
		    id => $billdbh->{'mysql_insertid'},
		    cash_balance => $last_cash_balance,
		    cash_balance_interval => $last_cash_balance_int,
		    free_time_balance => $last_free_balance,
		    free_time_balance_interval => $last_free_balance_int,
		    start_unix => $last_start,
		    end_unix => $last_end,
		    });

		$last_profile = $profile;

	}

	$r_package_info->{id} = $package_id;
    $r_package_info->{underrun_profile_threshold} = $underrun_profile_threshold;
	$r_package_info->{underrun_lock_threshold} = $underrun_lock_threshold;
	$r_package_info->{underrun_lock_level} = $underrun_lock_level;
	$r_package_info->{underrun_lock_applied} = $underrun_lock_applied;
	$r_package_info->{underrun_profiles_applied} = $underrun_profiles_applied;

	DEBUG "$balances_count contract balance rows created";

	return $balances_count;

}

sub get_contract_balances
{
	my $cdr = shift;
	my $contract_id = shift;
	my $r_package_info = shift;
	my $r_balances = shift;

	my $start_time = $cdr->{start_time};
	my $duration = $cdr->{duration};

	catchup_contract_balance($start_time + $duration,$contract_id,$r_package_info);
	#	or FATAL "Failed to catchup contract balances\n";

	my $sth = $sth_get_cbalances;
	$sth->execute($contract_id, $start_time)
		or FATAL "Error executing get contract balance statement: ".$sth->errstr;
	my $res = $sth->fetchall_arrayref({});
    $sth->finish;
	
	#FATAL "Failed to find contract balances\n" unless @$res;

	push(@$r_balances, @$res);

	return 1;
}

sub update_contract_balance
{
	my $r_balances = shift;
	
	my $changed = 0;

	for my $bal (@$r_balances) {
	    my @bind_parms = (
			    $bal->{cash_balance}, $bal->{cash_balance_interval},
			    $bal->{free_time_balance}, $bal->{free_time_balance_interval});
	    my $sth;
	    if (defined $bal->{underrun_profile_time} && defined $bal->{underrun_lock_time}) {
	        push(@bind_parms,$bal->{underrun_profile_time});
	        push(@bind_parms,$bal->{underrun_lock_time});
	        $sth = $sth_update_cbalance_w_underrun_profiles_lock;
	    } elsif (defined $bal->{underrun_profile_time}) {
	        push(@bind_parms,$bal->{underrun_profile_time});
	        $sth = $sth_update_cbalance_w_underrun_profiles;
	    } elsif (defined $bal->{underrun_lock_time}) {
	        push(@bind_parms,$bal->{underrun_lock_time});
	        $sth = $sth_update_cbalance_w_underrun_lock;
	    } else {
	        $sth = $sth_update_cbalance;
	    }
	    push(@bind_parms,$bal->{id});
	    $sth->execute(@bind_parms) or FATAL "Error executing update contract balance statement: ".$sth->errstr;
	    $sth->finish;
		$changed++;
	}
	
	DEBUG $changed . " contract balance row(s) updated";

	return 1;
}

sub get_subscriber_contract_id
{
	my $uuid = shift;

	my $sth = $sth_get_subscriber_contract_id;

	$sth->execute($uuid) or
		FATAL "Error executing get_subscriber_contract_id statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();
	FATAL "No contract id found for uuid '$uuid'\n" unless @res;

	return $res[0];
}

sub get_billing_info
{
	my $start = shift;
	my $contract_id = shift;
	my $source_ip = shift;
	my $r_info = shift;

	my $sth;
	if ($source_ip) {
		my $ip_size;
		my $ip = NetAddr::IP->new($source_ip);
		if($ip->version == 4) {
			$sth = $sth_billing_info_v4;
			$ip_size = 4;
		} elsif($ip->version == 6) {
			$sth = $sth_billing_info_v6;
			$ip_size = 16;
		} else {
			FATAL "Invalid source_ip $source_ip\n";
		}

		my $int_ip = $ip->bigint;
		my $ip_bytes = _bigint_to_bytes($int_ip, $ip_size);

		$sth->execute($contract_id, $start, $start, $ip_bytes, $ip_bytes) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
	} else {
		$sth = $sth_billing_info_panel;
		$sth->execute($contract_id, $start, $start) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
	}

	my @res = $sth->fetchrow_array();
	FATAL "No billing info found for contract_id $contract_id\n" unless @res;

	$r_info->{contract_id} = $contract_id;
	$r_info->{profile_id} = $res[0];
	$r_info->{product_id} = $res[1];
	$r_info->{prepaid} = $res[2];
	$r_info->{int_charge} = $res[3];
	$r_info->{int_free_time} = $res[4];
	$r_info->{int_free_cash} = $res[5];
	$r_info->{int_unit} = $res[6];
	$r_info->{int_count} = $res[7];

	$sth->finish;

	return 1;
}

sub get_profile_info
{
	my $bpid = shift;
	my $type = shift;
	my $direction = shift;
	my $source = shift;
	my $destination = shift;
	my $b_info = shift;
	my $start_time = shift;

	my @res;

	if($destination =~ /^\d+$/) {
		# let's see if we find the number in our LNP database
		$sth_lnp_number->execute($destination, $start_time, $start_time)
			or FATAL "Error executing LNP number statement: ".$sth_lnp_number->errstr;
		my ($lnppid) = $sth_lnp_number->fetchrow_array();

		if(defined $lnppid and $lnppid =~ /^\d+$/) {
			# let's see if we have a billing fee entry for the LNP provider ID
			$sth_lnp_profile_info->execute($bpid, $type, $direction, 'lnp:'.$lnppid)
				or FATAL "Error executing LNP profile info statement: ".$sth_lnp_profile_info->errstr;
			@res = $sth_lnp_profile_info->fetchrow_array();
			FATAL "Error fetching LNP profile info: ".$sth_lnp_profile_info->errstr
				if $sth_lnp_profile_info->err;
		}
	}

	my $sth = $sth_profile_info;

	unless(@res) {
		$sth->execute($bpid, $type, $direction, $source, $destination)
			or FATAL "Error executing profile info statement: ".$sth->errstr;
		@res = $sth->fetchrow_array();
	}

	return 0 unless @res;

	$b_info->{fee_id} = $res[0];
	$b_info->{source_pattern} = $res[1];
	$b_info->{pattern} = $res[2];
	$b_info->{on_init_rate} = $res[3];
	$b_info->{on_init_interval} = $res[4] == 0 ? 1 : $res[4]; # prevent loops
	$b_info->{on_follow_rate} = $res[5];
	$b_info->{on_follow_interval} = $res[6] == 0 ? 1 : $res[6];
	$b_info->{off_init_rate} = $res[7];
	$b_info->{off_init_interval} = $res[8] == 0 ? 1 : $res[8];
	$b_info->{off_follow_rate} = $res[9];
	$b_info->{off_follow_interval} = $res[10] == 0 ? 1 : $res[10];
	$b_info->{zone_id} = $res[11];
	$b_info->{use_free_time} = $res[12];

	$sth->finish;

	return 1;
}

sub get_offpeak_weekdays
{
	my $bpid = shift;
	my $start = shift;
	my $duration = shift;
	my $r_offpeaks = shift;

	my $sth = $sth_offpeak_weekdays;

	$sth->execute(
		$bpid,
		$start, $start, $duration,
		$start, $start, $duration,
		$bpid,
		$start, $start, $duration,
		$start, $start, $duration
	) or FATAL "Error executing weekday offpeak statement: ".$sth->errstr;

	while(my @res = $sth->fetchrow_array())
	{
		my %e = ();
		$e{weekday} = $res[0];
		$e{start} = $res[1];
		$e{end} = $res[2];
		push @$r_offpeaks, \%e;
	}

	return 1;
}

sub get_offpeak_special
{
	my $bpid = shift;
	my $start = shift;
	my $duration = shift;
	my $r_offpeaks = shift;

	my $sth = $sth_offpeak_special;

	$sth->execute(
		$bpid,
		$start, $start,
		$start, $start, $duration,
		$start, $duration, $start, $duration
	) or FATAL "Error executing special offpeak statement: ".$sth->errstr;

	while(my @res = $sth->fetchrow_array())
	{
		my %e = ();
		$e{start} = $res[0];
		$e{end} = $res[1];
		push @$r_offpeaks, \%e;
	}

	return 1;
}

sub is_offpeak_special
{
	my $start = shift;
	my $offset = shift;
	my $r_offpeaks = shift;

	my $secs = $start + $offset; # we have unix-timestamp as referenec

	foreach my $r_o(@$r_offpeaks)
	{
		return 1 if($secs >= $r_o->{start} && $secs <= $r_o->{end});
	}

	return 0;
}

sub is_offpeak_weekday
{
	my $start = shift;
	my $offset = shift;
	my $r_offpeaks = shift;

	my ($S, $M, $H, $d, $m, $y, $wd, $yd, $dst) = localtime($start + $offset);
	$wd = ($wd - 1) % 7; # convert to MySQL notation (mysql: mon=0, unix: mon=1)
	$y += 1900; $m += 1;
	#$H -= 1 if($dst == 1); # regard daylight saving time

	my $secs = $S + $M * 60 + $H * 3600; # we have seconds since midnight as reference
	foreach my $r_o(@$r_offpeaks)
	{
		return 1 if($wd == $r_o->{weekday} &&
			$secs >= $r_o->{start} && $secs <= $r_o->{end});
	}

	return 0;
}

sub check_shutdown {
	if ($shutdown) {
		syslog('warning', 'Shutdown detected, aborting work in progress');
		return 1;
	}
	return 0;
}

sub get_unrated_cdrs {
	my $r_cdrs = shift;

	my $sth = $sth_unrated_cdrs;
	$sth->execute
		or FATAL "Error executing unrated cdr statement: ".$sth->errstr;

    #my @cdrs = ();

	while (my $cdr = $sth->fetchrow_hashref()) {
	    #push(@cdrs,$cdr);
	    push(@$r_cdrs,$cdr);
		check_shutdown() and return 0;
	}

	# the while above may have been interupted because there is no
	# data left, or because there was an error. To decide what
	# happened, we have to query $sth->err()
	FATAL "Error fetching unrated cdr's: ". $sth->errstr
		if $sth->err;
    $sth->finish;

    #sort by end time:
    #foreach my $cdr (sort {($a->{start_time} + $a->{duration}) <=> ($b->{start_time} + $b->{duration})} @cdrs) {
    #    push(@$r_cdrs,$cdr);
    #}

	return 1;
}

sub update_cdr
{
	my $cdr = shift;

	$cdr->{rating_status} = 'ok';
	$cdr->{rated_at} = sql_time(time());

	my $sth = $sth_update_cdr;
	$sth->execute(
		$cdr->{source_carrier_cost}, $cdr->{source_reseller_cost}, $cdr->{source_customer_cost},
		$cdr->{source_carrier_free_time}, $cdr->{source_reseller_free_time}, $cdr->{source_customer_free_time},
		$cdr->{rated_at}, $cdr->{rating_status},
		$cdr->{source_carrier_billing_fee_id}, $cdr->{source_reseller_billing_fee_id}, $cdr->{source_customer_billing_fee_id},
		$cdr->{source_carrier_billing_zone_id}, $cdr->{source_reseller_billing_zone_id}, $cdr->{source_customer_billing_zone_id},
		$cdr->{destination_carrier_cost}, $cdr->{destination_reseller_cost}, $cdr->{destination_customer_cost},
		$cdr->{destination_carrier_free_time}, $cdr->{destination_reseller_free_time}, $cdr->{destination_customer_free_time},
		$cdr->{destination_carrier_billing_fee_id}, $cdr->{destination_reseller_billing_fee_id}, $cdr->{destination_customer_billing_fee_id},
		$cdr->{destination_carrier_billing_zone_id}, $cdr->{destination_reseller_billing_zone_id}, $cdr->{destination_customer_billing_zone_id},
		$cdr->{id})
		or FATAL "Error executing update cdr statement: ".$sth->errstr;

	if ($sth_duplicate_cdr) {
		$sth_duplicate_cdr->execute(@$cdr{@cdr_fields})
		or FATAL "Error executing duplicate cdr statement: ".$sth_duplicate_cdr->errstr;
	}

	return 1;
}

sub get_provider_info
{
	my $pid = shift;
	my $start = shift;
	my $r_info = shift;

	my $sth = $sth_provider_info;
	$sth->execute($pid, $start, $start)
		or FATAL "Error executing provider info statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();
	FATAL "No provider info for provider id $pid found\n"
		unless(@res);

	$r_info->{class} = $res[0];
	$r_info->{profile_id} = $res[1];
	$r_info->{contract_id} = $pid;

	return 1;
}

sub get_reseller_info
{
	my $uuid = shift;
	my $start = shift;
	my $r_info = shift;

	my $sth = $sth_reseller_info;
	$sth->execute($uuid, $start, $start)
		or FATAL "Error executing reseller info statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();
	FATAL "No reseller info for user id $uuid found\n"
		unless(@res);

	$r_info->{profile_id} = $res[0];
	$r_info->{class} = 'reseller';
	$r_info->{contract_id} = $res[1];

	return 1;
}

sub get_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $contract_id = shift;
	my $profile_id = shift;
	my $r_profile_info = shift;
	my $r_package_info = shift;
	my $r_cost = shift;
	my $r_real_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $r_onpeak = shift;
	my $r_balances = shift;

	$$r_rating_duration = 0; # ensure we start with zero length

	my $src_user = $cdr->{source_cli};
	my $src_user_domain = $cdr->{source_cli}.'@'.$cdr->{source_domain};
	my $dst_user = $cdr->{destination_user_in};
	my $dst_user_domain = $cdr->{destination_user_in}.'@'.$cdr->{destination_domain};

	DEBUG "fetching call cost for profile_id $profile_id with type $type, direction $direction, ".
		"src_user_domain $src_user_domain, dst_user_domain $dst_user_domain";

	unless(get_profile_info($profile_id, $type, $direction, $src_user_domain, $dst_user_domain,
		$r_profile_info, $cdr->{start_time}))
	{
		DEBUG "no match for full uris, trying user only for profile_id $profile_id with type $type, direction $direction, ".
			"src_user_domain $src_user, dst_user_domain $dst_user";
		unless(get_profile_info($profile_id, $type, $direction, $src_user, $dst_user,
			$r_profile_info, $cdr->{start_time}))
		{
			# we gracefully ignore missing profile infos for inbound direction
			FATAL "No outbound fee info for profile $profile_id and ".
			"source user '$src_user' or user/domain '$src_user_domain' and ".
			"destination user '$dst_user' or user/domain '$dst_user_domain' ".
			"found\n" if($direction eq "out");
			$$r_cost = 0;
			$$r_free_time = 0;
			return 1;
		}
	}

	DEBUG "billing fee is ".(Dumper $r_profile_info);

	#print Dumper $r_profile_info;

	my @offpeak_weekdays = ();
	get_offpeak_weekdays($profile_id, $cdr->{start_time},
		$cdr->{duration}, \@offpeak_weekdays) or
		FATAL "Error getting weekdays offpeak info\n";
	#print Dumper \@offpeak_weekdays;

	my @offpeak_special = ();
	get_offpeak_special($profile_id, $cdr->{start_time},
		$cdr->{duration}, \@offpeak_special) or
		FATAL "Error getting special offpeak info\n";
	#print Dumper \@offpeak_special;

	$$r_cost = 0;
	$$r_real_cost = 0;
	$$r_free_time = 0;
	my $interval = 0;
	my $rate = 0;
	my $offset = 0;
	my $onpeak = 0;
	my $init = 0;
	my $duration = $cdr->{duration};
	my $prev_bal_id = undef;
	my @cash_balance_rates = ();
	my $prev_cash_balance = undef;
	my $last_bal = undef;
    my $cash_balance_rate_sum;
	my ($underrun_lock_applied,$underrun_profiles_applied) = ($r_package_info->{underrun_lock_applied},$r_package_info->{underrun_profiles_applied});
	my ($underrun_profiles_time,$underrun_lock_time) = (undef,undef);
	my %bal_map = map { $_->{id} => $_; } @$r_balances;

	if($duration == 0) {  # zero duration call, yes these are possible
		if(is_offpeak_special($cdr->{start_time}, $offset, \@offpeak_special)
				   or is_offpeak_weekday($cdr->{start_time}, $offset, \@offpeak_weekdays))
		{
			$$r_onpeak = 0;
		} else {
			$$r_onpeak = 1;
		}
	}

	while($duration > 0)
	{
		DEBUG "try to rate remaining duration of $duration secs";

		if(is_offpeak_special($cdr->{start_time}, $offset, \@offpeak_special))
		{
			#print "offset $offset is offpeak-special\n";
			$onpeak = 0;
		}
		elsif(is_offpeak_weekday($cdr->{start_time}, $offset, \@offpeak_weekdays))
		{
			#print "offset $offset is offpeak-weekday\n";
			$onpeak = 0;
		}
		else
		{
			#print "offset $offset is onpeak\n";
			$onpeak = 1;
		}

		unless($init)
		{
			$init = 1;
			$interval = $onpeak == 1 ?
				$r_profile_info->{on_init_interval} : $r_profile_info->{off_init_interval};
			$rate = $onpeak == 1 ?
				$r_profile_info->{on_init_rate} : $r_profile_info->{off_init_rate};
			$$r_onpeak = $onpeak;
			DEBUG "add init rate $rate per sec to costs";
		}
		else
		{
			$interval = $onpeak == 1 ?
				$r_profile_info->{on_follow_interval} : $r_profile_info->{off_follow_interval};
			$rate = $onpeak == 1 ?
				$r_profile_info->{on_follow_rate} : $r_profile_info->{off_follow_rate};
			DEBUG "add follow rate $rate per sec to costs";
		}
		$rate *= $interval;
		DEBUG "interval is $interval, so rate for this interval is $rate";

		#my @bals = grep {($_->{start_unix} + $offset) <= $cdr->{start_time}} @$r_balances;
		my $current_call_time = int($cdr->{start_time} + $offset);
		my @bals = grep {
			$_->{start_unix} <= $current_call_time &&
			($current_call_time <= $_->{end_unix} || _is_infinite_unix($_->{end_unix}))
		} @$r_balances;
		@bals or FATAL "No contract balance for CDR $cdr->{id} found";
		WARNING "overlapping contract balances for CDR $cdr->{id} found: ".(Dumper \@bals) if (scalar @bals) > 1;
		foreach my $bal (@bals) {
		    delete $bal_map{$bal->{id}};
		}
		@bals = sort {$a->{start_unix} <=> $b->{start_unix}} @bals;
		my $bal = $bals[0];
		$last_bal = $bal;

		if (defined $prev_bal_id) {
		    if ($bal->{id} != $prev_bal_id) { #contract balance transition
		        DEBUG "next contract balance entered: ".(Dumper $bal);
		        $prev_cash_balance = $bal->{cash_balance};
		        #carry over the costs so far:
		        $cash_balance_rate_sum = 0;
		        foreach my $cash_balance_rate (@cash_balance_rates) {
                    if ($cash_balance_rate <= $bal->{cash_balance}) {
            			$bal->{cash_balance} -= $cash_balance_rate;
            			$cash_balance_rate_sum += $cash_balance_rate;
            		}
		        }
		        DEBUG "carry over costs - rates of $cash_balance_rate_sum so far were subtracted from cash balance $prev_cash_balance";
		        $prev_bal_id = $bal->{id};
		    }
	    } else {
	        DEBUG "starting with contract balance: ".(Dumper $bal);
	        $prev_bal_id = $bal->{id};
	        $prev_cash_balance = $bal->{cash_balance};
	    }

    	if ($r_profile_info->{use_free_time} && $bal->{free_time_balance} >= $interval) {
			DEBUG "subtracting $interval sec from free_time_balance $$bal{free_time_balance} and skip costs for this interval";
			$$r_rating_duration += $interval;
			$duration -= $interval;
			$bal->{free_time_balance} -= $interval;
			$bal->{free_time_balance_interval} += $interval;
			$$r_free_time += $interval;
			next;
		}

		if ($r_profile_info->{use_free_time} && $bal->{free_time_balance} > 0) {
			DEBUG "using $$bal{free_time_balance} sec free time for this interval and calculate cost for remaining interval chunk";
			$$r_free_time += $bal->{free_time_balance};
			$$r_rating_duration += $bal->{free_time_balance};
			$duration -= $bal->{free_time_balance};
			$bal->{free_time_balance_interval} += $bal->{free_time_balance};
			$rate *= 1.0 - ($bal->{free_time_balance} / $interval);
			$interval -= $bal->{free_time_balance};
			$bal->{free_time_balance} = 0;
			DEBUG "calculate cost for remaining interval chunk $interval";
		}

		if ($rate <= $bal->{cash_balance}) {
			DEBUG "we still have cash balance $$bal{cash_balance} left, subtract rate $rate from that";
			$bal->{cash_balance} -= $rate;
			push(@cash_balance_rates,$rate);
		} else {
			DEBUG "add current interval cost $rate to total cost $$r_cost";
			$$r_cost += $rate;
			$bal->{cash_balance_interval} += $rate;
		}

		$$r_real_cost += $rate;

		$duration -= $interval;
		$$r_rating_duration += $interval;

		$offset += $interval;
	}

	if ((scalar @cash_balance_rates) > 0) {
	    my @remaining_bals = sort {$a->{start_unix} <=> $b->{start_unix}} values %bal_map;
	    foreach my $bal (@remaining_bals) {
	        DEBUG "remaining contract balance: ".(Dumper $bal);
	        $last_bal = $bal;
		    $prev_cash_balance = $bal->{cash_balance};
            $cash_balance_rate_sum = 0;
		    foreach my $cash_balance_rate (@cash_balance_rates) {
                if ($cash_balance_rate <= $bal->{cash_balance}) {
                    $bal->{cash_balance} -= $cash_balance_rate;
            		$cash_balance_rate_sum += $cash_balance_rate;
            	}
		    }
		    DEBUG "carry over costs - rates of $cash_balance_rate_sum so far were subtracted from cash balance $prev_cash_balance";
		}
	}

	if (defined $last_bal && defined $prev_cash_balance) {
	    my $now = time;
        if (!$underrun_lock_applied && defined $r_package_info->{underrun_lock_threshold} && $prev_cash_balance >= $r_package_info->{underrun_lock_threshold} && $last_bal->{cash_balance} < $r_package_info->{underrun_lock_threshold}) {
    	    $underrun_lock_applied = 1;
    	    DEBUG "cash balance was decreased from $prev_cash_balance to $last_bal->{cash_balance} and dropped below underrun lock threshold $r_package_info->{underrun_lock_threshold}";
    	    if (defined $r_package_info->{underrun_lock_level}) {
    	        set_subscriber_lock_level($contract_id,$r_package_info->{underrun_lock_level});
    	        $last_bal->{underrun_lock_time} = $now;
    	    }
    	}

    	if (!$underrun_profiles_applied && defined $r_package_info->{underrun_profile_threshold} && $prev_cash_balance >= $r_package_info->{underrun_profile_threshold} && $last_bal->{cash_balance} < $r_package_info->{underrun_profile_threshold}) {
    	    $underrun_profiles_applied = 1;
    	    DEBUG "cash balance was decreased from $prev_cash_balance to $last_bal->{cash_balance} and dropped below underrun profile threshold $r_package_info->{underrun_profile_threshold}";
    	    if (add_profile_mappings($contract_id,$cdr->{start_time} + $cdr->{duration},$r_package_info->{id},'underrun') > 0) {
    	        $last_bal->{underrun_profile_time} = $now;
    	    }
    	}
    }

	return 1;
}

sub get_customer_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;
	my $real_cost = 0;

	my $dir;
	if($direction eq "out") {
		$dir = "source_";
	} else {
		$dir = "destination_";
	}

	my $contract_id = get_subscriber_contract_id($cdr->{$dir."user_id"});

	my @balances;
	my %package_info = ();
	get_contract_balances($cdr, $contract_id, \%package_info, \@balances);

	my %billing_info = (); #profiles might have switched due to underrun while was carry over discarded
	get_billing_info($cdr->{start_time}, $contract_id, $cdr->{source_ip}, \%billing_info) or
		FATAL "Error getting billing info\n";
	#print Dumper \%billing_info;

	unless($billing_info{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		return -1;
	}

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,
		$billing_info{profile_id}, \%profile_info, \%package_info, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, \@balances)
		or FATAL "Error getting customer call cost\n";

	$cdr->{$dir."customer_billing_fee_id"} = $profile_info{fee_id};
	$cdr->{$dir."customer_billing_zone_id"} = $profile_info{zone_id};
	DEBUG "got call cost $$r_cost and free time $r_free_time";

	# we don't do prepaid for termination fees for now, so treat it as post-paid
	if($billing_info{prepaid} != 1 || $direction eq "in")
	{
		if($billing_info{prepaid} == 1 && $direction eq "in") {
			DEBUG "treat pre-paid billing profile as post-paid for termination fees";
			$$r_cost = $real_cost;
		} else {
			DEBUG "billing profile is post-paid, update contract balance";
		}
		update_contract_balance(\@balances)
			or FATAL "Error updating customer contract balance\n";
	}
	else {
		DEBUG "billing profile is prepaid";
		# overwrite the calculated costs with the ones from our table
		if (!$prepaid_costs) {
			DEBUG "no prepaid_costs, fetch it";
			$sth_prepaid_costs->execute()
				or FATAL "Error executing get prepaid costs statement: ".$sth_prepaid_costs->errstr;
			$prepaid_costs = $sth_prepaid_costs->fetchall_hashref('call_id');
		} else {
			DEBUG "already prefetched prepaid_costs";
		}
		if (exists($prepaid_costs->{$cdr->{call_id}})) {
			my $entry = $prepaid_costs->{$cdr->{call_id}};
			$$r_cost = $entry->{cost};
			$$r_free_time = $entry->{free_time_used};
			$sth_delete_prepaid_cost->execute($entry->{call_id});
			delete($prepaid_costs->{$cdr->{call_id}});
		} else {
			update_contract_balance(\@balances)
				or FATAL "Error updating customer contract balance\n";
			$$r_cost = $real_cost;
		}
	}

	DEBUG "cost for this call is $$r_cost";

	return 1;
}

sub get_provider_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $r_info = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;
	my $real_cost = 0;

    my $contract_id = $$r_info{contract_id};

	my @balances;
	my %package_info = ();
	get_contract_balances($cdr, $contract_id, \%package_info, \@balances);

	my %billing_info = ();
	get_billing_info($cdr->{start_time}, $contract_id, $cdr->{source_ip}, \%billing_info) or
		FATAL "Error getting billing info\n";
	#print Dumper \%billing_info;

	unless($billing_info{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		return -1;
	}

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,
		$r_info->{profile_id}, \%profile_info, \%package_info, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, \@balances)
		or FATAL "Error getting provider call cost\n";

	unless($billing_info{prepaid} == 1)
	{
		update_contract_balance(\@balances)
			or FATAL "Error updating provider contract balance\n";
	}

	if($r_info->{class} eq "reseller")
	{
		if($direction eq 'out') {
			$cdr->{source_reseller_billing_fee_id} = $profile_info{fee_id};
			$cdr->{source_reseller_billing_zone_id} = $profile_info{zone_id};
		} elsif($direction eq 'in') {
			$cdr->{destination_reseller_billing_fee_id} = $profile_info{fee_id};
			$cdr->{destination_reseller_billing_zone_id} = $profile_info{zone_id};
		}
	}
	else
	{
		if($direction eq 'out') {
			$cdr->{source_carrier_billing_fee_id} = $profile_info{fee_id};
			$cdr->{source_carrier_billing_zone_id} = $profile_info{zone_id};
		} elsif($direction eq 'in') {
			$cdr->{destination_carrier_billing_fee_id} = $profile_info{fee_id};
			$cdr->{destination_carrier_billing_zone_id} = $profile_info{zone_id};
		}
	}

	return 1;
}

sub rate_cdr
{
	my $cdr = shift;
	my $type = shift;

	my $source_customer_cost = 0;
	my $source_carrier_cost = 0;
	my $source_reseller_cost = 0;
	my $source_customer_free_time = 0;
	my $source_carrier_free_time = 0;
	my $source_reseller_free_time = 0;
	my $destination_customer_cost = 0;
	my $destination_carrier_cost = 0;
	my $destination_reseller_cost = 0;
	my $destination_customer_free_time = 0;
	my $destination_carrier_free_time = 0;
	my $destination_reseller_free_time = 0;

	my $direction;
	my $rating_duration;

	unless($cdr->{call_status} eq "ok")
	{
		DEBUG "cdr #$$cdr{id} has call_status $$cdr{call_status}, skip.";
		$cdr->{source_carrier_cost} = $source_carrier_cost;
		$cdr->{source_reseller_cost} = $source_reseller_cost;
		$cdr->{source_customer_cost} = $source_customer_cost;
		$cdr->{source_carrier_free_time} = $source_carrier_free_time;
		$cdr->{source_reseller_free_time} = $source_reseller_free_time;
		$cdr->{source_customer_free_time} = $source_customer_free_time;
		$cdr->{destination_carrier_cost} = $destination_carrier_cost;
		$cdr->{destination_reseller_cost} = $destination_reseller_cost;
		$cdr->{destination_customer_cost} = $destination_customer_cost;
		$cdr->{destination_carrier_free_time} = $destination_carrier_free_time;
		$cdr->{destination_reseller_free_time} = $destination_reseller_free_time;
		$cdr->{destination_customer_free_time} = $destination_customer_free_time;
		return 1;
	}

	DEBUG "fetching source provider info for source_provider_id #$$cdr{source_provider_id}";
	my %source_provider_info = ();
	if($cdr->{source_provider_id} eq "0") {
		WARNING "Missing source_provider_id for source_user_id ".$cdr->{source_user_id}." in cdr #".$cdr->{id}."\n";
	} else {
		get_provider_info($cdr->{source_provider_id}, $cdr->{start_time}, \%source_provider_info)
			or FATAL "Error getting source provider info for cdr #".$cdr->{id}."\n";
	}
	DEBUG "source_provider_info is ".(Dumper \%source_provider_info);

	#unless($source_provider_info{profile_info}) {
	#   FATAL "Missing billing profile for source_provider_id ".$cdr->{source_provider_id}." for cdr #".$cdr->{id}."\n";
	#}

	DEBUG "fetching destination provider info for destination_provider_id #$$cdr{destination_provider_id}";
	my %destination_provider_info = ();
	if($cdr->{destination_provider_id} eq "0") {
		WARNING "Missing destination_provider_id for destination_user_id ".$cdr->{destination_user_id}." in cdr #".$cdr->{id}."\n";
	} else {
		get_provider_info($cdr->{destination_provider_id}, $cdr->{start_time}, \%destination_provider_info)
			or FATAL "Error getting destination provider info for cdr #".$cdr->{id}."\n";
	}
	DEBUG "destination_provider_info is ".(Dumper \%destination_provider_info);

	#unless($destination_provider_info{profile_info}) {
	#   FATAL "Missing billing profile for destination_provider_id ".$cdr->{destination_provider_id}." for cdr #".$cdr->{id}."\n";
	#}

	# call from local subscriber
	if($cdr->{source_user_id} ne "0") {
		DEBUG "call from local subscriber, source_user_id is $$cdr{source_user_id}";
		# if we have a call from local subscriber, the source provider MUST be a reseller
		if($source_provider_info{profile_id} && $source_provider_info{class} ne "reseller") {
			FATAL "The local source_user_id ".$cdr->{source_user_id}." has a source_provider_id ".$cdr->{source_provider_id}.
				" which is not a reseller in cdr #".$cdr->{id}."\n";
		}

		if($cdr->{destination_user_id} ne "0") {
			DEBUG "call to local subscriber, destination_user_id is $$cdr{destination_user_id}";
			# call to local subscriber (on-net)

			# there is no carrier cost for on-net calls

			# for calls towards a local user, termination fees might apply if
			# we find a fee with direction "in"
			if($destination_provider_info{profile_id}) {
				DEBUG "destination provider has billing profile $destination_provider_info{profile_id}, get reseller termination cost";
				get_provider_call_cost($cdr, $type, "in",
							\%destination_provider_info, \$destination_reseller_cost, \$destination_reseller_free_time,
							\$rating_duration)
					or FATAL "Error getting destination reseller cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
				DEBUG "destination reseller termination cost is $destination_reseller_cost";
			} else {
				# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
				# in theory, all resellers MUST have a billing profile, so we could bail out here
				DEBUG "destination provider $$cdr{destination_provider_id} has no billing profile, skip reseller termination cost";
			}
			DEBUG "get customer termination cost for destination_user_id $$cdr{destination_user_id}";
			get_customer_call_cost($cdr, $type, "in",
						\$destination_customer_cost, \$destination_customer_free_time,
						\$rating_duration)
				or FATAL "Error getting destination customer cost for local destination_user_id ".
						$cdr->{destination_user_id}." for cdr ".$cdr->{id}."\n";
			DEBUG "destination customer termination cost is $destination_customer_cost";

		} else {
			# we can't charge termination fees to the callee if it's not local

			# for the carrier cost, we use the destination billing profile of a peer
			# (this is what the peering provider is charging the carrier)
			if($destination_provider_info{profile_id}) {
				DEBUG "fetching source_carrier_cost based on destination_provider_info ".(Dumper \%destination_provider_info);
				get_provider_call_cost($cdr, $type, "out",
							\%destination_provider_info, \$source_carrier_cost, \$source_carrier_free_time,
							\$rating_duration)
						or FATAL "Error getting source carrier cost for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing destination profile, so we can't calculate source_carrier_cost for destination_provider_info ".(Dumper \%destination_provider_info);
			}
		}

		# get reseller cost
		if($source_provider_info{profile_id}) {
			get_provider_call_cost($cdr, $type, "out",
						\%source_provider_info, \$source_reseller_cost, \$source_reseller_free_time,
						\$rating_duration)
				 or FATAL "Error getting source reseller cost for cdr ".$cdr->{id}."\n";
		} else {
			# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
			# in theory, all resellers MUST have a billing profile, so we could bail out here
		}

		# get customer cost
		get_customer_call_cost($cdr, $type, "out",
					\$source_customer_cost, \$source_customer_free_time,
					\$rating_duration)
			or FATAL "Error getting source customer cost for local source_user_id ".
					$cdr->{source_user_id}." for cdr ".$cdr->{id}."\n";
	} else {
		# call from a foreign caller

		# in this case, termination fees for the callee might still apply
		if($cdr->{destination_user_id} ne "0") {
			# call to local subscriber

			# for calls towards a local user, termination fees might apply if
			# we find a fee with direction "in"

			# we use the source provider info (the one of the peer) for the carrier termination fees,
			# as this is what the peer is charging us
			if($source_provider_info{profile_id}) {
				DEBUG "fetching destination_carrier_cost based on source_provider_info ".(Dumper \%source_provider_info);
				get_provider_call_cost($cdr, $type, "in",
							\%source_provider_info, \$destination_carrier_cost, \$destination_carrier_free_time,
							\$rating_duration)
					or FATAL "Error getting destination carrier cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing source profile, so we can't calculate destination_carrier_cost for source_provider_info ".(Dumper \%source_provider_info);
			}
			if($destination_provider_info{profile_id}) {
				DEBUG "fetching destination_reseller_cost based on source_provider_info ".(Dumper \%destination_provider_info);
				get_provider_call_cost($cdr, $type, "in",
							\%destination_provider_info, \$destination_reseller_cost, \$destination_reseller_free_time,
							\$rating_duration)
					or FATAL "Error getting destination reseller cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
				# in theory, all resellers MUST have a billing profile, so we could bail out here
				WARNING "missing destination profile, so we can't calculate destination_reseller_cost for destination_provider_info ".(Dumper \%destination_provider_info);
			}
			get_customer_call_cost($cdr, $type, "in",
						\$destination_customer_cost, \$destination_customer_free_time,
						\$rating_duration)
				or FATAL "Error getting destination customer cost for local destination_user_id ".
						$cdr->{destination_user_id}." for cdr ".$cdr->{id}."\n";
		} else {
			# TODO what about transit calls?
		}
	}

	$cdr->{source_carrier_cost} = $source_carrier_cost;
	$cdr->{source_reseller_cost} = $source_reseller_cost;
	$cdr->{source_customer_cost} = $source_customer_cost;
	$cdr->{source_carrier_free_time} = $source_carrier_free_time;
	$cdr->{source_reseller_free_time} = $source_reseller_free_time;
	$cdr->{source_customer_free_time} = $source_customer_free_time;
	$cdr->{destination_carrier_cost} = $destination_carrier_cost;
	$cdr->{destination_reseller_cost} = $destination_reseller_cost;
	$cdr->{destination_customer_cost} = $destination_customer_cost;
	$cdr->{destination_carrier_free_time} = $destination_carrier_free_time;
	$cdr->{destination_reseller_free_time} = $destination_reseller_free_time;
	$cdr->{destination_customer_free_time} = $destination_customer_free_time;
	return 1;
}

sub daemonize
{
	my $pidfile = shift;

	chdir '/' or FATAL "Can't chdir to /: $!\n";
	open STDIN, '<', '/dev/null' or FATAL "Can't read /dev/null: $!\n";
	open STDOUT, "|-", "logger -s -t $log_ident" or FATAL "Can't open logger output stream: $!\n";
	open STDERR, '>&STDOUT' or FATAL "Can't dup stdout: $!\n";
	open $PID, ">>", "$pidfile" or FATAL "Can't open '$pidfile' for writing: $!\n";
	flock($PID, LOCK_EX | LOCK_NB) or FATAL "Unable to lock pidfile '$pidfile': $!\n";
	defined(my $pid = fork) or FATAL "Can't fork: $!\n";
	exit if $pid;
	setsid or FATAL "Can't start a new session: $!\n";
	seek $PID, 0, SEEK_SET;
	truncate $PID, 0;
	printflush $PID "$$\n";
}

sub signal_handler
{
	$shutdown = 1;
}


sub main
{
	openlog($log_ident, $log_opts, $log_facility)
		or die "Error opening syslog: $!\n";

	daemonize($pidfile)
		if($fork == 1);

	$SIG{TERM} = $SIG{INT} = $SIG{QUIT} = $SIG{HUP} = \&signal_handler;

	init_db or FATAL "Error initializing database handlers\n";
	my $rated = 0;
	my $next_del = 10000;

	INFO "Up and running.\n";
	while(!$shutdown)
	{
		$billdbh->ping || init_db;
		$acctdbh->ping || init_db;
		$provdbh->ping || init_db;
		$dupdbh and ($dupdbh->ping || init_db);
		undef($prepaid_costs);

		my @cdrs = ();
		eval {
		    get_unrated_cdrs(\@cdrs);
		};
		if($@) {
			if($DBI::err == 2006) {
				INFO "DB connection gone, retrying...";
				next;
			}
			FATAL "Error getting next bunch of CDRs: " . $@;
		}

        $shutdown and last;

		unless (@cdrs) {
			INFO "No new CDRs to rate, sleep $loop_interval";
			sleep($loop_interval);
			next;
		}

		my $rated_batch = 0;

		eval
		{
			foreach my $cdr (@cdrs)
			{
			    begin_transaction($billdbh,'READ COMMITTED'); #required to avoid contract_balances duplications during catchup
			    lock_contracts($cdr); #whenever we have to catchup/lock multiple contracts, we need to do this at once, at the beginning
			    begin_transaction($provdbh);
			    begin_transaction($acctdbh);
			    begin_transaction($dupdbh);

				INFO "rate cdr #".$cdr->{id}."\n";
				rate_cdr($cdr, $type) && update_cdr($cdr);
				$rated_batch++;

				#we would need a XA/transaction manager for this:
				commit_transaction($billdbh);
				commit_transaction($provdbh);
        		commit_transaction($acctdbh);
				commit_transaction($dupdbh);

				check_shutdown() and last;
			}
		};
		if($@)
		{
		    my $error = $@;
			if(defined $DBI::err)
			{
				INFO "Caught DBI:err ".$DBI::err, "\n";
				if($DBI::err == 2006)
				{
					INFO "DB connection gone, retrying...";
					# disconnect from all of them so transactions are on par
        			eval { rollback_transaction($billdbh); };
        			eval { rollback_transaction($provdbh); };
        			eval { rollback_transaction($acctdbh); };
        			eval { rollback_transaction($dupdbh); };
					$billdbh->disconnect;
					$provdbh->disconnect;
					$acctdbh->disconnect;
					$dupdbh and ($dupdbh->disconnect);
					next;
				}
				if ($DBI::err == 1213) {
					INFO "Transaction concurrency problem, rolling back and retrying...";
        			eval { rollback_transaction($billdbh); };
        			eval { rollback_transaction($provdbh); };
        			eval { rollback_transaction($acctdbh); };
        			eval { rollback_transaction($dupdbh); };
					next;
				}
			}
			eval { rollback_transaction($billdbh); };
			eval { rollback_transaction($provdbh); };
			eval { rollback_transaction($acctdbh); };
			eval { rollback_transaction($dupdbh); };
			FATAL "Error rating CDR batch: " . $error;
		}

		$rated += $rated_batch;
		INFO "$rated CDRs rated so far.\n";

		$shutdown and last;

		if ($rated >= $next_del) {
			$next_del = $rated + 10000;
			while ($sth_delete_old_prepaid->execute > 0) { ; }
		}

		unless(@cdrs >= 5)
		{
			INFO "Less than 5 new CDRs rated, sleep $loop_interval";
			sleep($loop_interval);
			next;
		}
	}

	INFO "Shutting down.\n";

	$sth_get_subscriber_contract_id->finish;
	$sth_billing_info_v4->finish;
	$sth_billing_info_v6->finish;
	$sth_billing_info_panel->finish;
	$sth_profile_info->finish;
	$sth_offpeak_weekdays->finish;
	$sth_offpeak_special->finish;
	$sth_unrated_cdrs->finish;
	$sth_update_cdr->finish;
	$sth_provider_info->finish;
	$sth_reseller_info->finish;
	$sth_get_cbalances->finish;
	$sth_update_cbalance_w_underrun_profiles_lock->finish;
    $sth_update_cbalance_w_underrun_lock->finish;
    $sth_update_cbalance_w_underrun_profiles->finish;
	$sth_update_cbalance->finish;
	$sth_new_cbalance->finish;
	$sth_new_cbalance_infinite_future->finish;
	$sth_get_last_cbalance->finish;
	$sth_get_first_cbalance->finish;
	$sth_get_last_topup_cbalance->finish;
	$sth_lnp_number->finish;
	$sth_lnp_profile_info->finish;
	$sth_get_contract_info->finish;
	$sth_prepaid_costs->finish;
	$sth_delete_prepaid_cost->finish;
	$sth_delete_old_prepaid->finish;
    $sth_get_billing_voip_subscribers->finish;
    $sth_get_package_profile_sets->finish;
    $sth_create_billing_mappings->finish;
    $sth_get_provisioning_voip_subscribers->finish;
    $sth_get_usr_preference_attribute->finish;
    $sth_get_usr_preference_value->finish;
    $sth_create_usr_preference_value->finish;
    $sth_update_usr_preference_value->finish;
    $sth_delete_usr_preference_value->finish;
	$sth_duplicate_cdr and $sth_duplicate_cdr->finish;


	$billdbh->disconnect;
	$provdbh->disconnect;
	$acctdbh->disconnect;
	$dupdbh and $dupdbh->disconnect;
	closelog;
	close $PID;
	unlink $pidfile;
}
