#!/usr/bin/perl -w
use lib '/usr/share/ngcp-rate-o-mat';
use strict;
use warnings;

use DBI;
use POSIX qw(setsid mktime);
use Fcntl qw(LOCK_EX LOCK_NB SEEK_SET);
use IO::Handle;
use Sys::Syslog;
use NetAddr::IP;
use Data::Dumper;
use Time::HiRes qw(); #for debugging info only
use List::Util qw(shuffle);

# constants: ###########################################################

$0 = 'rate-o-mat'; ## no critic (Variables::RequireLocalizedPunctuationVars)
my $fork = $ENV{RATEOMAT_DAEMONIZE} // 1;
my $PID;
my $pidfile = '/var/run/rate-o-mat.pid';
my $type = 'call';
my $loop_interval = ((defined $ENV{RATEOMAT_LOOP_INTERVAL} && $ENV{RATEOMAT_LOOP_INTERVAL}) ? int $ENV{RATEOMAT_LOOP_INTERVAL} : 10);
my $debug = ((defined $ENV{RATEOMAT_DEBUG} && $ENV{RATEOMAT_DEBUG}) ? int $ENV{RATEOMAT_DEBUG} : 0);

my $log_ident = 'rate-o-mat';
my $log_facility = 'daemon';
my $log_opts = 'ndely,cons,pid,nowait';

# number of unrated cdrs to fetch at once:
my $batch_size = ((defined $ENV{RATEOMAT_BATCH_SIZE} && $ENV{RATEOMAT_BATCH_SIZE} > 0) ? int $ENV{RATEOMAT_BATCH_SIZE} : 100);

# if rate-o-mat processes are working on the same accounting.cdr table:
# set to 1 to minimize collisions (and thus rollbacks)
my $shuffle_batch = ((defined $ENV{RATEOMAT_SHUFFLE_BATCH} && $ENV{RATEOMAT_SHUFFLE_BATCH}) ? int $ENV{RATEOMAT_SHUFFLE_BATCH} : 0);

# preload the whole prepaid_costs table, if number of records
# is below this limit:
my $prepaid_costs_cache_limit = ((defined $ENV{RATEOMAT_PREPAID_COSTS_CACHE} && $ENV{RATEOMAT_PREPAID_COSTS_CACHE} > 0) ? int $ENV{RATEOMAT_PREPAID_COSTS_CACHE} : 10000);

# if the LNP database is used not just for LNP, but also for on-net
# billing, special routing or similar things, this should be set to
# better guess the correct LNP provider ID when selecting ported numbers
# e.g.:
# my @lnp_order_by = ("lnp_provider_id ASC");
my @lnp_order_by = ();

# if split_peak_parts is set to true, rate-o-mat will create a separate
# CDR every time a peak time border is crossed for either the customer,
# the reseller or the carrier billing profile.
my $split_peak_parts = ((defined $ENV{RATEOMAT_SPLIT_PEAK_PARTS} && $ENV{RATEOMAT_SPLIT_PEAK_PARTS}) ? int $ENV{RATEOMAT_SPLIT_PEAK_PARTS} : 0);

# update subscriber prepaid attribute value upon profile mapping updates:
my $update_prepaid_preference = 1;

# don't update balance of prepaid contracts, if no prepaid_costs record is found (re-rating):
my $prepaid_update_balance = 0;

# control writing cdr relation data:
# disable it for now until this will be limited to prepaid contracts,
# as it produces massive amounts of zeroed or unneeded data.
my $write_cash_balance_before_after = 0;
my $write_free_time_balance_before_after = 0;
my $write_profile_package_id = 0;
my $write_contract_balance_id = 0;

# terminate if the same cdr fails $failed_cdr_max_retries + 1 times:
my $failed_cdr_max_retries = ((defined $ENV{RATEOMAT_MAX_RETRIES} && $ENV{RATEOMAT_MAX_RETRIES} >= 0) ? int $ENV{RATEOMAT_MAX_RETRIES} : 2);
my $failed_cdr_retry_delay = ((defined $ENV{RATEOMAT_RETRY_DELAY} && $ENV{RATEOMAT_RETRY_DELAY} >= 0) ? int $ENV{RATEOMAT_RETRY_DELAY} : 30);
# with 2 retries and 30sec delay, rato-o-mat tolerates a replication
# lag of around 60secs until it terminates.

# pause between db connect attempts:
my $connect_interval = 3;

my $maintenance_mode = $ENV{RATEOMAT_MAINTENANCE} // 'no';

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
my $ProvDB_User = $ENV{RATEOMAT_PROVISIONING_DB_USER};
my $ProvDB_Pass = $ENV{RATEOMAT_PROVISIONING_DB_PASS};
# duplication database
my $DupDB_Name = $ENV{RATEOMAT_DUPLICATE_DB_NAME} || 'accounting';
my $DupDB_Host = $ENV{RATEOMAT_DUPLICATE_DB_HOST} || 'localhost';
my $DupDB_Port = $ENV{RATEOMAT_DUPLICATE_DB_PORT} ? int $ENV{RATEOMAT_DUPLICATE_DB_PORT} : 3306;
my $DupDB_User = $ENV{RATEOMAT_DUPLICATE_DB_USER};
my $DupDB_Pass = $ENV{RATEOMAT_DUPLICATE_DB_PASS};

my @cdr_fields = qw(source_user_id source_provider_id source_external_subscriber_id source_external_contract_id source_account_id source_user source_domain source_cli source_clir source_ip source_lnp_prefix source_user_out destination_user_id destination_provider_id destination_external_subscriber_id destination_external_contract_id destination_account_id destination_user destination_domain destination_user_dialed destination_user_in destination_domain_in destination_lnp_prefix destination_user_out peer_auth_user peer_auth_realm call_type call_status call_code init_time start_time duration call_id source_carrier_cost source_reseller_cost source_customer_cost source_carrier_free_time source_reseller_free_time source_customer_free_time source_carrier_billing_fee_id source_reseller_billing_fee_id source_customer_billing_fee_id source_carrier_billing_zone_id source_reseller_billing_zone_id source_customer_billing_zone_id destination_carrier_cost destination_reseller_cost destination_customer_cost destination_carrier_free_time destination_reseller_free_time destination_customer_free_time destination_carrier_billing_fee_id destination_reseller_billing_fee_id destination_customer_billing_fee_id destination_carrier_billing_zone_id destination_reseller_billing_zone_id destination_customer_billing_zone_id frag_carrier_onpeak frag_reseller_onpeak frag_customer_onpeak is_fragmented split rated_at rating_status exported_at export_status source_lnp_type destination_lnp_type);
foreach my $gpp_idx(0 .. 9) {
	push @cdr_fields, ("source_gpp$gpp_idx", "destination_gpp$gpp_idx");
}

my $acc_cash_balance_col_model_key = 1;
my $acc_time_balance_col_model_key = 2;
my $acc_relation_col_model_key = 3;

my $dup_cash_balance_col_model_key = 4;
my $dup_time_balance_col_model_key = 5;
my $dup_relation_col_model_key = 6;

# globals: #############################################################

my $shutdown = 0;
my $prepaid_costs_cache;
my %cdr_col_models = ();
my $rollback;
my $log_fatal = 1;

# stmt handlers: #######################################################

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
my $sth_create_cdr_fragment;
my $sth_get_cbalances;
my $sth_update_cbalance_w_underrun_profiles_lock;
my $sth_update_cbalance_w_underrun_lock;
my $sth_update_cbalance_w_underrun_profiles;
my $sth_update_cbalance;
my $sth_new_cbalance;
my $sth_new_cbalance_infinite_future;
my $sth_get_last_cbalance;
my $sth_get_cbalance;
my $sth_get_first_cbalance;
my $sth_get_last_topup_cbalance,
my $sth_lnp_number;
my $sth_lnp_profile_info;
my $sth_prepaid_costs_cache;
my $sth_prepaid_costs_count;
my $sth_prepaid_cost;
my $sth_delete_prepaid_cost;
my $sth_delete_old_prepaid;
my $sth_get_billing_voip_subscribers;
my $sth_get_package_profile_sets;
my $sth_create_billing_mappings;
my $sth_lock_billing_subscribers;
my $sth_unlock_billing_subscribers;
my $sth_get_provisioning_voip_subscribers;
my $sth_get_usr_preference_attribute;
my $sth_get_usr_preference_value;
my $sth_create_usr_preference_value;
my $sth_update_usr_preference_value;
my $sth_delete_usr_preference_value;
my $sth_duplicate_cdr;

# run the main loop: ##################################################

main();
exit 0;

# implementation: ######################################################

sub FATAL {
	my $msg = shift;
	chomp $msg;
	print "FATAL: $msg\n" if($fork != 1);
	syslog('crit', $msg) if $log_fatal;
	die "$msg\n";

}

sub DEBUG {

	return unless $debug;
	my $msg = shift;
	$msg = &$msg() if 'CODE' eq ref $msg;
	chomp $msg;
	$msg =~ s/#012 +/ /g;
	print "DEBUG: $msg\n" if($fork != 1);
	syslog('debug', $msg);

}

sub INFO {

	my $msg = shift;
	chomp $msg;
	print "INFO: $msg\n" if($fork != 1);
	syslog('info', $msg);

}

sub WARNING {

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

sub set_start_strtime {

	my $start = shift;
	my $r_str = shift;

	$$r_str = sql_time($start);
	return 0;

}

sub connect_billdbh {

	do {
		INFO "Trying to connect to billing db...";
		$billdbh = DBI->connect("dbi:mysql:database=$BillDB_Name;host=$BillDB_Host;port=$BillDB_Port", $BillDB_User, $BillDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $billdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($billdbh);
	INFO "Successfully connected to billing db...";

}

sub connect_acctdbh {

	do {
		INFO "Trying to connect to accounting db...";
		$acctdbh = DBI->connect("dbi:mysql:database=$AcctDB_Name;host=$AcctDB_Host;port=$AcctDB_Port", $AcctDB_User, $AcctDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $acctdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($acctdbh);
	INFO "Successfully connected to accounting db...";

}

sub connect_provdbh {

	unless ($ProvDB_User) {
		undef $dupdbh;
		WARNING "No provisioning db credentials, disabled.";
		return;
	}

	do {
		INFO "Trying to connect to provisioning db...";
		$provdbh = DBI->connect("dbi:mysql:database=$ProvDB_Name;host=$ProvDB_Host;port=$ProvDB_Port", $ProvDB_User, $ProvDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $provdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($provdbh);
	INFO "Successfully connected to provisioning db...";

}

sub connect_dupdbh {

	unless ($DupDB_User) {
		undef $dupdbh;
		WARNING "No duplication db credentials, disabled.";
		return;
	}

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

sub rollback_all {

	eval { rollback_transaction($billdbh); };
	eval { rollback_transaction($provdbh); };
	eval { rollback_transaction($acctdbh); };
	eval { rollback_transaction($dupdbh); };

}

sub bigint_to_bytes {

	my ($bigint,$size) = @_;
	return pack('C' x $size, map { hex($_) } (sprintf('%0' . 2 * $size . 's',substr($bigint->as_hex(),2)) =~ /(..)/g));

}

sub is_infinite_unix {

	my $unix_ts = shift;
	return 1 unless defined $unix_ts; #internally, we use undef for infinite future
	return $unix_ts == 0 ? 1 : 0; #If you pass an out-of-range date to UNIX_TIMESTAMP(), it returns 0

}

sub last_day_of_month {

	my $t = shift;
	my ($month,$year) = (localtime($t))[4,5];
	$month++;
	$year += 1900;
	if (1 == $month || 3 == $month || 5 == $month || 7 == $month || 8 == $month || 10 == $month || 12 == $month) {
		return 31;
	} elsif (2 == $month) {
		my $is_leap_year = 0;
		if ($year % 4 == 0) {
			$is_leap_year = 1;
		}
		if ($year % 100 == 0) {
			$is_leap_year = 0;
		}
		if ($year % 400 == 0) {
			$is_leap_year = 1;
		}
		if ($is_leap_year) {
			return 29;
		} else {
			return 28;
		}
	} else {
		return 30;
	}

}

sub init_db {

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

	$sth_get_cbalance = $billdbh->prepare(
		"SELECT id, UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end), cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, topup_count, timely_topup_count ".
		"FROM billing.contract_balances ".
		"WHERE id = ?"
	) or FATAL "Error preparing get last contract balance statement: ".$billdbh->errstr;

	$sth_get_first_cbalance = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(start),UNIX_TIMESTAMP(end) ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? ".
		"ORDER BY start ASC LIMIT 1"
	) or FATAL "Error preparing get first contract balance statement: ".$billdbh->errstr;
	$sth_get_last_topup_cbalance = $billdbh->prepare(
		"SELECT UNIX_TIMESTAMP(start),UNIX_TIMESTAMP(end) ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"topup_count > 0 ".
		"ORDER BY end DESC LIMIT 1"
	) or FATAL "Error preparing get last topup contract balance statement: ".$billdbh->errstr;

	$sth_get_subscriber_contract_id = $billdbh->prepare(
		"SELECT contract_id FROM billing.voip_subscribers WHERE uuid = ?"
	) or FATAL "Error preparing subscriber contract id statement: ".$billdbh->errstr;

	$sth_billing_info_v4 = $billdbh->prepare(<<EOS
		SELECT b.billing_profile_id, b.product_id, p.class, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		LEFT JOIN billing.products p ON b.product_id = p.id
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
		SELECT b.billing_profile_id, b.product_id, p.class, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		LEFT JOIN billing.products p ON b.product_id = p.id
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
		SELECT b.billing_profile_id, b.product_id, p.class, d.prepaid,
			d.interval_charge, d.interval_free_time, d.interval_free_cash,
			d.interval_unit, d.interval_count
		FROM billing.billing_mappings b
		JOIN billing.billing_profiles d ON b.billing_profile_id = d.id
		LEFT JOIN billing.products p ON b.product_id = p.id
		WHERE b.contract_id = ?
		AND ( b.start_date IS NULL OR b.start_date <= FROM_UNIXTIME(?) )
		AND ( b.end_date IS NULL OR b.end_date >= FROM_UNIXTIME(?) )
		ORDER BY b.start_date DESC, b.id DESC
		LIMIT 1
EOS
	) or FATAL "Error preparing panel billing info statement: ".$billdbh->errstr;

	$sth_lnp_number = $billdbh->prepare(<<EOS
		SELECT lnp_provider_id
		  FROM billing.lnp_numbers
		 WHERE ? LIKE CONCAT(number,'%')
		   AND (start <= FROM_UNIXTIME(?) OR start IS NULL)
		   AND (end > FROM_UNIXTIME(?) OR end IS NULL)
EOS
		. join(", ", "ORDER BY LENGTH(number) DESC", @lnp_order_by) .
		" LIMIT 1"
	) or FATAL "Error preparing LNP number statement: ".$billdbh->errstr;

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
		"ORDER BY start_time ASC LIMIT " . $batch_size
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
		"destination_carrier_billing_zone_id = ?, destination_reseller_billing_zone_id = ?, destination_customer_billing_zone_id = ?, ".
		"frag_carrier_onpeak = ?, frag_reseller_onpeak = ?, frag_customer_onpeak = ?, is_fragmented = ?, ".
		"duration = ? ".
		"WHERE id = ? AND rating_status = 'unrated'"
	) or FATAL "Error preparing update cdr statement: ".$acctdbh->errstr;

	if ($split_peak_parts) {
		my @exclude_fragment_fields = qw(start_time duration is_fragmented);
		my %exclude_fragment_fields = map { $_ => 1 } @exclude_fragment_fields;
		my @fragment_fields = grep {!$exclude_fragment_fields{$_}} @cdr_fields;
		$sth_create_cdr_fragment = $acctdbh->prepare(
			"INSERT INTO accounting.cdr (".
			join(',', @fragment_fields, @exclude_fragment_fields).
			") SELECT ".
			join(',', @fragment_fields). ", " .
			"start_time + ?,duration - ?,1 " .
			"FROM accounting.cdr " .
			"WHERE id = ? AND rating_status = 'unrated'"
		) or FATAL "Error preparing create cdr fragment statement: ".$acctdbh->errstr;
	}

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
		" contract_id, cash_balance, initial_cash_balance, cash_balance_interval, free_time_balance, initial_free_time_balance, free_time_balance_interval, underrun_profiles, underrun_lock, start, end".
		") VALUES (?, ?, ?, ?, ?, ?, ?, IF(? = 0, NULL, FROM_UNIXTIME(?)), IF(? = 0, NULL, FROM_UNIXTIME(?)), FROM_UNIXTIME(?), FROM_UNIXTIME(?))"
	) or FATAL "Error preparing create contract balance statement: ".$billdbh->errstr;

	$sth_new_cbalance_infinite_future = $billdbh->prepare(
		"INSERT INTO billing.contract_balances (".
		" contract_id, cash_balance, initial_cash_balance, cash_balance_interval, free_time_balance, initial_free_time_balance, free_time_balance_interval, underrun_profiles, underrun_lock, start, end".
		") VALUES (?, ?, ?, ?, ?, ?, ?, IF(? = 0, NULL, FROM_UNIXTIME(?)), IF(? = 0, NULL, FROM_UNIXTIME(?)), FROM_UNIXTIME(?), '9999-12-31 23:59:59')"
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

	$sth_prepaid_costs_cache = $acctdbh->prepare(
		"SELECT * FROM accounting.prepaid_costs order by timestamp asc" # newer entries overwrite older ones
	) or FATAL "Error preparing prepaid costs cache statement: ".$acctdbh->errstr;

	$sth_prepaid_costs_count = $acctdbh->prepare(
		"SELECT count(cnt.id) FROM (SELECT id FROM accounting.prepaid_costs LIMIT " . ($prepaid_costs_cache_limit + 1) . ") AS cnt"
	) or FATAL "Error preparing prepaid costs count statement: ".$acctdbh->errstr;

	$sth_prepaid_cost = $acctdbh->prepare( #call_id index required
		'SELECT * FROM accounting.prepaid_costs WHERE call_id = ? ' .
		'AND source_user_id = ? AND destination_user_id = ?' .
		'ORDER BY timestamp ASC' # newer entries overwrite older ones
	) or FATAL "Error preparing prepaid cost statement: ".$acctdbh->errstr;

	$sth_delete_prepaid_cost = $acctdbh->prepare( #call_id index required
		'DELETE FROM accounting.prepaid_costs WHERE call_id = ? ' .
		'AND source_user_id = ? AND destination_user_id = ?'
	) or FATAL "Error preparing delete prepaid costs statement: ".$acctdbh->errstr;

	$sth_delete_old_prepaid = $acctdbh->prepare(
		"DELETE FROM accounting.prepaid_costs WHERE timestamp < DATE_SUB(NOW(), INTERVAL 7 DAY) LIMIT 10000"
	) or FATAL "Error preparing delete old prepaid statement: ".$acctdbh->errstr;

	$sth_get_billing_voip_subscribers = $billdbh->prepare(
		"SELECT uuid FROM billing.voip_subscribers WHERE contract_id = ? AND status != 'terminated'"
	) or FATAL "Error preparing get billing voip subscribers statement: ".$billdbh->errstr;

	$sth_get_package_profile_sets = $billdbh->prepare(
		"SELECT profile_id, network_id FROM billing.package_profile_sets WHERE package_id = ? AND discriminator = ?"
	) or FATAL "Error preparing get package profile sets statement: ".$billdbh->errstr;

	$sth_create_billing_mappings = $billdbh->prepare(
		"INSERT INTO billing.billing_mappings (contract_id, billing_profile_id, network_id, product_id, start_date) VALUES (?, ?, ?, ?, FROM_UNIXTIME(?))"
	) or FATAL "Error preparing create billing mappings statement: ".$billdbh->errstr;

	$sth_lock_billing_subscribers = $billdbh->prepare(
		"UPDATE billing.voip_subscriber SET status = 'locked' WHERE contract_id = ? AND status = 'active'"
	) or FATAL "Error preparing lock billing subscribers statement: ".$billdbh->errstr;

	$sth_unlock_billing_subscribers = $billdbh->prepare(
		"UPDATE billing.voip_subscriber SET status = 'active' WHERE contract_id = ? AND status = 'locked'"
	) or FATAL "Error preparing lock billing subscribers statement: ".$billdbh->errstr;

	if ($provdbh) {
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
	}

	prepare_cdr_col_models($acctdbh,
		$acc_cash_balance_col_model_key,
		$acc_time_balance_col_model_key,
		$acc_relation_col_model_key,
		'local');

	if ($dupdbh) {
		$sth_duplicate_cdr = $dupdbh->prepare(
			'insert into cdr ('.
			join(',', @cdr_fields).
			') values ('.
			join(',', (map {'?'} @cdr_fields)).
			')'
		) or FATAL "Error preparing duplicate_cdr statement: ".$dupdbh->errstr;

		prepare_cdr_col_models($dupdbh,
		$dup_cash_balance_col_model_key,
		$dup_time_balance_col_model_key,
		$dup_relation_col_model_key,
		'duplication');
	}

	return 1;

}

sub prepare_cdr_col_models {

	my $dbh = shift;
	my $cash_balance_col_model_key = shift;
	my $time_balance_col_model_key = shift;
	my $relation_col_model_key = shift;
	my $description_prefix = shift;

	prepare_cdr_col_model($dbh,$cash_balance_col_model_key,$description_prefix.' cdr cash balance column model',$description_prefix,
		[ 'direction', 'provider', 'cash_balance' ], # avoid using Tie::IxHash
		{
			provider => {
				sql => 'SELECT * FROM accounting.cdr_provider',
				description => "get $description_prefix cdr provider cols",
			},
			direction => { # the name "direction" for "source" and "destination" is not ideal
				sql => 'SELECT * FROM accounting.cdr_direction',
				description => "get $description_prefix cdr direction cols",
			},
			cash_balance => {
				sql => 'SELECT * FROM accounting.cdr_cash_balance',
				description => "get $description_prefix cdr cash balance cols",
			},
		},{
			sql => "INSERT INTO accounting.cdr_cash_balance_data".
				"  (cdr_id,cdr_start_time,direction_id,provider_id,cash_balance_id,val_before,val_after) VALUES".
				"  (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val_before = ?, val_after = ?",
			description => "write $description_prefix cdr cash balance col data",
		}
	);

	prepare_cdr_col_model($dbh,$time_balance_col_model_key,$description_prefix.' cdr time balance column model',$description_prefix,
		[ 'direction', 'provider', 'time_balance' ],
		{
			provider => {
				sql => 'SELECT * FROM accounting.cdr_provider',
				description => "get $description_prefix cdr provider cols",
			},
			direction => {
				sql => 'SELECT * FROM accounting.cdr_direction',
				description => "get $description_prefix cdr direction cols",
			},
			time_balance => {
				sql => 'SELECT * FROM accounting.cdr_time_balance',
				description => "get $description_prefix cdr time balance cols",
			},
		},{
			sql => "INSERT INTO accounting.cdr_time_balance_data".
				"  (cdr_id,cdr_start_time,direction_id,provider_id,time_balance_id,val_before,val_after) VALUES".
				"  (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val_before = ?, val_after = ?",
			description => "write $description_prefix cdr time balance col data",
		}
	);

	prepare_cdr_col_model($dbh,$relation_col_model_key,$description_prefix.' cdr relation column model',$description_prefix,
		[ 'direction', 'provider', 'relation' ],
		{
			provider => {
				sql => 'SELECT * FROM accounting.cdr_provider',
				description => "get $description_prefix cdr provider cols",
			},
			direction => {
				sql => 'SELECT * FROM accounting.cdr_direction',
				description => "get $description_prefix cdr direction cols",
			},
			relation => {
				sql => 'SELECT * FROM accounting.cdr_relation',
				description => "get $description_prefix relation cols",
			},
		},{
			sql => "INSERT INTO accounting.cdr_relation_data".
				"  (cdr_id,cdr_start_time,direction_id,provider_id,relation_id,val) VALUES".
				"  (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val = ?",
			description => "write $description_prefix cdr relation col data",
		}
	);

}

sub lock_contracts {

	my $cdr = shift;
	# we lock all contracts when rating a single CDR, which will
	# eventually need a contract_balances catchup. that are up to 4.
	# there must be a single 'for update' select statement to lock
	# the contracts all at once, otherwise deadlock situations are
	# guaranteed. this final lock statement must avoid joins, otherwise
	# all rows of joined tables can get locked, since innodb poorly
	# locks rows by touching an index value. to prepare the lock
	# statement, we need to determine the 4 contract ids sperately
	# before:
	my %provider_cids = ();
	# caller "provider" contract:
	$provider_cids{$cdr->{source_provider_id}} = 1 if $cdr->{source_provider_id} ne "0";
	# callee "provider" contract:
	$provider_cids{$cdr->{destination_provider_id}} = 1 if $cdr->{destination_provider_id} ne "0";
	my @pcids = keys %provider_cids;
	my $pcid_count = scalar @pcids;
	my $sth = undef;
	my %lock_cids = ();
	if ($pcid_count > 0) {
		$sth = $billdbh->prepare("SELECT c.id from billing.contracts c ".
			"WHERE c.id IN (" . substr(',?' x $pcid_count,1) . ")")
			 or FATAL "Error preparing contract row lock selection statement: ".$billdbh->errstr;
		$sth->execute(@pcids)
		 	 or FATAL "Error executing contract row lock selection statement: ".$sth->errstr;
		while (my @res = $sth->fetchrow_array) {
			$lock_cids{$res[0]} = 1;
		}
		$sth->finish;
	}
	my %user_ids = ();
	# callee subscriber contract:
	WARNING "empty source_user_id for CDR ID $cdr->{id}" unless length($cdr->{source_user_id}) > 0;
	$user_ids{$cdr->{source_user_id}} = 1 if $cdr->{source_user_id} ne "0";
	# (onnet) caller subscriber:
	WARNING "empty destination_user_id for CDR ID $cdr->{id}" unless length($cdr->{destination_user_id}) > 0;
	$user_ids{$cdr->{destination_user_id}} = 1 if $cdr->{destination_user_id} ne "0";
	my @uuids = keys %user_ids;
	my $uuid_count = scalar @uuids;
	if ($uuid_count > 0) {
		$sth = $billdbh->prepare("SELECT DISTINCT c.id from billing.contracts c ".
			" JOIN billing.voip_subscribers s ON c.id = s.contract_id ".
			"WHERE s.uuid IN (" . substr(',?' x $uuid_count,1) . ")")
			 or FATAL "Error preparing subscriber contract row lock selection statement: ".$billdbh->errstr;
		$sth->execute(@uuids)
		     or FATAL "Error executing subscriber contract row lock selection statement: ".$sth->errstr;
		while (my @res = $sth->fetchrow_array) {
			$lock_cids{$res[0]} = 1;
		}
		$sth->finish;
	}
	my @cids = keys %lock_cids;
	my $lock_count = scalar @cids;
	if ($lock_count > 0) {
		@cids = sort { $a <=> $b } @cids; #"Access your tables and rows in a fixed order."
		my $sth = $billdbh->prepare("SELECT c.id from billing.contracts c ".
			"WHERE c.id IN (" . substr(',?' x $lock_count,1) . ") FOR UPDATE")
			 or FATAL "Error preparing contract row lock statement: ".$billdbh->errstr;
		#finally lock the contract rows at this point:
		$sth->execute(@cids)
		     or FATAL "Error executing contract row lock statement: ".$sth->errstr;
		$sth->finish;
		DEBUG "$lock_count contract(s) locked: ".join(', ',@cids);
	}

	return $lock_count;

}

sub add_interval {

	my ($unit,$count,$from_time,$align_eom_time,$src) = @_;
	my $to_time;
	my ($from_year,$from_month,$from_day,$from_hour,$from_minute,$from_second) = (localtime($from_time))[5,4,3,2,1,0];
	if($unit eq "minute") {
		$to_time = mktime($from_second,$from_minute + $count,$from_hour,$from_day,$from_month,$from_year);
	} elsif($unit eq "hour") {
		$to_time = mktime($from_second,$from_minute,$from_hour + $count,$from_day,$from_month,$from_year);
	} elsif($unit eq "day") {
		$to_time = mktime($from_second,$from_minute,$from_hour,$from_day + $count,$from_month,$from_year);
	} elsif($unit eq "week") {
		$to_time = mktime($from_second,$from_minute,$from_hour,$from_day + 7*$count,$from_month,$from_year);
	} elsif($unit eq "month") {
		$to_time = mktime($from_second,$from_minute,$from_hour,$from_day,$from_month + $count,$from_year);
		#DateTime's "preserve" mode would get from 30.Jan to 30.Mar, when adding 2 months
		#When adding 1 month two times, we get 28.Mar or 29.Mar, so we adjust:
		if (defined $align_eom_time) {
			my $align_eom_day = (localtime($align_eom_time))[3]; #local or not is irrelavant here
			my $to_day = (localtime($to_time))[3]; #local or not is irrelavant here
			if ($to_day > $align_eom_day
				&& $from_day == last_day_of_month($from_time)) {
				my $delta = last_day_of_month($align_eom_time) - $align_eom_day;
				$to_day = last_day_of_month($to_time) - $delta;
				$to_time = mktime($from_second,$from_minute,$from_hour,$to_day,$from_month,$from_year);
			}
		}
	} else {
		die("Invalid interval unit '$unit' in $src");
	}
	return $to_time;

}

sub truncate_day {

	my $t = shift;
	my ($year,$month,$day,$hour,$minute,$second) = (localtime($t))[5,4,3,2,1,0];
	return mktime(0,0,0,$day,$month,$year);

}

sub set_subscriber_first_int_attribute_value {

	my $contract_id = shift;
	my $new_value = shift;
	my $attribute = shift;
	my $readonly = shift;

	my $changed = 0;
	my $attr_id = undef;
	my $sth;

	unless ($sth_get_provisioning_voip_subscribers &&
		$sth_get_usr_preference_attribute &&
		$sth_get_usr_preference_value &&
		$sth_create_usr_preference_value &&
		$sth_update_usr_preference_value &&
		$sth_delete_usr_preference_value) {
		return $changed;
	}

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
				if ($readonly) {
					if ($old_value != $new_value) {
                        WARNING "'$attribute' usr preference value ID $val_id should be '$new_value' instead of '$old_value'";
                    } else {
						DEBUG "'$attribute' usr preference value ID $val_id value '$new_value' is correct";
					}
				} else {
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
				}
			} elsif ($new_value > 0) {
				if ($readonly) {
                    WARNING "'$attribute' usr preference value '$new_value' missing for prov subscriber ID $prov_subs_id";
				} else {
					$sth = $sth_create_usr_preference_value;
					$sth->execute($prov_subs_id,$attr_id,$new_value)
						or FATAL "Error executing create usr preference value statement: ".$sth->errstr;
					$changed++;
					DEBUG "'$attribute' usr preference value ID ".$provdbh->{'mysql_insertid'}." with value '$new_value' created";
				}
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
	my $readonly = shift;

	return set_subscriber_first_int_attribute_value($contract_id,$lock_level // 0,'lock',$readonly);

}

sub set_subscriber_status {

	my $contract_id = shift;
	my $lock_level = shift; #int
	my $readonly = shift;

	my $changed = 0;
	my $sth;

	if ($readonly) {
		#todo: warn about billing subscriber discrepancies
	} else {
		if (defined $lock_level && $lock_level > 0) {
			$sth = $sth_lock_billing_subscribers;
			$changed = $sth->execute($contract_id);
			if ($changed) {
				DEBUG "status of $changed billing subscriber(s) set to 'locked'";
			}
		} else {
			$sth = $sth_unlock_billing_subscribers;
			$changed = $sth->execute($contract_id);
			if ($changed) {
				DEBUG "status of $changed billing subscriber(s) set to 'active'";
			}
		}
	}
	$sth->finish if $sth;
	return $changed;

}

sub switch_prepaid {

	my $contract_id = shift;
	my $prepaid = shift; #int
	my $readonly = shift;

	return set_subscriber_first_int_attribute_value($contract_id,($prepaid ? 1 : 0),'prepaid',$readonly);

}

sub add_profile_mappings {

	my $contract_id = shift;
	my $stime = shift;
	my $package_id = shift;
	my $profiles = shift;
	my $readonly = shift;

	my $mappings_added = 0;
	my $profile_id;
	my $network_id;
	my $now = time;
	my $profile = undef;

	$sth_get_package_profile_sets->execute($package_id,$profiles)
		or FATAL "Error executing get package profile sets statement: ".$sth_get_package_profile_sets->errstr;

	while (my @res = $sth_get_package_profile_sets->fetchrow_array) {
		($profile_id,$network_id) = @res;
		if ($readonly) {
            DEBUG "Adding profile mappings skipped";
        } else {
			unless (defined $profile) {
				$profile = {};
				get_billing_info($now, $contract_id, undef, $profile) or
					FATAL "Error getting billing info for date '".$now."' and contract_id $contract_id\n";
			}
			$sth_create_billing_mappings->execute($contract_id,$profile_id,$network_id,$profile->{product_id},$stime)
				or FATAL "Error executing create billing mappings statement: ".$sth_create_billing_mappings->errstr;
			$sth_create_billing_mappings->finish;
			$mappings_added++;
		}
	}
	$sth_get_package_profile_sets->finish;
	if ($update_prepaid_preference && $mappings_added > 0) {
		DEBUG "$mappings_added '$profiles' profile mappings added";
		get_billing_info($now, $contract_id, undef, $profile) or
			FATAL "Error getting billing info for date '".$now."' and contract_id $contract_id\n";
		switch_prepaid($contract_id,$profile->{prepaid},$readonly);
	}

	return $mappings_added;

}

sub get_notopup_expiration {

	my $contract_id = shift;
	my $last_start_time = shift;
	my $notopup_discard_intervals = shift;
	my $interval_unit = shift;
	my $align_eom_time = shift;
	my $package_id = shift;

	my $sth;
	my $notopup_expiration = undef;
	my $last_topup_start_time;
	my $last_topup_end_time;
	if ($notopup_discard_intervals) { #get notopup_expiration:
		if (defined $last_start_time) {
			$last_topup_start_time = $last_start_time;
		} else {
			$sth = $sth_get_last_topup_cbalance;
			$sth->execute($contract_id) or FATAL "Error executing get latest contract balance statement: ".$sth->errstr;
			($last_topup_start_time,$last_topup_end_time) = $sth->fetchrow_array();
			$sth->finish;
			if (!$last_topup_start_time) {
				$sth = $sth_get_first_cbalance;
				$sth->execute($contract_id) or FATAL "Error executing get first contract balance statement: ".$sth->errstr;
				($last_topup_start_time,$last_topup_end_time) = $sth->fetchrow_array();
				$sth->finish;
			}
			if ($last_topup_start_time) {
				if (!is_infinite_unix($last_topup_end_time)) {
					$last_topup_start_time = $last_topup_end_time + 1;
				}
			}
		}
		if ($last_topup_start_time) {
			$notopup_expiration = add_interval($interval_unit, $notopup_discard_intervals,
				$last_topup_start_time, $align_eom_time, "package id " . $package_id);
		}
	}
	return $notopup_expiration;

}

sub get_timely_end {

	my $last_start_time = shift;
	my $interval_value = shift;
	my $interval_unit = shift;
	#my $align_eom = shift;
	my $carry_over_mode = shift;
	my $package_id = shift;

	my $timely_end_time = undef;

	if ("carry_over_timely" eq $carry_over_mode) {
		$timely_end_time = add_interval($interval_unit, $interval_value,
					$last_start_time, undef, "package id " . $package_id);
		$timely_end_time--;
	}

	return $timely_end_time;

}

sub catchup_contract_balance {

	my $call_start_time = shift;
	my $call_end_time = shift;
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
	my $align_eom_time;
	if ("create" eq $start_mode && defined $create_time) {
		$align_eom_time = $create_time;
	} #no eom preserve, since we don't have the begin of the first topup interval
	#} elsif ("topup_interval" eq $start_mode && defined x) {
	#    $align_eom_time = x;
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
	my $timely_end = 0;
	my $now = time;
	my $bal;

	while (defined $last_id && !is_infinite_unix($last_end) && $last_end < $call_end_time) {
		$next_start = $last_end + 1;

		if ($has_package && $balances_count == 0) {
			#we have two queries here, so do it only if really creating contract_balances
			$notopup_expiration = get_notopup_expiration($contract_id,undef,$notopup_discard_intervals,$interval_unit,$align_eom_time,$package_id);
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

		$stime = $next_start;
		if ("topup" eq $start_mode) {
			$etime = undef;
		} else {
			$etime = add_interval($interval_unit, $interval_value, $next_start, $align_eom_time, $has_package ? "package id " . $package_id : "profile id " . $profile->{profile_id});
			$etime--;
		}

		#balance values:
		$cash_balance = 0;
		if (("carry_over" eq $carry_over_mode || ("carry_over_timely" eq $carry_over_mode && $last_timely_topups > 0))
			&& (!$notopup_expiration || $stime < $notopup_expiration)) {

			$ratio = 1.0;
			if($create_time > $last_start and $create_time < $last_end) {
				$create_time_aligned = truncate_day($create_time);
				$create_time_aligned = $create_time if $create_time_aligned < $last_start;
				$ratio = ($last_end + 1 - $create_time_aligned) / ($last_end + 1 - $last_start);
			}
			DEBUG "last ratio: $ratio";
			#take the previous interval's (old) free cash, e.g. 5euro:
			$old_free_cash = $ratio * ($last_profile->{int_free_cash} // 0.0);
			#carry over the last cash balance value, e.g. 23euro:
			$cash_balance = $last_cash_balance;
			if ($last_cash_balance_int < $old_free_cash) {
				# the customer didn't spent all of the the old free cash, but
				# only e.g. 2euro overall. to get the raw balance, subtract the
				# unused rest of the old free cash, e.g. -3euro.
				$cash_balance += $last_cash_balance_int - $old_free_cash;
			} #the customer spent all free cash
			# new free cash can be added ...
		} else {
			DEBUG "discarding cash balance (mode '$carry_over_mode'".($notopup_expiration ? ", notopup expiration " . $notopup_expiration : "").")";
		}
		$ratio = 1.0;
		$free_cash = $ratio * ($profile->{int_free_cash} // 0.0); #backward-defaults
		$cash_balance += $free_cash; #add new free cash
		$cash_balance_interval = 0.0;

		$free_time = $ratio * ($profile->{int_free_time} // 0);
		$free_time_balance = $free_time; #just set free cash for now
		$free_time_balance_interval = 0;

		if (!$underrun_lock_applied && defined $underrun_lock_threshold && $last_cash_balance >= $underrun_lock_threshold && $cash_balance < $underrun_lock_threshold) {
			$underrun_lock_applied = 1;
			DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun lock threshold $underrun_lock_threshold";
			if (defined $underrun_lock_level) {
				set_subscriber_lock_level($contract_id,$underrun_lock_level,0);
				set_subscriber_status($contract_id,$underrun_lock_level,0);
				$underrun_lock_time = $now;
			}
		}

		if (!$underrun_profiles_applied && defined $underrun_profile_threshold && $last_cash_balance >= $underrun_profile_threshold && $cash_balance < $underrun_profile_threshold) {
			$underrun_profiles_applied = 1;
			DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun profile threshold $underrun_profile_threshold";
			if (add_profile_mappings($contract_id,$stime,$package_id,'underrun',0) > 0) {
				$underrun_profiles_time = $now;
				goto PREPARE_BALANCE_CATCHUP;
			}
		}

		#exec create statement:
		$sth = (defined $etime ? $sth_new_cbalance : $sth_new_cbalance_infinite_future);
		($last_cash_balance,$last_cash_balance_int,$last_free_balance,$last_free_balance_int) =
		(truncate_cash_balance($cash_balance), truncate_cash_balance($cash_balance_interval),
			truncate_free_time_balance($free_time_balance), truncate_free_time_balance($free_time_balance_interval));
		my @bind_parms = ($contract_id,
			($last_cash_balance) x 2,$last_cash_balance_int,($last_free_balance) x 2,$last_free_balance_int,
			((defined $underrun_profiles_time ? $underrun_profiles_time : 0)) x 2,((defined $underrun_lock_time ? $underrun_lock_time : 0)) x 2,$stime);
		push(@bind_parms,$etime) if defined $etime;
		$sth->execute(@bind_parms)
			or FATAL "Error executing new contract balance statement: ".$sth->errstr;
		$sth->finish;
		$balances_count++;

		#reload the contract balance to have mysql's local timezone applied to $last_start, $last_end by UNIX_TIMESTAMP:
		$sth = $sth_get_cbalance;
		$sth->execute($billdbh->{'mysql_insertid'}) or FATAL "Error executing reload contract balance statement: ".$sth->errstr;
		($last_id,$last_start,$last_end,$last_cash_balance,$last_cash_balance_int,$last_free_balance,$last_free_balance_int,$last_topups,$last_timely_topups) = $sth->fetchrow_array();
		$sth->finish;

		$bal = {
			id => $last_id,
			cash_balance => $last_cash_balance,
			cash_balance_interval => $last_cash_balance_int,
			free_time_balance => $last_free_balance,
			free_time_balance_interval => $last_free_balance_int,
			start_unix => $last_start,
			end_unix => $last_end,
			};

		DEBUG sub { "contract balance created: ".(Dumper $bal) };

		$last_profile = $profile;

	}

	# in case of "topup" or "topup_interval" start modes, the current interval end can be
	# infinite and no new contract balances are created. for this infinite end interval,
	# the interval start represents the time the last topup happened in case of "topup".
	# in case of "topup_interval", the interval start represents the contract creation.
	# the cash balance should be discarded when
	#  1. the current/call time is later than than $notopup_discard_intervals periods
	#  after the interval start, or
	#  2. we have the "carry_over_timely" mode, and the current/call time is beyond
	#  the timely end already
	if ($has_package && defined $last_id && is_infinite_unix($last_end)) {
		$notopup_expiration = get_notopup_expiration($contract_id,$last_start,$notopup_discard_intervals,$interval_unit,$align_eom_time,$package_id);
		$timely_end = get_timely_end($last_start,$interval_value,$interval_unit,$carry_over_mode,$package_id);
		if ((defined $notopup_expiration && $call_start_time >= $notopup_expiration)
			|| (defined $timely_end && $call_start_time > $timely_end)) {
			DEBUG "discarding cash balance (mode '$carry_over_mode'".($timely_end ? ", timely end " . $timely_end : "").
				($notopup_expiration ? ", notopup expiration " . $notopup_expiration : "").")";
			$bal = {
				id => $last_id,
				cash_balance => 0,
				cash_balance_interval => $last_cash_balance_int,
				free_time_balance => $last_free_balance,
				free_time_balance_interval => $last_free_balance_int,
				underrun_profile_time => undef,
				underrun_lock_time => undef,
			};
			if (!$underrun_lock_applied && defined $underrun_lock_threshold && $last_cash_balance >= $underrun_lock_threshold && 0.0 < $underrun_lock_threshold) {
				$underrun_lock_applied = 1;
				DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun lock threshold $underrun_lock_threshold";
				if (defined $underrun_lock_level) {
					set_subscriber_lock_level($contract_id,$underrun_lock_level,0);
					set_subscriber_status($contract_id,$underrun_lock_level,0);
					$bal->{underrun_lock_time} = $now;
				}
			}

			if (!$underrun_profiles_applied && defined $underrun_profile_threshold && $last_cash_balance >= $underrun_profile_threshold && 0.0 < $underrun_profile_threshold) {
				$underrun_profiles_applied = 1;
				DEBUG "cash balance was decreased from $last_cash_balance to $cash_balance and dropped below underrun profile threshold $underrun_profile_threshold";
				if (add_profile_mappings($contract_id,$call_start_time,$package_id,'underrun',0) > 0) {
					$underrun_profiles_time = $now;
					$bal->{underrun_profile_time} = $now;
				}
			}
			update_contract_balance([$bal])
				or FATAL "Error updating customer contract balance\n";
		}
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

sub get_contract_balances {

	my $cdr = shift;
	my $contract_id = shift;
	my $r_package_info = shift;
	my $r_balances = shift;

	my $start_time = $cdr->{start_time};
	my $duration = $cdr->{duration};

	catchup_contract_balance(int($start_time),int($start_time + $duration),$contract_id,$r_package_info);

	my $sth = $sth_get_cbalances;
	$sth->execute($contract_id, int($start_time))
		or FATAL "Error executing get contract balance statement: ".$sth->errstr;
	my $res = $sth->fetchall_arrayref({});
	$sth->finish;

	foreach my $bal (@$res) {
		# balances savepoint:
		$bal->{cash_balance_old} = $bal->{cash_balance};
		$bal->{free_time_balance_old} = $bal->{free_time_balance};
		push(@$r_balances,$bal);
	}

	return scalar @$res;

}

sub update_contract_balance {

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

sub get_subscriber_contract_id {

	my $uuid = shift;

	my $sth = $sth_get_subscriber_contract_id;

	$sth->execute($uuid) or
		FATAL "Error executing get_subscriber_contract_id statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();
	FATAL "No contract id found for uuid '$uuid'\n" unless @res;

	return $res[0];

}

sub get_billing_info {

	my $start = shift;
	my $contract_id = shift;
	my $source_ip = shift;
	my $r_info = shift;

	my $label;
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
		my $ip_bytes = bigint_to_bytes($int_ip, $ip_size);

		$sth->execute($contract_id, $start, $start, $ip_bytes, $ip_bytes) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
		$label = "ipv".$ip->version." source ip $source_ip";
	} else {
		$sth = $sth_billing_info_panel;
		$sth->execute($contract_id, $start, $start) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
		$label = "panel";
	}

	my @res = $sth->fetchrow_array();
	FATAL "No billing info found for contract_id $contract_id\n" unless @res;

	$r_info->{contract_id} = $contract_id;
	$r_info->{profile_id} = $res[0];
	$r_info->{product_id} = $res[1];
	$r_info->{class} = $res[2];
	$r_info->{prepaid} = $res[3];
	$r_info->{int_charge} = $res[4];
	$r_info->{int_free_time} = $res[5];
	$r_info->{int_free_cash} = $res[6];
	$r_info->{int_unit} = $res[7];
	$r_info->{int_count} = $res[8];

	DEBUG "contract ID $contract_id billing mapping ($r_info->{class}) is profile id $r_info->{profile_id} for time $start from $label";

	$sth->finish;

	return 1;

}

sub get_profile_info {

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

sub get_offpeak_weekdays {

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

	while(my @res = $sth->fetchrow_array()) {
		my %e = ();
		$e{weekday} = $res[0];
		$e{start} = $res[1];
		$e{end} = $res[2];
		push @$r_offpeaks, \%e;
	}

	return 1;

}

sub get_offpeak_special {

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

sub is_offpeak_special {

	my $start = shift;
	my $offset = shift;
	my $r_offpeaks = shift;

	my $secs = $start + $offset; # we have unix-timestamp as reference

	foreach my $r_o(@$r_offpeaks) {
		return 1 if($secs >= $r_o->{start} && $secs <= $r_o->{end});
	}

	return 0;

}

sub is_offpeak_weekday {

	my $start = shift;
	my $offset = shift;
	my $r_offpeaks = shift;

	my ($S, $M, $H, $d, $m, $y, $wd, $yd, $dst) = localtime($start + $offset);
	$wd = ($wd - 1) % 7; # convert to MySQL notation (mysql: mon=0, unix: mon=1)
	$y += 1900; $m += 1;
	#$H -= 1 if($dst == 1); # regard daylight saving time

	my $secs = $S + $M * 60 + $H * 3600; # we have seconds since midnight as reference
	foreach my $r_o(@$r_offpeaks) {
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

	my @cdrs = ();

	while (my $cdr = $sth->fetchrow_hashref()) {
		push(@cdrs,$cdr);
		check_shutdown() and return 0;
	}

	# the while above may have been interupted because there is no
	# data left, or because there was an error. To decide what
	# happened, we have to query $sth->err()
	FATAL "Error fetching unrated cdr's: ". $sth->errstr
		if $sth->err;
	$sth->finish;

	if ($shuffle_batch) {
		# if concurrent rate-o-mat instances grab the same cdr batch, there
		# can be a contention due to waits on same caller/callee contract
		# lock attempts when they start processing the batch in the same order.
		foreach my $cdr (shuffle @cdrs) {
		    push(@$r_cdrs,$cdr);
		}
    } else {
		@$r_cdrs = @cdrs;
	}

	return 1;

}

sub update_cdr {

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
		$cdr->{frag_carrier_onpeak}, $cdr->{frag_reseller_onpeak}, $cdr->{frag_customer_onpeak},
		$cdr->{is_fragmented} // 0, $cdr->{duration},
		$cdr->{id})
		or FATAL "Error executing update cdr statement: ".$sth->errstr;

	if ($sth->rows > 0) {
		DEBUG "cdr ID $cdr->{id} updated";
		write_cdr_cols($cdr,$cdr->{id},
			$acc_cash_balance_col_model_key,
			$acc_time_balance_col_model_key,
			$acc_relation_col_model_key);
		if ($dupdbh) {
			$sth_duplicate_cdr->execute(@$cdr{@cdr_fields})
			or FATAL "Error executing duplicate cdr statement: ".$sth_duplicate_cdr->errstr;
			my $dup_cdr_id = $dupdbh->{'mysql_insertid'};
			if ($dup_cdr_id) {
				DEBUG "local cdr ID $cdr->{id} was duplicated to duplication cdr ID $dup_cdr_id";
				write_cdr_cols($cdr,$dup_cdr_id,
					$dup_cash_balance_col_model_key,
					$dup_time_balance_col_model_key,
					$dup_relation_col_model_key);
			} else {
				FATAL "cdr ID $cdr->{id} and col data could not be duplicated";
			}
		}
	} else {
		$rollback = 1;
		FATAL "cdr ID $cdr->{id} seems to be already processed by someone else";
	}

	return 1;

}

sub write_cdr_cols {

	my $cdr = shift;
	my $cdr_id = shift;
	my $cash_balance_col_model_key = shift;
	my $time_balance_col_model_key = shift;
	my $relation_col_model_key = shift;

	foreach my $dir (('source', 'destination')) {
		foreach my $provider (('carrier','reseller','customer')) {
			write_cdr_col_data($cash_balance_col_model_key,$cdr,$cdr_id,
				{ direction => $dir, provider => $provider, cash_balance => 'cash_balance' },
				$cdr->{$dir.'_'.$provider."_cash_balance_before"},
				$cdr->{$dir.'_'.$provider."_cash_balance_after"}) if $write_cash_balance_before_after;

			write_cdr_col_data($time_balance_col_model_key,$cdr,$cdr_id,
				{ direction => $dir, provider => $provider, time_balance => 'free_time_balance' },
				$cdr->{$dir.'_'.$provider."_free_time_balance_before"},
				$cdr->{$dir.'_'.$provider."_free_time_balance_after"}) if $write_free_time_balance_before_after;

			write_cdr_col_data($relation_col_model_key,$cdr,$cdr_id,
				{ direction => $dir, provider => $provider, relation => 'profile_package_id' },
				$cdr->{$dir.'_'.$provider."_profile_package_id"}) if $write_profile_package_id;

			write_cdr_col_data($relation_col_model_key,$cdr,$cdr_id,
				{ direction => $dir, provider => $provider, relation => 'contract_balance_id' },
				$cdr->{$dir.'_'.$provider."_contract_balance_id"}) if $write_contract_balance_id;
		}
	}

}

sub get_call_cost {

	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $contract_id = shift;
	my $profile_id = shift;
	my $readonly = shift;
	my $r_profile_info = shift;
	my $r_package_info = shift;
	my $r_cost = shift;
	my $r_real_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $r_onpeak = shift;
	my $r_balances = shift;

	my $src_user = $cdr->{source_cli};
	my $src_user_domain = $cdr->{source_cli}.'@'.$cdr->{source_domain};
	my $dst_user = $cdr->{destination_user_in};
	my $dst_user_domain = $cdr->{destination_user_in}.'@'.$cdr->{destination_domain};

	DEBUG "calculating call cost for profile_id $profile_id with type $type, direction $direction, ".
		"src_user_domain $src_user_domain, dst_user_domain $dst_user_domain";

	unless(get_profile_info($profile_id, $type, $direction, $src_user_domain, $dst_user_domain,
		$r_profile_info, $cdr->{start_time})) {
		DEBUG "no match for full uris, trying user only for profile_id $profile_id with type $type, direction $direction, ".
			"src_user_domain $src_user, dst_user_domain $dst_user";
		unless(get_profile_info($profile_id, $type, $direction, $src_user, $dst_user,
			$r_profile_info, $cdr->{start_time})) {
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

	$$r_rating_duration = 0; # ensure we start with zero length

	DEBUG sub { "billing fee is ".(Dumper $r_profile_info) };

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
	my $init = $cdr->{is_fragmented} // 0;
	my $duration = (defined $cdr->{rating_duration} and $cdr->{rating_duration} < $cdr->{duration}) ? $cdr->{rating_duration} : $cdr->{duration};
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
				   or is_offpeak_weekday($cdr->{start_time}, $offset, \@offpeak_weekdays)) {
			$$r_onpeak = 0;
		} else {
			$$r_onpeak = 1;
		}
	}

	while ($duration > 0) {
		DEBUG "try to rate remaining duration of $duration secs";

		if(is_offpeak_special($cdr->{start_time}, $offset, \@offpeak_special)) {
			#print "offset $offset is offpeak-special\n";
			$onpeak = 0;
		} elsif(is_offpeak_weekday($cdr->{start_time}, $offset, \@offpeak_weekdays)) {
			#print "offset $offset is offpeak-weekday\n";
			$onpeak = 0;
		} else {
			#print "offset $offset is onpeak\n";
			$onpeak = 1;
		}

		unless($init) {
			$init = 1;
			$interval = $onpeak == 1 ?
				$r_profile_info->{on_init_interval} : $r_profile_info->{off_init_interval};
			$rate = $onpeak == 1 ?
				$r_profile_info->{on_init_rate} : $r_profile_info->{off_init_rate};
			DEBUG "add init rate $rate per sec to costs";
		} else {
			last if $split_peak_parts and defined($$r_onpeak) and $$r_onpeak != $onpeak
                                and not defined $cdr->{rating_duration};

			$interval = $onpeak == 1 ?
				$r_profile_info->{on_follow_interval} : $r_profile_info->{off_follow_interval};
			$rate = $onpeak == 1 ?
				$r_profile_info->{on_follow_rate} : $r_profile_info->{off_follow_rate};
			DEBUG "add follow rate $rate per sec to costs";
		}
		$$r_onpeak = $onpeak;
		$rate *= $interval;
		DEBUG "interval is $interval, so rate for this interval is $rate";

		#my @bals = grep {($_->{start_unix} + $offset) <= $cdr->{start_time}} @$r_balances;
		my $current_call_time = int($cdr->{start_time} + $offset);
		my @bals = grep {
			$_->{start_unix} <= $current_call_time &&
			($current_call_time <= $_->{end_unix} || is_infinite_unix($_->{end_unix}))
		} @$r_balances;
		@bals or FATAL "No contract balance for CDR $cdr->{id} found";
		WARNING "overlapping contract balances for CDR $cdr->{id} found: ".(Dumper \@bals) if (scalar @bals) > 1;
		foreach my $bal (@bals) {
			delete $bal_map{$bal->{id}};
		}
		@bals = @{ sort_contract_balances(\@bals) };
		my $bal = $bals[0];
		$last_bal = $bal;

		if (defined $prev_bal_id) {
			if ($bal->{id} != $prev_bal_id) { #contract balance transition
				DEBUG sub { "next contract balance entered: ".(Dumper $bal) };
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
			DEBUG sub { "starting with contract balance: ".(Dumper $bal) };
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
		}
		$bal->{cash_balance_interval} += $rate;

		$$r_real_cost += $rate;

		$duration -= $interval;
		$$r_rating_duration += $interval;

		$offset += $interval;
	}

	if ((scalar @cash_balance_rates) > 0) {
		my @remaining_bals = @{ sort_contract_balances([ values %bal_map ]) };
		foreach my $bal (@remaining_bals) {
			DEBUG sub { "remaining contract balance: ".(Dumper $bal) };
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
				set_subscriber_lock_level($contract_id,$r_package_info->{underrun_lock_level},$readonly);
				set_subscriber_status($contract_id,$r_package_info->{underrun_lock_level},$readonly);
				$last_bal->{underrun_lock_time} = $now;
			}
		}

		if (!$underrun_profiles_applied && defined $r_package_info->{underrun_profile_threshold} && $prev_cash_balance >= $r_package_info->{underrun_profile_threshold} && $last_bal->{cash_balance} < $r_package_info->{underrun_profile_threshold}) {
			$underrun_profiles_applied = 1;
			DEBUG "cash balance was decreased from $prev_cash_balance to $last_bal->{cash_balance} and dropped below underrun profile threshold $r_package_info->{underrun_profile_threshold}";
			if (add_profile_mappings($contract_id,$cdr->{start_time} + $cdr->{duration},$r_package_info->{id},'underrun',$readonly) > 0) {
				$last_bal->{underrun_profile_time} = $now;
			}
		}
	}

	return 1;

}

sub truncate_cash_balance {

	return sprintf("%.4f",shift);

}
sub truncate_free_time_balance {

	return sprintf("%.0f",shift);

}

sub sort_contract_balances {

	my $balances = shift;
	my $desc = shift;
	$desc = ($desc ? -1 : 1);
	my @bals = sort { ($a->{start_unix} <=> $b->{start_unix}) * $desc; } @$balances;
	return \@bals;

}

sub get_prepaid {

	my $cdr = shift;
	my $billing_info = shift;
	my $prefix = shift;
	my $prepaid = (defined $billing_info ? $billing_info->{prepaid} : undef);
	# todo: fetch these from another eav table ..
	if (defined $prefix && exists $cdr->{$prefix.'prepaid'} && defined $cdr->{$prefix.'prepaid'}) {
		# cdr is supposed to provide prefilled columns:
		#   source_prepaid
		#   destination_prepaid <-- mediator should provide this one at least
		$prepaid = $cdr->{$prefix.'prepaid'};
	} else {
		# undefined without billing info and prefix
	}
	return $prepaid;

}

sub get_snapshot_contract_balance {

	my $balances = shift;
	return sort_contract_balances($balances)->[-1];

}

sub populate_prepaid_cost_cache {

	if (!defined $prepaid_costs_cache) {
		DEBUG "empty prepaid_costs cache, populate it";
		$sth_prepaid_costs_count->execute()
			or FATAL "Error executing get prepaid costs count statement: ".$sth_prepaid_costs_count->errstr;
		my ($count) = $sth_prepaid_costs_count->fetchrow_array();
		if ($count > $prepaid_costs_cache_limit) {
			WARNING "over $prepaid_costs_cache_limit pending prepaid_costs records, too many to preload";
		} else {
			$prepaid_costs_cache = {};
			$sth_prepaid_costs_cache->execute()
				or FATAL "Error executing get prepaid costs cache statement: ".$sth_prepaid_costs_cache->errstr;
			while (my $prepaid_cost = $sth_prepaid_costs_cache->fetchrow_hashref()) {
				$prepaid_costs_cache->{$prepaid_cost->{call_id}} //= {};
				my $map = $prepaid_costs_cache->{$prepaid_cost->{call_id}};
				$map->{$prepaid_cost->{source_user_id}} //= {};
				$map = $map->{$prepaid_cost->{source_user_id}};
				if (exists $map->{$prepaid_cost->{destination_user_id}}) {
					WARNING "duplicate prepaid_costs call_id = $prepaid_cost->{call_id}, source_user_id = $prepaid_cost->{source_user_id}, destination_user_id = $prepaid_cost->{destination_user_id}";
				}
				$map->{$prepaid_cost->{destination_user_id}} = $prepaid_cost;
			}
			DEBUG "prepaid_costs cache populated, $count records";
			return 1;
		}
	} else {
		DEBUG "prepaid_costs cache already populated";
	}
	return 0;

}

sub clear_prepaid_cost_cache {

	undef $prepaid_costs_cache;

}

sub get_prepaid_cost {

	my $cdr = shift;
	my $entry = undef;
	my @call_ids = (
		$cdr->{call_id},
		$cdr->{call_id} . '_pbx-1',
	);
	if (defined $prepaid_costs_cache) {
		foreach my $call_id (@call_ids) {
			if (exists $prepaid_costs_cache->{$call_id}) {
				my $map = $prepaid_costs_cache->{$call_id};
				if (exists $map->{$cdr->{source_user_id}}) {
					$map = $map->{$cdr->{source_user_id}};
					if (exists $map->{$cdr->{destination_user_id}}) {
						DEBUG "prepaid_costs call_id = $cdr->{call_id}, source_user_id = $cdr->{source_user_id}, destination_user_id = $cdr->{destination_user_id} found in cache";
						$entry = $map->{$cdr->{destination_user_id}};
						last;
					}
				}
			}
		}
	} else {
		foreach my $call_id (@call_ids) {
			$sth_prepaid_cost->execute($call_id,$cdr->{source_user_id},$cdr->{destination_user_id})
				or FATAL "Error executing get prepaid cost statement: ".$sth_prepaid_cost->errstr;
			my $prepaid_cost = $sth_prepaid_cost->fetchall_hashref('destination_user_id');
			if ($prepaid_cost && exists $prepaid_cost->{$cdr->{destination_user_id}}) {
				DEBUG "prepaid cost record for call ID $cdr->{call_id} retrieved";
				$entry = $prepaid_cost->{$cdr->{destination_user_id}};
				last;
			}
		}
	}
	return $entry;

}

sub drop_prepaid_cost {

	my $entry = shift;
	my $count = $sth_delete_prepaid_cost->execute($entry->{call_id},$entry->{source_user_id},$entry->{destination_user_id})
		or FATAL "Error executing delete prepaid cost statement: ".$sth_delete_prepaid_cost->errstr;
	if ($count > 1) {
		WARNING "multiple prepaid_costs call_id = $entry->{call_id}, source_user_id = $entry->{source_user_id}, destination_user_id = $entry->{destination_user_id} deleted";
	} elsif ($count == 1) {
		DEBUG "prepaid_costs call_id = $entry->{call_id}, source_user_id = $entry->{source_user_id}, destination_user_id = $entry->{destination_user_id} deleted";
	} elsif ($count == 1) {
		WARNING "no prepaid_costs call_id = $entry->{call_id}, source_user_id = $entry->{source_user_id}, destination_user_id = $entry->{destination_user_id} deleted";
	}
	if (defined $prepaid_costs_cache) {
		if (exists $prepaid_costs_cache->{$entry->{call_id}}) {
			my $map = $prepaid_costs_cache->{$entry->{call_id}};
			if (exists $map->{$entry->{source_user_id}}) {
				$map = $map->{$entry->{source_user_id}};
				if (exists $map->{$entry->{destination_user_id}}) {
					delete $map->{$entry->{destination_user_id}};
					my $empty = (scalar keys %$map) == 0;
					$map = $prepaid_costs_cache->{$entry->{call_id}};
					delete $map->{$entry->{source_user_id}} if $empty;
					$empty = (scalar keys %$map) == 0;
					delete $prepaid_costs_cache->{$entry->{call_id}} if $empty;
					DEBUG "dropped prepaid_costs call_id = $entry->{call_id}, source_user_id = $entry->{source_user_id}, destination_user_id = $entry->{destination_user_id} from cache";
				}
			}
		}
	}
	return $count;

}

sub prepare_cdr_col_model {

	my $dbh = shift;
	my $col_model_key = shift;
	#print "prepare: $col_model_key\n";
	my $model_description = shift;
	my $description_prefix = shift;
	my $dimensions = shift;
	my $col_dimension_stmt_map = shift;
	my $write_stmt = shift;

	$cdr_col_models{$col_model_key} = {
		description => $model_description,
		description_prefix => $description_prefix,
	};
	my $model = $cdr_col_models{$col_model_key};

	$model->{dimensions} = $dimensions;

	my %col_dimension_map = ();
	foreach my $dimension (@$dimensions) {
		my $stmt = $col_dimension_stmt_map->{$dimension}->{sql};
		my $description = $col_dimension_stmt_map->{$dimension}->{description};
		my $get_col = { description => $description, };
		$get_col->{sth} = $dbh->prepare($stmt)
			or FATAL "Error preparing $description statement: ".$dbh->errstr;
		$col_dimension_map{$dimension} = $get_col;
	}
	$model->{dimension_sths} = \%col_dimension_map;

	$model->{write_sth} = { description => $write_stmt->{description}, };
	$model->{write_sth}->{sth} = $dbh->prepare($write_stmt->{sql})
		or FATAL "Error preparing ".$write_stmt->{description}." statement: ".$dbh->errstr;

}

sub init_cdr_col_model {

	my $col_model_key = shift;
	#print "init: $col_model_key\n";
	FATAL "unknown column model key $col_model_key" unless exists $cdr_col_models{$col_model_key};
	my $model = $cdr_col_models{$col_model_key};
	$model->{dimension_dictionaries} = {};
    foreach my $dimension (keys %{$model->{dimension_sths}}) {
		my $sth = $model->{dimension_sths}->{$dimension}->{sth};
		$sth->execute()
			or FATAL "Error executing ".
			$model->{dimension_sths}->{$dimension}->{description}
			." statement: ".$sth->errstr;
		$model->{dimension_dictionaries}->{$dimension} = $sth->fetchall_hashref('type');
		$sth->finish;
    }
	INFO $model->{description} . " loaded\n";

}

sub write_cdr_col_data {

	my $col_model_key = shift;
	my $cdr = shift;
	my $cdr_id = shift;
	my $lookup = shift;
	my @vals = @_;
	FATAL "unknown column model key $col_model_key" unless exists $cdr_col_models{$col_model_key};
	my $model = $cdr_col_models{$col_model_key};
	my @bind_parms = ($cdr_id,$cdr->{start_time});
	my $virtual_col_name = '';
	foreach my $dimension (@{$model->{dimensions}}) {
		my $dimension_value = $lookup->{$dimension};
		unless ($dimension_value) {
			FATAL "missing '$dimension' dimension for writing ".$model->{description_prefix}." col data of ".$model->{description};
		}
		my $dictionary = $model->{dimension_dictionaries}->{$dimension};
		my $dimension_value_lookup = $dictionary->{$dimension_value};
		unless ($dimension_value_lookup) {
			FATAL "unknown '$dimension' col name '$dimension_value' for writing ".$model->{description_prefix}." col data of ".$model->{description};
		}
		push(@bind_parms,$dimension_value_lookup->{id});
		$virtual_col_name .= '_' if length($virtual_col_name) > 0;
		$virtual_col_name .= $lookup->{$dimension};
	}

	if ((scalar @vals) == 0 || (scalar grep { defined $_ } @vals) == 0) {
        DEBUG "empty '$virtual_col_name' ".$model->{description_prefix}." col data for cdr id ".$cdr_id.', skipping';
		return 0;
    } else {
		push(@bind_parms,@vals);
		push(@bind_parms,@vals);
	}

	my $sth = $model->{write_sth}->{sth};
	$sth->execute(@bind_parms)
		or FATAL "Error executing ".
		$model->{write_sth}->{description}
		."statement: ".$sth->errstr;
	if ($sth->rows == 1) {
		DEBUG $model->{description_prefix}.' col data created or up to date for cdr id '.$cdr_id.", column '$virtual_col_name': ".join(', ',@vals);
	} elsif ($sth->rows > 1) {
		DEBUG $model->{description_prefix}.' col data updated for cdr id '.$cdr_id.", column '$virtual_col_name': ".join(', ',@vals);
	#} else {
	#	DEBUG 'no '.$model->{description_prefix}.' col data written for cdr id '.$cdr_id.", column '$virtual_col_name': ".join(', ',@vals);
	}
	return $sth->rows;

}

sub get_customer_call_cost {

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

	my @balances = ();
	my %package_info = ();
	get_contract_balances($cdr, $contract_id, \%package_info, \@balances)
		or FATAL "Error getting ".$dir."customer contract ID $contract_id balances\n";

	my %billing_info = (); #profiles might have switched due to underrun while was carry over discarded
	get_billing_info($cdr->{start_time}, $contract_id, $cdr->{source_ip}, \%billing_info) or
		FATAL "Error getting ".$dir."customer billing info\n";

	DEBUG sub { $dir."customer info is " . Dumper({
		billing => \%billing_info,
		package => \%package_info,
		balances => \@balances,
		})};

	unless($billing_info{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		DEBUG "no billing info for ".$dir."customer contract ID $contract_id, skip";
		return -1;
	}

	my $prepaid = get_prepaid($cdr, \%billing_info, $dir.'user_');
	my $outgoing_prepaid = ($prepaid == 1 && $direction eq "out");
	my $prepaid_cost_entry = undef;
	if ($outgoing_prepaid) {
		DEBUG "billing profile is prepaid";
		populate_prepaid_cost_cache();
		$prepaid_cost_entry = get_prepaid_cost($cdr);
	}

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,
		$billing_info{profile_id}, $outgoing_prepaid && defined $prepaid_cost_entry,
		\%profile_info, \%package_info, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, \@balances)
		or FATAL "Error getting ".$dir."customer call cost\n";

	DEBUG "got call cost $$r_cost and free time $$r_free_time";

	my $snapshot_bal = get_snapshot_contract_balance(\@balances);
	$cdr->{$dir."customer_cash_balance_before"} = $snapshot_bal->{cash_balance_old};
	$cdr->{$dir."customer_free_time_balance_before"} = $snapshot_bal->{free_time_balance_old};
	$cdr->{$dir."customer_cash_balance_after"} = $snapshot_bal->{cash_balance_old};
	$cdr->{$dir."customer_free_time_balance_after"} = $snapshot_bal->{free_time_balance_old};
	$cdr->{$dir."customer_profile_package_id"} = $package_info{id};
	$cdr->{$dir."customer_contract_balance_id"} = $snapshot_bal->{id};

	$cdr->{$dir."customer_billing_fee_id"} = $profile_info{fee_id};
	$cdr->{$dir."customer_billing_zone_id"} = $profile_info{zone_id};
	$cdr->{frag_customer_onpeak} = $onpeak if $split_peak_parts;

	if ($outgoing_prepaid) { #prepaid out
		# overwrite the calculated costs with the ones from our table
		if (defined $prepaid_cost_entry) {
			$$r_cost = $prepaid_cost_entry->{cost};
			$$r_free_time = $prepaid_cost_entry->{free_time_used};
			drop_prepaid_cost($prepaid_cost_entry);

			# it would be more safe to add *_balance_before/after columns to the prepaid_costs table,
			# instead of reconstructing the balance values:
			$cdr->{$dir."customer_cash_balance_before"} = truncate_cash_balance($cdr->{$dir."customer_cash_balance_before"} * 1.0 + $prepaid_cost_entry->{cost});
			$cdr->{$dir."customer_free_time_balance_before"} = truncate_free_time_balance($cdr->{$dir."customer_free_time_balance_before"} * 1.0 + $prepaid_cost_entry->{free_time_used});

		} else {
			# maybe another rateomat was faster and already processed+deleted it?
			# in that case we should bail out here.
			WARNING "no prepaid cost record found for call ID $cdr->{call_id}, applying calculated costs";
			if ($prepaid_update_balance) {
				update_contract_balance(\@balances)
					or FATAL "Error updating ".$dir."customer contract balance\n";
			}
			$$r_cost = $real_cost;
			$cdr->{$dir."customer_cash_balance_after"} = $snapshot_bal->{cash_balance};
			$cdr->{$dir."customer_free_time_balance_after"} = $snapshot_bal->{free_time_balance};
		}
	} else { #postpaid in, postpaid out, prepaid in
		# we don't do prepaid for termination fees for now, so treat it as post-paid
		if($prepaid == 1 && $direction eq "in") { #prepaid in
			DEBUG "treat pre-paid billing profile as post-paid for termination fees";
			$$r_cost = $real_cost;
		} else { #postpaid in, postpaid out
			DEBUG "billing profile is post-paid, update contract balance";
		}
		update_contract_balance(\@balances)
			or FATAL "Error updating ".$dir."customer contract balance\n";
		$cdr->{$dir."customer_cash_balance_after"} = $snapshot_bal->{cash_balance};
		$cdr->{$dir."customer_free_time_balance_after"} = $snapshot_bal->{free_time_balance};
	}

	DEBUG "cost for this call is $$r_cost";

	return 1;

}

sub get_provider_call_cost {

	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $provider_info = shift;
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

	my $contract_id = $provider_info->{billing}->{contract_id};

	unless($provider_info->{billing}->{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		DEBUG "no billing info for ".$dir."provider contract ID $contract_id, skip";
		return -1;
	}

	my $provider_type;
	if ($provider_info->{billing}->{class} eq "reseller") {
		$provider_type = "reseller_";
	} else {
		$provider_type = "carrier_";
	}

	my $prepaid = get_prepaid($cdr, $provider_info->{billing},$dir.'provider_');

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,
		$provider_info->{billing}->{profile_id}, $prepaid, # no underruns for providers with prepaid profile
		\%profile_info, $provider_info->{package}, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, $provider_info->{balances})
		or FATAL "Error getting ".$dir."provider call cost\n";

	my $snapshot_bal = get_snapshot_contract_balance($provider_info->{balances});

	$cdr->{$dir.$provider_type."package_id"} = $provider_info->{package}->{id};
	$cdr->{$dir.$provider_type."contract_balance_id"} = $snapshot_bal->{id};

	$cdr->{$dir.$provider_type."billing_fee_id"} = $profile_info{fee_id};
	$cdr->{$dir.$provider_type."billing_zone_id"} = $profile_info{zone_id};
	$cdr->{'frag_'.$provider_type.'onpeak'} = $onpeak if $split_peak_parts;

	unless($prepaid == 1) {
		$cdr->{$dir.$provider_type."cash_balance_before"} = $snapshot_bal->{cash_balance_old};
		$cdr->{$dir.$provider_type."free_time_balance_before"} = $snapshot_bal->{free_time_balance_old};
		$cdr->{$dir.$provider_type."cash_balance_after"} = $snapshot_bal->{cash_balance_old};
		$cdr->{$dir.$provider_type."free_time_balance_after"} = $snapshot_bal->{free_time_balance_old};

		update_contract_balance($provider_info->{balances})
			or FATAL "Error updating ".$dir.$provider_type."provider contract balance\n";

		$cdr->{$dir.$provider_type."cash_balance_after"} = $snapshot_bal->{cash_balance};
		$cdr->{$dir.$provider_type."free_time_balance_after"} = $snapshot_bal->{free_time_balance};

	} else {
		WARNING $dir.$provider_type."provider is prepaid\n";
		# there are no prepaid cost records for providers, so we cannot
		# restore the original balance and leave the fields empty

		# no balance update for providers with prepaid profile
	}

	return 1;

}

sub rate_cdr {

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
	my @rating_durations;

	unless($cdr->{call_status} eq "ok") {
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
	my %source_provider_billing_info = ();
	my %source_provider_package_info = ();
	my @source_provider_balances = ();
	if($cdr->{source_provider_id} eq "0") {
		WARNING "Missing source_provider_id for source_user_id ".$cdr->{source_user_id}." in cdr #".$cdr->{id}."\n";
	} else {
		# we have to catchup balances at this point before getting the profile, since underrun profiles could get applied:
		get_contract_balances($cdr, $cdr->{source_provider_id}, \%source_provider_package_info, \@source_provider_balances)
			or FATAL "Error getting source provider contract ID $cdr->{source_provider_id} balances\n";
		get_billing_info($cdr->{start_time}, $cdr->{source_provider_id}, $cdr->{source_ip}, \%source_provider_billing_info)
			or FATAL "Error getting source provider billing info for cdr #".$cdr->{id}."\n";
	}
	my $source_provider_info = {
		billing => \%source_provider_billing_info,
		package => \%source_provider_package_info,
		balances => \@source_provider_balances,
	};
	DEBUG sub { "source_provider_info is ".(Dumper $source_provider_info) };

	#unless($source_provider_billing_info{profile_info}) {
	#   FATAL "Missing billing profile for source_provider_id ".$cdr->{source_provider_id}." for cdr #".$cdr->{id}."\n";
	#}

	DEBUG "fetching destination provider info for destination_provider_id #$$cdr{destination_provider_id}";
	my %destination_provider_billing_info = ();
	my %destination_provider_package_info = ();
	my @destination_provider_balances = ();
	if($cdr->{destination_provider_id} eq "0") {
		WARNING "Missing destination_provider_id for destination_user_id ".$cdr->{destination_user_id}." in cdr #".$cdr->{id}."\n";
	} else {
		# we have to catchup balances at this point before getting the profile, since underrun profiles could get applied:
		get_contract_balances($cdr, $cdr->{destination_provider_id}, \%destination_provider_package_info, \@destination_provider_balances)
			or FATAL "Error getting destination provider contract ID $cdr->{destination_provider_id} balances\n";
		get_billing_info($cdr->{start_time}, $cdr->{destination_provider_id}, $cdr->{source_ip}, \%destination_provider_billing_info)
			or FATAL "Error getting destination provider billing info for cdr #".$cdr->{id}."\n";
	}
	my $destination_provider_info = {
		billing => \%destination_provider_billing_info,
		package => \%destination_provider_package_info,
		balances => \@destination_provider_balances,
	};
	DEBUG sub { "destination_provider_info is ".(Dumper $destination_provider_info) };

	#unless($destination_provider_billing_info{profile_info}) {
	#   FATAL "Missing billing profile for destination_provider_id ".$cdr->{destination_provider_id}." for cdr #".$cdr->{id}."\n";
	#}

	# call from local subscriber
	if($cdr->{source_user_id} ne "0") {
		DEBUG "call from local subscriber, source_user_id is $$cdr{source_user_id}";
		# if we have a call from local subscriber, the source provider MUST be a reseller
		if($source_provider_billing_info{profile_id} && $source_provider_billing_info{class} ne "reseller") {
			FATAL "The local source_user_id ".$cdr->{source_user_id}." has a source_provider_id ".$cdr->{source_provider_id}.
				" which is not a reseller in cdr #".$cdr->{id}."\n";
		}

		if($cdr->{destination_user_id} ne "0") {
			DEBUG "call to local subscriber, destination_user_id is $$cdr{destination_user_id}";
			# call to local subscriber (on-net)

			# there is no carrier cost for on-net calls

			# for calls towards a local user, termination fees might apply if
			# we find a fee with direction "in"
			if($destination_provider_billing_info{profile_id}) {
				DEBUG "destination provider has billing profile $destination_provider_billing_info{profile_id}, get reseller termination cost";
				get_provider_call_cost($cdr, $type, "in",
							$destination_provider_info, \$destination_reseller_cost, \$destination_reseller_free_time,
							\$rating_durations[@rating_durations])
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
						\$rating_durations[@rating_durations])
				or FATAL "Error getting destination customer cost for local destination_user_id ".
						$cdr->{destination_user_id}." for cdr ".$cdr->{id}."\n";
			DEBUG "destination customer termination cost is $destination_customer_cost";

		} else {
			# we can't charge termination fees to the callee if it's not local

			# for the carrier cost, we use the destination billing profile of a peer
			# (this is what the peering provider is charging the carrier)
			if($destination_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching source_carrier_cost based on destination_provider_billing_info ".(Dumper \%destination_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "out",
							$destination_provider_info, \$source_carrier_cost, \$source_carrier_free_time,
							\$rating_durations[@rating_durations])
						or FATAL "Error getting source carrier cost for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing destination profile, so we can't calculate source_carrier_cost for destination_provider_billing_info ".(Dumper \%destination_provider_billing_info);
			}
		}

		# get reseller cost
		if($source_provider_billing_info{profile_id}) {
			get_provider_call_cost($cdr, $type, "out",
						$source_provider_info, \$source_reseller_cost, \$source_reseller_free_time,
						\$rating_durations[@rating_durations])
				 or FATAL "Error getting source reseller cost for cdr ".$cdr->{id}."\n";
		} else {
			# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
			# in theory, all resellers MUST have a billing profile, so we could bail out here
		}

		# get customer cost
		get_customer_call_cost($cdr, $type, "out",
					\$source_customer_cost, \$source_customer_free_time,
					\$rating_durations[@rating_durations])
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
			if($source_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching destination_carrier_cost based on source_provider_billing_info ".(Dumper \%source_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "in",
							$source_provider_info, \$destination_carrier_cost, \$destination_carrier_free_time,
							\$rating_durations[@rating_durations])
					or FATAL "Error getting destination carrier cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing source profile, so we can't calculate destination_carrier_cost for source_provider_billing_info ".(Dumper \%source_provider_billing_info);
			}
			if($destination_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching destination_reseller_cost based on source_provider_billing_info ".(Dumper \%destination_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "in",
							$destination_provider_info, \$destination_reseller_cost, \$destination_reseller_free_time,
							\$rating_durations[@rating_durations])
					or FATAL "Error getting destination reseller cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
				# in theory, all resellers MUST have a billing profile, so we could bail out here
				WARNING "missing destination profile, so we can't calculate destination_reseller_cost for destination_provider_billing_info ".(Dumper \%destination_provider_billing_info);
			}
			get_customer_call_cost($cdr, $type, "in",
						\$destination_customer_cost, \$destination_customer_free_time,
						\$rating_durations[@rating_durations])
				or FATAL "Error getting destination customer cost for local destination_user_id ".
						$cdr->{destination_user_id}." for cdr ".$cdr->{id}."\n";
		} else {
			# TODO what about transit calls?
		}
	}

	if ($split_peak_parts) {
		# We require the onpeak/offpeak thresholds to be the same for all rating fee profiles used by any
		# one particular CDR, so that CDR fragmentations are uniform across customer/carrier/reseller/etc
		# entries. Mismatching onpeak/offpeak thresholds are a fatal error (which also results in a
		# transaction rollback).

		my %rating_durations;
		for my $rd (@rating_durations) {
			defined($rd) and $rating_durations{$rd} = 1;
		}
		scalar(keys(%rating_durations)) > 1
			and FATAL "Error getting consistent rating fragment for cdr ".$cdr->{id}.". Rating profiles don't match.";
		my $rating_duration = (keys(%rating_durations))[0] // $cdr->{duration};

		if ($rating_duration < $cdr->{duration}) {
			my $sth = $sth_create_cdr_fragment;
			$sth->execute($rating_duration, $rating_duration, $cdr->{id})
				or FATAL "Error executing create cdr fragment statement: ".$sth->errstr;
			if ($sth->rows > 0) {
				DEBUG "cdr ID $cdr->{id} covers $rating_duration secs before crossing coherent onpeak/offpeak. another cdr for remaining " .
				($cdr->{duration} - $rating_duration) . " secs of call ID $cdr->{call_id} was created";
			} else {
				$rollback = 1;
				FATAL "cdr ID $cdr->{id} seems to be already processed by someone else";
			}
			$cdr->{is_fragmented} = 1;
			$cdr->{duration} = $rating_duration;
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

sub daemonize {

	my $pidfile = shift;

	chdir '/' or FATAL "Can't chdir to /: $!\n";
	open STDIN, '<', '/dev/null' or FATAL "Can't read /dev/null: $!\n";
	open STDOUT, ">", "/dev/null" or FATAL "Can't open /dev/null: $!\n";
	open STDERR, ">", "/dev/null" or FATAL "Can't open /dev/null: $!\n";
	open $PID, ">>", "$pidfile" or FATAL "Can't open '$pidfile' for writing: $!\n";
	flock($PID, LOCK_EX | LOCK_NB) or FATAL "Unable to lock pidfile '$pidfile': $!\n";
	defined(my $pid = fork) or FATAL "Can't fork: $!\n";
	exit if $pid;
	setsid or FATAL "Can't start a new session: $!\n";
	seek $PID, 0, SEEK_SET;
	truncate $PID, 0;
	printflush $PID "$$\n";
	open STDOUT, "|-", "logger -s -t $log_ident" or FATAL "Can't open logger output stream: $!\n";
	open STDERR, '>&STDOUT' or FATAL "Can't dup stdout: $!\n";

}

sub signal_handler {

	$shutdown = 1;

}

sub debug_rating_time {

	my $t = shift;
	my $cdr_id = shift;
	my $error = shift;
	DEBUG sub { "rating cdr ID $cdr_id " . ($error ? "aborted after" : "completed successfully in") . ' ' . sprintf("%.3f",Time::HiRes::time() - $t) . " secs" };

}

sub main {

	openlog($log_ident, $log_opts, $log_facility)
		or die "Error opening syslog: $!\n";

	daemonize($pidfile)
		if($fork == 1);

	local $SIG{TERM} = \&signal_handler;
	local $SIG{INT} = \&signal_handler;
	local $SIG{QUIT} = \&signal_handler;
	local $SIG{HUP} = \&signal_handler;

	if ($maintenance_mode eq 'yes') {
		while (!$shutdown) {
			sleep(1);
		}
		exit(0);
	}

	init_db or FATAL "Error initializing database handlers\n";
	my $rated = 0;
	my $next_del = 10000;
	my %failed_counter_map = ();
	foreach (keys %cdr_col_models) {
		init_cdr_col_model($_);
	}

	INFO "Up and running.\n";
	while (!$shutdown) {

		$log_fatal = 1;
		$billdbh->ping || init_db;
		$acctdbh->ping || init_db;
		$provdbh and ($provdbh->ping || init_db);
		$dupdbh and ($dupdbh->ping || init_db);
		clear_prepaid_cost_cache();

		my $error;
		my @cdrs = ();
		if ($billdbh && $acctdbh && $provdbh) {
			eval {
				get_unrated_cdrs(\@cdrs);
				INFO "Grabbed ".(scalar @cdrs)." CDRs" if (scalar @cdrs) > 0;
			};
			$error = $@;
			if ($error) {
				if ($DBI::err == 2006) {
					INFO "DB connection gone, retrying...";
					next;
				}
				FATAL "Error getting next bunch of CDRs: " . $error;
			}
		} else {
			WARNING "no-op loop since mandatory db connections are n/a";
		}

		$shutdown and last;

		unless (@cdrs) {
			INFO "No new CDRs to rate, sleep $loop_interval";
			sleep($loop_interval);
			next;
		}

		my $rated_batch = 0;
		my $t;
		my $cdr_id;
		my $info_prefix;
		my $failed = 0;
		eval {
			foreach my $cdr (@cdrs) {
				$rollback = 0;
				$log_fatal = 0;
				$info_prefix = ($rated_batch + 1) . "/" . (scalar @cdrs) . " - ";
				eval {
					$t = Time::HiRes::time() if $debug;
					$cdr_id = $cdr->{id};
					DEBUG "start rating CDR ID $cdr_id";
					# required to avoid contract_balances duplications during catchup:
					begin_transaction($billdbh,'READ COMMITTED');
					# row locks are released upon commit/rollback and have to cover
					# the whole transaction. thus locking contract rows for preventing
					# concurrent catchups will be our very first SQL statement in the
					# billingdb transaction:
					lock_contracts($cdr);
					begin_transaction($provdbh);
					begin_transaction($acctdbh);
					begin_transaction($dupdbh);

					INFO $info_prefix."rate CDR ID ".$cdr->{id};
					rate_cdr($cdr, $type) && update_cdr($cdr);

					# we would need a XA/distributed transaction manager for this:
					commit_transaction($billdbh);
					commit_transaction($provdbh);
					commit_transaction($acctdbh);
					commit_transaction($dupdbh);

					$rated_batch++;
					delete $failed_counter_map{$cdr_id};
					debug_rating_time($t,$cdr_id,0);
					check_shutdown() and last;
				};
				$error = $@;
				if ($error) {
					debug_rating_time($t,$cdr_id,1);
					if ($rollback) {
						INFO $info_prefix."rolling back changes for CDR ID $cdr_id";
						rollback_all();
						next; #move on to the next cdr of the batch
					} else {
						$failed_counter_map{$cdr_id} = 0 if !exists $failed_counter_map{$cdr_id};
						if ($failed_counter_map{$cdr_id} < $failed_cdr_max_retries && !defined $DBI::err) {
							WARNING $info_prefix."rating CDR ID $cdr_id aborted " .
								($failed_counter_map{$cdr_id} > 0 ? " (retry $failed_counter_map{$cdr_id})" : "") .
								": " . $error;
							$failed_counter_map{$cdr_id} = $failed_counter_map{$cdr_id} + 1;
							$failed += 1;
							rollback_all();
							next; #move on to the next cdr of the batch
						} else {
							die($error); #rethrow
						}
					}
				}
			}
		};
		$log_fatal = 1;
		$error = $@;
		if ($error)	{
			if (defined $DBI::err) {
				INFO "Caught DBI:err ".$DBI::err, "\n";
				if ($DBI::err == 2006) {
					INFO "DB connection gone, retrying...";
					# disconnect from all of them so transactions are on par
					rollback_all();
					$billdbh->disconnect;
					$provdbh and ($provdbh->disconnect);
					$acctdbh->disconnect;
					$dupdbh and ($dupdbh->disconnect);
					next; #fetch new batch
				} elsif ($DBI::err == 1213) {
					INFO "Transaction concurrency problem, rolling back and retrying...";
					rollback_all();
					next; #fetch new batch
				} else {
					rollback_all();
					FATAL $error; #terminate upon other DB errors
				}
			} else {
				rollback_all();
				FATAL $info_prefix."rating CDR ID $cdr_id aborted (failed ".
					($failed_cdr_max_retries + 1)." times), please fix it manually: " . $error; #terminate
			}
		}

		$rated += $rated_batch;
		INFO "Batch of $rated_batch CDRs completed. $rated CDRs rated overall so far.\n";

		$shutdown and last;

		if ($rated >= $next_del) { # not ideal imho
			$next_del = $rated + 10000;
			while ($sth_delete_old_prepaid->execute > 0) {
				WARNING $sth_delete_old_prepaid->rows;
			}
		}

		if ((scalar @cdrs) < 5)	{
			INFO "Less than 5 new CDRs, sleep $loop_interval";
			sleep($loop_interval);
		}
		if ($failed > 0) {
            INFO "There were $failed failed CDRs, sleep $failed_cdr_retry_delay";
			sleep($failed_cdr_retry_delay);
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
	$split_peak_parts and $sth_create_cdr_fragment->finish;
	$sth_get_cbalances->finish;
	$sth_update_cbalance_w_underrun_profiles_lock->finish;
	$sth_update_cbalance_w_underrun_lock->finish;
	$sth_update_cbalance_w_underrun_profiles->finish;
	$sth_update_cbalance->finish;
	$sth_new_cbalance->finish;
	$sth_new_cbalance_infinite_future->finish;
	$sth_get_last_cbalance->finish;
	$sth_get_cbalance->finish;
	$sth_get_first_cbalance->finish;
	$sth_get_last_topup_cbalance->finish;
	$sth_lnp_number->finish;
	$sth_lnp_profile_info->finish;
	$sth_get_contract_info->finish;
	$sth_prepaid_costs_cache->finish;
	$sth_prepaid_costs_count->finish;
	$sth_prepaid_cost->finish;
	$sth_delete_prepaid_cost->finish;
	$sth_delete_old_prepaid->finish;
	$sth_get_billing_voip_subscribers->finish;
	$sth_get_package_profile_sets->finish;
	$sth_create_billing_mappings->finish;
	$sth_lock_billing_subscribers->finish;
	$sth_unlock_billing_subscribers->finish;
	$sth_get_provisioning_voip_subscribers and $sth_get_provisioning_voip_subscribers->finish;
	$sth_get_usr_preference_attribute and $sth_get_usr_preference_attribute->finish;
	$sth_get_usr_preference_value and $sth_get_usr_preference_value->finish;
	$sth_create_usr_preference_value and $sth_create_usr_preference_value->finish;
	$sth_update_usr_preference_value and $sth_update_usr_preference_value->finish;
	$sth_delete_usr_preference_value and $sth_delete_usr_preference_value->finish;
	$sth_duplicate_cdr and $sth_duplicate_cdr->finish;
	foreach (keys %cdr_col_models) {
		my $model = $cdr_col_models{$_};
		$model->{write_sth}->{sth}->finish;
		foreach (values %{$model->{dimension_sths}}) {
			$_->{sth}->finish;
		}
	}

	$billdbh->disconnect;
	$provdbh->disconnect;
	$provdbh and $acctdbh->disconnect;
	$dupdbh and $dupdbh->disconnect;
	closelog;
	close $PID;
	unlink $pidfile;
}
