#!/usr/bin/perl -w
use lib '/usr/share/ngcp-rate-o-mat';
use strict;
use warnings;

use DBI;
use POSIX qw(setsid mktime ceil);
use Fcntl qw(LOCK_EX LOCK_NB SEEK_SET);
use IO::Handle;
use IO::Socket::UNIX;
use NetAddr::IP;
use Data::Dumper;
use Time::HiRes qw(); #for debugging info only
use List::Util qw(shuffle);
use Storable qw(dclone);
use JSON::XS qw(encode_json decode_json);

# constants: ###########################################################

$0 = 'ngcp-rate-o-mat'; ## no critic (Variables::RequireLocalizedPunctuationVars)
my $fork = $ENV{RATEOMAT_DAEMONIZE} // 0;
my $pidfile = $ENV{RATEOMAT_PIDFILE} // '/run/ngcp-rate-o-mat.pid';
my $type = 'call';
my $loop_interval = ((defined $ENV{RATEOMAT_LOOP_INTERVAL} && $ENV{RATEOMAT_LOOP_INTERVAL}) ? int $ENV{RATEOMAT_LOOP_INTERVAL} : 10);
my $debug = ((defined $ENV{RATEOMAT_DEBUG} && $ENV{RATEOMAT_DEBUG}) ? int $ENV{RATEOMAT_DEBUG} : 0);

# number of unrated cdrs to fetch at once:
my $batch_size = ((defined $ENV{RATEOMAT_BATCH_SIZE} && $ENV{RATEOMAT_BATCH_SIZE} > 0) ? int $ENV{RATEOMAT_BATCH_SIZE} : 100);

# if rate-o-mat processes are working on the same accounting.cdr table:
# set to 1 to minimize collisions (and thus rollbacks)
my $shuffle_batch = ((defined $ENV{RATEOMAT_SHUFFLE_BATCH} && $ENV{RATEOMAT_SHUFFLE_BATCH}) ? int $ENV{RATEOMAT_SHUFFLE_BATCH} : 0);

# preload the whole prepaid_costs table, if number of records
# is below this limit:
my $prepaid_costs_cache_limit = ((defined $ENV{RATEOMAT_PREPAID_COSTS_CACHE} && $ENV{RATEOMAT_PREPAID_COSTS_CACHE} > 0) ? int $ENV{RATEOMAT_PREPAID_COSTS_CACHE} : 10000);

# if split_peak_parts is set to true, rate-o-mat will create a separate
# CDR every time a peak time border is crossed for either the customer,
# the reseller or the carrier billing profile.
my $split_peak_parts = ((defined $ENV{RATEOMAT_SPLIT_PEAK_PARTS} && $ENV{RATEOMAT_SPLIT_PEAK_PARTS}) ? int $ENV{RATEOMAT_SPLIT_PEAK_PARTS} : 0);

# set to 1 to write real call costs to CDRs for postpaid, even if balance was consumed:
my $use_customer_real_cost = 0;
my $use_provider_real_cost = 0;
# don't update balance of prepaid contracts, if no prepaid_costs record is found (re-rating):
my $prepaid_update_balance = ((defined $ENV{RATEOMAT_PREPAID_UPDATE_BALANCE} && $ENV{RATEOMAT_PREPAID_UPDATE_BALANCE}) ? int $ENV{RATEOMAT_PREPAID_UPDATE_BALANCE} : 0);

# control writing cdr relation data:
# disable it for now until this will be limited to prepaid contracts,
# as it produces massive amounts of zeroed or unneeded data.
my $write_cash_balance_before_after = $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} // 0;
my $write_free_time_balance_before_after = $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} // 0;
my $write_profile_package_id = $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} // 0;
my $write_contract_balance_id = $ENV{RATEOMAT_WRITE_CDR_RELATION_DATA} // 0;

# terminate if the same cdr fails $failed_cdr_max_retries + 1 times:
my $failed_cdr_max_retries = ((defined $ENV{RATEOMAT_MAX_RETRIES} && $ENV{RATEOMAT_MAX_RETRIES} >= 0) ? int $ENV{RATEOMAT_MAX_RETRIES} : 2);
my $failed_cdr_retry_delay = ((defined $ENV{RATEOMAT_RETRY_DELAY} && $ENV{RATEOMAT_RETRY_DELAY} >= 0) ? int $ENV{RATEOMAT_RETRY_DELAY} : 30);
# with 2 retries and 30sec delay, rato-o-mat tolerates a replication
# lag of around 60secs until it terminates.

# use source_user if number and source_cli =~ /anonymous/i:
my $offnet_anonymous_source_cli_fallback = 1;

# pause between db connect attempts:
my $connect_interval = 3;

my $maintenance_mode = $ENV{RATEOMAT_MAINTENANCE} // 'no';

my $hostname_filepath = '/etc/ngcp_hostname';
$hostname_filepath = $ENV{RATEOMAT_HOSTNAME_FILEPATH} if exists $ENV{RATEOMAT_HOSTNAME_FILEPATH};

my $multi_master = ((defined $ENV{RATEOMAT_MUTLI_MASTER} && $ENV{RATEOMAT_MUTLI_MASTER}) ? int $ENV{RATEOMAT_MUTLI_MASTER} : 0);

#execute contract subscriber locks if fraud limits are exceeded after a call:
my $apply_fraud_lock = ((defined $ENV{RATEOMAT_FRAUD_LOCK} && $ENV{RATEOMAT_FRAUD_LOCK}) ? int $ENV{RATEOMAT_FRAUD_LOCK} : 0);

# test may execute rate-o-mat on another host with different
# timezone. the connection timezone can therefore be forced to
# eg. the UTC default on ngcp.
my $connection_timezone = $ENV{RATEOMAT_CONNECTION_TIMEZONE};
# $ENV{TZ} has to be adjusted in the root thread.

# option to transform onpeak/offpeak times to a subscriber contract's timezone:
my $subscriber_offpeak_tz = 0; #0;

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

my @mos_data_fields = qw(mos_average mos_average_packetloss mos_average_jitter mos_average_roundtrip);

my $acc_cash_balance_col_model_key = 1;
my $acc_time_balance_col_model_key = 2;
my $acc_relation_col_model_key = 3;
my $acc_tag_col_model_key = 4;

my $dup_cash_balance_col_model_key = 5;
my $dup_time_balance_col_model_key = 6;
my $dup_relation_col_model_key = 7;
my $dup_tag_col_model_key = 8;

# globals: #############################################################

my $shutdown = 0;
my $prepaid_costs_cache;
my %cdr_col_models = ();
my $rollback;
my $log_fatal = 1;

# load equalization using first or second order low pass filter:
my $cps_info = {
	rated => 0,
	rated_old => 0,
	d_rated => 0,
	d_rated_old => 0,
	dd_rated => 0,

	t => 0.0,
	t_old => 0.0,
	dt => 0.0,

	delay => $loop_interval,
	cps => 0.0,

	speedup => 0.02,
	speeddown => 0.01,
};

# stmt handlers: #######################################################

my $billdbh;
my $acctdbh;
my $provdbh;
my $dupdbh;
my $sth_get_contract_info;
my $sth_get_subscriber_contract_id;
my $sth_billing_info_network;
my $sth_billing_info;
my $sth_profile_info;
my $sth_profile_fraud_info;
my $sth_contract_fraud_info;
my $sth_upsert_cdr_period_costs;
my $sth_get_cdr_period_costs;
my $sth_duplicate_upsert_cdr_period_costs;
my $sth_duplicate_get_cdr_period_costs;
my $sth_offpeak;
my $sth_offpeak_subscriber;
my $sth_unrated_cdrs;
my $sth_get_cdr;
my $sth_lock_cdr;
my $sth_update_cdr;
my $sth_create_cdr_fragment;
my $sth_mos_data;
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
my $sth_lock_billing_subscribers;
my $sth_unlock_billing_subscribers;
my $sth_get_provisioning_voip_subscribers;
my $sth_get_usr_preference_attribute;
my $sth_get_usr_preference_value;
my $sth_create_usr_preference_value;
my $sth_update_usr_preference_value;
my $sth_delete_usr_preference_value;
my $sth_duplicate_cdr;
my $sth_duplicate_mos_data;

# run the main loop: ##################################################

main();
exit 0;

# implementation: ######################################################

sub FATAL {
	my $msg = shift;
	chomp $msg;
	die "FATAL: $msg\n";
}

sub DEBUG {

	return unless $debug;
	my $msg = shift;
	$msg = &$msg() if 'CODE' eq ref $msg;
	chomp $msg;
	$msg =~ s/#012 +/ /g;
	print "DEBUG: $msg\n";

}

sub INFO {

	my $msg = shift;
	chomp $msg;
	print "INFO: $msg\n";

}

sub WARNING {

	my $msg = shift;
	chomp $msg;
	warn "WARNING: $msg\n";

}

sub _sql_offpeak_convert_tz {

	my ($date_col) = @_;

	return 'unix_timestamp(convert_tz('.$date_col.
		',@@session.time_zone,(select coalesce((select tz.name FROM billing.v_contract_timezone tz WHERE tz.contract_id = ? LIMIT 1),@@session.time_zone))))';

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
	$billdbh->do("SET SESSION binlog_format = 'STATEMENT'") or WARNING 'error setting session binlog_format';
	$billdbh->do('SET time_zone = ?',undef,$connection_timezone) or FATAL 'error setting connection timezone' if $connection_timezone;
	INFO "Successfully connected to billing db...";

}

sub connect_acctdbh {

	do {
		INFO "Trying to connect to accounting db...";
		$acctdbh = DBI->connect("dbi:mysql:database=$AcctDB_Name;host=$AcctDB_Host;port=$AcctDB_Port", $AcctDB_User, $AcctDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $acctdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($acctdbh);
	$acctdbh->do("SET SESSION binlog_format = 'STATEMENT'") or WARNING 'error setting session binlog_format';
	$acctdbh->do('SET time_zone = ?',undef,$connection_timezone) or FATAL 'error setting connection timezone' if $connection_timezone;
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
	$provdbh->do("SET SESSION binlog_format = 'STATEMENT'") or WARNING 'error setting session binlog_format';
	$provdbh->do('SET time_zone = ?',undef,$connection_timezone) or FATAL 'error setting connection timezone' if $connection_timezone;
	INFO "Successfully connected to provisioning db...";

}

sub connect_dupdbh {

	unless ($DupDB_User) {
		undef $dupdbh;
		INFO "No duplication db credentials, disabled.";
		return;
	}

	do {
		INFO "Trying to connect to duplication db...";
		$dupdbh = DBI->connect("dbi:mysql:database=$DupDB_Name;host=$DupDB_Host;port=$DupDB_Port", $DupDB_User, $DupDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	} while(!defined $dupdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($dupdbh);
	$dupdbh->do("SET SESSION binlog_format = 'STATEMENT'") or WARNING 'error setting session binlog_format';
	$dupdbh->do('SET time_zone = ?',undef,$connection_timezone) or FATAL 'error setting connection timezone' if $connection_timezone;
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
		#capture result to force list context and prevent legacy komodo perl5db.pl bug:
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
		" p.underrun_lock_level, ".
		" (SELECT COUNT(*) FROM billing.package_profile_sets WHERE package_id = p.id AND discriminator = 'underrun') as underrun_profiles_count, ".
		" product.class ".
		"FROM billing.contracts c ".
		"JOIN billing.products product ON c.product_id = product.id ".
		"LEFT JOIN billing.profile_packages p ON c.profile_package_id = p.id ".
		"LEFT JOIN billing.contacts co ON c.contact_id = co.id ".
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

	$sth_billing_info_network = $billdbh->prepare(<<EOS
		SELECT bp.id, bp.prepaid,
			bp.interval_charge, bp.interval_free_time, bp.interval_free_cash,
			bp.interval_unit, bp.interval_count, bp.ignore_domain
		FROM billing.billing_profiles bp
		WHERE bp.id = billing.get_billing_profile_by_contract_id_network(?,?,?)
EOS
	) or FATAL "Error preparing network billing info statement: ".$billdbh->errstr;

	$sth_billing_info = $billdbh->prepare(<<EOS
		SELECT bp.id, bp.prepaid,
			bp.interval_charge, bp.interval_free_time, bp.interval_free_cash,
			bp.interval_unit, bp.interval_count, bp.ignore_domain
		FROM billing.billing_profiles bp
		WHERE bp.id = billing.get_billing_profile_by_contract_id(?,?)
EOS
	) or FATAL "Error preparing billing info statement: ".$billdbh->errstr;

	$sth_lnp_number = $billdbh->prepare(
		"SELECT lnp_provider_id,type FROM billing.lnp_numbers WHERE id = billing.get_lnp_number_id(?,?)"
	) or FATAL "Error preparing LNP number statement: ".$billdbh->errstr;

	$sth_profile_info = $billdbh->prepare(
		"SELECT id, source, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, offpeak_use_free_time, onpeak_use_free_time, ".
		"onpeak_extra_second, onpeak_extra_rate, ".
		"offpeak_extra_second, offpeak_extra_rate ".
		"FROM billing.billing_fees_history WHERE id = billing.get_billing_fee_id(?,?,?,?,?,null)"
	) or FATAL "Error preparing profile info statement: ".$billdbh->errstr;

	$sth_profile_fraud_info = $billdbh->prepare(
		"SELECT bp.fraud_interval_limit, bp.fraud_daily_limit, " .
		"bp.fraud_interval_lock, bp.fraud_daily_lock, bp.fraud_use_reseller_rates " .
		"FROM billing.billing_profiles bp WHERE bp.id = ?"
	) or FATAL "Error preparing profile fraud info statement: ".$billdbh->errstr;

	$sth_contract_fraud_info = $billdbh->prepare(
		"SELECT cfp.fraud_interval_limit, cfp.fraud_daily_limit, " .
		"cfp.fraud_interval_lock, cfp.fraud_daily_lock " .
		"FROM billing.contract_fraud_preferences cfp WHERE cfp.contract_id = ?"
	) or FATAL "Error preparing contract fraud info statement: ".$billdbh->errstr;

	$sth_lnp_profile_info = $billdbh->prepare(
		"SELECT id, source, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, offpeak_use_free_time, onpeak_use_free_time ".
		"FROM billing.billing_fees_history WHERE id = billing.get_billing_fee_id(?,?,?,null,?,\"exact_destination\")"
	) or FATAL "Error preparing LNP profile info statement: ".$billdbh->errstr;

	$sth_offpeak = $billdbh->prepare("select ".
		"unix_timestamp(concat(date_enum.d,' ',pw.start)),unix_timestamp(concat(date_enum.d,' ',pw.end))".
		" from ngcp.date_range_helper as date_enum ".
		"join billing.billing_peaktime_weekdays pw on pw.weekday=weekday(date_enum.d) ".
		"where date_enum.d >= date(from_unixtime(?)) ".
		"and date_enum.d <= date(from_unixtime(? + ?)) ".
		"and pw.billing_profile_id = ?".
		" union ".
		"select ".
		"unix_timestamp(ps.start),unix_timestamp(ps.end)" .
		" from billing.billing_peaktime_special as ps ".
		"where ps.billing_profile_id = ? ".
		"and (ps.start <= from_unixtime(? + ?) and ps.end >= from_unixtime(?))"
	) or FATAL "Error preparing offpeak statement: ".$billdbh->errstr;

	$sth_offpeak_subscriber = $billdbh->prepare("select ".
		_sql_offpeak_convert_tz("concat(date_enum.d,' ',pw.start)") .','. _sql_offpeak_convert_tz("concat(date_enum.d,' ',pw.end)") .
		" from ngcp.date_range_helper as date_enum ".
		"join billing.billing_peaktime_weekdays pw on pw.weekday=weekday(date_enum.d) ".
		"where date_enum.d >= date(from_unixtime(?)) ".
		"and date_enum.d <= date(from_unixtime(? + ?)) ".
		"and pw.billing_profile_id = ?".
		" union ".
		"select ".
		_sql_offpeak_convert_tz("ps.start") .','. _sql_offpeak_convert_tz("ps.end") .
		" from billing.billing_peaktime_special as ps ".
		"where ps.billing_profile_id = ? ".
		"and (ps.start <= from_unixtime(? + ?) and ps.end >= from_unixtime(?))"
	) or FATAL "Error preparing offpeak subscriber statement: ".$billdbh->errstr;

	$sth_unrated_cdrs = $acctdbh->prepare(
		"SELECT * ".
		"FROM accounting.cdr WHERE rating_status = 'unrated' ".
		"ORDER BY start_time ASC LIMIT " . $batch_size
	) or FATAL "Error preparing unrated cdr statement: ".$acctdbh->errstr;

	$sth_get_cdr = $acctdbh->prepare(
		"SELECT * ".
		"FROM accounting.cdr WHERE id = ?"
	) or FATAL "Error preparing get cdr statement: ".$acctdbh->errstr;

	$sth_lock_cdr = $acctdbh->prepare(
		"SELECT id, rating_status ".
		"FROM accounting.cdr WHERE id = ? FOR UPDATE"
	) or FATAL "Error preparing lock cdr statement: ".$acctdbh->errstr;

	$sth_update_cdr = $acctdbh->prepare(
		"UPDATE LOW_PRIORITY accounting.cdr SET ".
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
		"WHERE id = ?"
	) or FATAL "Error preparing update cdr statement: ".$acctdbh->errstr;

	my $upsert_cdr_period_costs_stmt = "INSERT INTO accounting.cdr_period_costs (" .
		"  id," .
		"  contract_id," .
		"  period," .
		"  period_date," .
		"  direction," .
		  #billing_profile_id,
		"  customer_cost," .
		"  reseller_cost," .
		"  cdr_count," .
		"  fraud_limit_exceeded," .
		"  fraud_limit_type," .
		"  first_cdr_start_time," .
		"  first_cdr_id," .
		"  last_cdr_start_time," .
		"  last_cdr_id" .
		") VALUES (" .
		"  NULL," .
		"  ?," . #_contract_id," .
		"  ?," . #'month'," .
		"  ?," . #_month_period_date," .
		"  ?," . #_direction," .
		  #_billing_profile_id,
		"  ?," . #_customer_cost," .
		"  ?," . #_reseller_cost," .
		"  1," .
		"  if(? > 0," . #_fraud_use_reseller_rates
		"   if(coalesce(? + 0.0 >= ? + 0.0,0),1,0)," . #_reseller_cost _fraud_interval_limit
		"   if(coalesce(? + 0.0 >= ? + 0.0,0),1,0))," . #_customer_cost _fraud_interval_limit
		"  ?," . #_fraud_limit_type," .
		"  ?," . #_cdr_start_time," .
		"  ?," . #_cdr_id," .
		"  ?," . #_cdr_start_time" .
		"  ?" . #_cdr_id," .
		") ON DUPLICATE KEY UPDATE " .
		  #billing_profile_id = _billing_profile_id,
		"  id = LAST_INSERT_ID(id)," . #_customer_cost," .
		"  fraud_limit_exceeded = if(? > 0," . #_fraud_use_reseller_rates
		"   if(coalesce(? + reseller_cost >= ? + 0.0,0),1,0)," . #_reseller_cost _fraud_interval_limit
		"   if(coalesce(? + customer_cost >= ? + 0.0,0),1,0))," . #_customer_cost _fraud_interval_limit
		"  customer_cost = ? + customer_cost," . #_customer_cost," .
		"  reseller_cost = ? + reseller_cost," . #_reseller_cost," .
		"  cdr_count = cdr_count + 1," .
		"  fraud_limit_type = ?," . #_fraud_limit_type
		"  first_cdr_start_time = if(? + 0.0 < first_cdr_start_time," . #_cdr_start_time
		"   ?," . #_cdr_start_time
		"   first_cdr_start_time)," .
		"  first_cdr_id = if(? + 0 < first_cdr_id," . #_cdr_id
		"   ?," . #_cdr_id
		"   first_cdr_id)," .
		"  last_cdr_start_time = if(? + 0.0 > last_cdr_start_time," . #_cdr_start_time
		"   ?," . #_cdr_start_time
		"   last_cdr_start_time)," .
		"  last_cdr_id = if(? + 0 > last_cdr_id," . #_cdr_id
		"   ?," . #_cdr_id
		"   last_cdr_id)";

	my $get_cdr_period_costs_stmt = "SELECT " .
		"cpc.fraud_limit_exceeded, cpc.customer_cost, cpc.reseller_cost, cpc.cdr_count " .
		"FROM accounting.cdr_period_costs as cpc WHERE " .
		"cpc.id = LAST_INSERT_ID()";

	$sth_upsert_cdr_period_costs = $acctdbh->prepare(
		$upsert_cdr_period_costs_stmt
	) or FATAL "Error preparing upsert cdr period costs statement: ".$acctdbh->errstr;

	$sth_get_cdr_period_costs = $acctdbh->prepare(
		$get_cdr_period_costs_stmt
	) or FATAL "Error preparing get cdr period costs statement: ".$acctdbh->errstr;

	$sth_mos_data = $acctdbh->prepare(
		"SELECT * ".
		"FROM accounting.cdr_mos_data WHERE cdr_id = ?"
	) or FATAL "Error preparing mos data statement: ".$acctdbh->errstr;

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

	$sth_lock_billing_subscribers = $billdbh->prepare(
		"UPDATE billing.voip_subscribers SET status = 'locked' WHERE contract_id = ? AND status = 'active'"
	) or FATAL "Error preparing lock billing subscribers statement: ".$billdbh->errstr;

	$sth_unlock_billing_subscribers = $billdbh->prepare(
		"UPDATE billing.voip_subscribers SET status = 'active' WHERE contract_id = ? AND status = 'locked'"
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
		$acc_tag_col_model_key,
		'local');

	if ($dupdbh) {
		$sth_duplicate_cdr = $dupdbh->prepare(
			'insert into cdr ('.
			join(',', @cdr_fields).
			') values ('.
			join(',', (map {'?'} @cdr_fields)).
			')'
		) or FATAL "Error preparing duplicate_cdr statement: ".$dupdbh->errstr;

		$sth_duplicate_mos_data = $dupdbh->prepare(
			'insert into cdr_mos_data ('.
			join(',', 'cdr_id',@mos_data_fields,'cdr_start_time').
			') values (?,'.
			join(',', (map {'?'} @mos_data_fields)).
			',?) ON DUPLICATE KEY UPDATE ' . join(',',map { $_ . ' = ?'; } @mos_data_fields)
		) or FATAL "Error preparing duplicate_mos_data statement: ".$dupdbh->errstr;

		$sth_duplicate_upsert_cdr_period_costs = $dupdbh->prepare(
			$upsert_cdr_period_costs_stmt
		) or FATAL "Error preparing duplicate upsert cdr period costs statement: ".$dupdbh->errstr;

		$sth_duplicate_get_cdr_period_costs = $dupdbh->prepare(
			$get_cdr_period_costs_stmt
		) or FATAL "Error preparing duplicate get cdr period costs statement: ".$dupdbh->errstr;

		prepare_cdr_col_models($dupdbh,
		$dup_cash_balance_col_model_key,
		$dup_time_balance_col_model_key,
		$dup_relation_col_model_key,
		$dup_tag_col_model_key,
		'duplication');
	}

	foreach (keys %cdr_col_models) {
		init_cdr_col_model($_);
	}

	return 1;

}

sub prepare_cdr_col_models {

	my $dbh = shift;
	my $cash_balance_col_model_key = shift;
	my $time_balance_col_model_key = shift;
	my $relation_col_model_key = shift;
	my $tag_col_model_key = shift;
	my $description_prefix = shift;

	prepare_cdr_col_model($dbh,$cash_balance_col_model_key,$description_prefix.' cdr cash balance column model',$description_prefix,
		[ 'provider', 'direction', 'cash_balance' ], # avoid using Tie::IxHash
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
				"  (cdr_id,cdr_start_time,provider_id,direction_id,cash_balance_id,val_before,val_after) VALUES".
				"  (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val_before = ?, val_after = ?",
			description => "write $description_prefix cdr cash balance col data",
		},{
			sql => "SELECT val_before, val_after FROM accounting.cdr_cash_balance_data".
				"  WHERE cdr_id = ? AND provider_id = ? AND direction_id = ? AND cash_balance_id = ?",
			description => "read $description_prefix cdr cash balance col data",
		}
	);

	prepare_cdr_col_model($dbh,$time_balance_col_model_key,$description_prefix.' cdr time balance column model',$description_prefix,
		[ 'provider', 'direction', 'time_balance' ],
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
				"  (cdr_id,cdr_start_time,provider_id,direction_id,time_balance_id,val_before,val_after) VALUES".
				"  (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val_before = ?, val_after = ?",
			description => "write $description_prefix cdr time balance col data",
		},{
			sql => "SELECT val_before, val_after FROM accounting.cdr_time_balance_data".
				"  WHERE cdr_id = ? AND provider_id = ? AND direction_id = ? AND time_balance_id = ?",
			description => "read $description_prefix cdr time balance col data",
		}
	);

	prepare_cdr_col_model($dbh,$relation_col_model_key,$description_prefix.' cdr relation column model',$description_prefix,
		[ 'provider', 'direction', 'relation' ],
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
				"  (cdr_id,cdr_start_time,provider_id,direction_id,relation_id,val) VALUES".
				"  (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val = ?",
			description => "write $description_prefix cdr relation col data",
		},{
			sql => "SELECT val FROM accounting.cdr_relation_data".
				"  WHERE cdr_id = ? AND provider_id = ? AND direction_id = ? AND relation_id = ?",
			description => "read $description_prefix cdr relation col data",
		}
	);

	prepare_cdr_col_model($dbh,$tag_col_model_key,$description_prefix.' cdr tag column model',$description_prefix,
		[ 'provider', 'direction', 'tag' ],
		{
			provider => {
				sql => 'SELECT * FROM accounting.cdr_provider',
				description => "get $description_prefix cdr provider cols",
			},
			direction => {
				sql => 'SELECT * FROM accounting.cdr_direction',
				description => "get $description_prefix cdr direction cols",
			},
			tag => {
				sql => 'SELECT * FROM accounting.cdr_tag',
				description => "get $description_prefix tag cols",
			},
		},{
			sql => "INSERT INTO accounting.cdr_tag_data".
				"  (cdr_id,cdr_start_time,provider_id,direction_id,tag_id,val) VALUES".
				"  (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ".
				"val = ?",
			description => "write $description_prefix cdr tag col data",
		},{
			sql => "SELECT val FROM accounting.cdr_tag_data".
				"  WHERE cdr_id = ? AND provider_id = ? AND direction_id = ? AND tag_id = ?",
			description => "read $description_prefix cdr tag col data",
		}
	);

}

sub lock_cdr {

	my $cdr = shift;
	my $sth = $sth_lock_cdr;
	$sth->execute($cdr->{id})
		or FATAL "Error executing cdr row lock selection statement: ".$sth->errstr;
	my ($id,$rating_status) = $sth->fetchrow_array;
	$sth->finish;
	return $rating_status;

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
	# statement, we need to determine the 4 contract ids saparately
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
		FATAL "Invalid interval unit '$unit' in $src";
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
						INFO "'$attribute' usr preference value ID $val_id with value '$old_value' deleted";
					} else {
						$sth = $sth_update_usr_preference_value;
						$sth->execute($new_value,$val_id)
							or FATAL "Error executing update usr preference value statement: ".$sth->errstr;
						$changed++;
						INFO "'$attribute' usr preference value ID $val_id updated from old value '$old_value' to new value '$new_value'";
					}
				}
			} elsif ($new_value > 0) {
				if ($readonly) {
					WARNING "creating '$attribute' usr preference value '$new_value' skipped for prov subscriber ID $prov_subs_id";
				} else {
					$sth = $sth_create_usr_preference_value;
					$sth->execute($prov_subs_id,$attr_id,$new_value)
						or FATAL "Error executing create usr preference value statement: ".$sth->errstr;
					$changed++;
					INFO "'$attribute' usr preference value ID ".$provdbh->{'mysql_insertid'}." with value '$new_value' created";
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

sub add_profile_mappings {

	my $contract_id = shift;
	my $stime = shift;
	my $package_id = shift;
	my $profiles = shift;

	$billdbh->do("CALL billing.create_contract_billing_profile_network_from_package(?,?,?,?)",undef,$contract_id,$stime,$package_id,$profiles)
		or FATAL "Error executing create billing mappings statement: ".$DBI::errstr;

}

sub add_period_costs {

	my $dup = shift;
	my $cdr_id = shift;
	my $contract_id = shift;
	my $stime = shift;
	my $duration = shift;
	my $billing_profile_id = shift;
	my $customer_cost = shift;
	my $reseller_cost = shift;

	$sth_profile_fraud_info->execute($billing_profile_id)
		or FATAL "Error executing profile fraud info statement: ".$sth_profile_fraud_info->errstr;
	my ($profile_fraud_interval_limit,
		$profile_fraud_daily_limit,
		$profile_fraud_interval_lock,
		$profile_fraud_daily_lock,
		$fraud_use_reseller_rates) = $sth_profile_fraud_info->fetchrow_array();

	$sth_contract_fraud_info->execute($contract_id)
		or FATAL "Error executing contracts fraud info statement: ".$sth_contract_fraud_info->errstr;
	my ($contract_fraud_interval_limit,
		$contract_fraud_daily_limit,
		$contract_fraud_interval_lock,
		$contract_fraud_daily_lock) = $sth_contract_fraud_info->fetchrow_array();

	my ($month_period_date,$day_period_date);
	{
		my ($y, $m, $d, $H, $M, $S) = (localtime(ceil($stime + $duration)))[5,4,3,2,1,0];
		$y += 1900;
		$m += 1;
		$day_period_date = sprintf('%04d-%02d-%02d', $y, $m, $d);
		$month_period_date = sprintf('%04d-%02d-01', $y, $m);
	}
	my $direction = "out";
	my ($fraud_limit_type,$fraud_limit,$month_lock,$daily_lock);
	my ($upsert_sth, $get_sth);
	if ($dup) {
		$upsert_sth = $sth_duplicate_upsert_cdr_period_costs;
		$get_sth = $sth_duplicate_get_cdr_period_costs;
	} else {
		$upsert_sth = $sth_upsert_cdr_period_costs;
		$get_sth = $sth_get_cdr_period_costs;
	}
	if (defined $contract_fraud_interval_limit and $contract_fraud_interval_limit > 0.0) {
		$fraud_limit = $contract_fraud_interval_limit;
		$fraud_limit_type = "contract";
		$month_lock = $contract_fraud_interval_lock;
	} elsif (defined $profile_fraud_interval_limit and $profile_fraud_interval_limit > 0.0) {
		$fraud_limit = $profile_fraud_interval_limit;
		$fraud_limit_type = "billing_profile";
		$month_lock = $profile_fraud_interval_lock;
	} else {
		$fraud_limit = undef;
		$fraud_limit_type = undef;
		$month_lock = undef;
	}

	my @bind_params = ($contract_id,
		"month",
		$month_period_date,
		$direction,
		$customer_cost,
		$reseller_cost,

		$fraud_use_reseller_rates,
		$reseller_cost, $fraud_limit,
		$customer_cost, $fraud_limit,

		$fraud_limit_type,
		$stime,
		$cdr_id,
		$stime,
		$cdr_id,


		$fraud_use_reseller_rates,
		$reseller_cost, $fraud_limit,
		$customer_cost, $fraud_limit,

		$customer_cost,
		$reseller_cost,

		$fraud_limit_type,

		$stime,$stime,$cdr_id,$cdr_id,
		$stime,$stime,$cdr_id,$cdr_id,
	);

	DEBUG sub { "month fraud check: ".(Dumper {
		fraud_limit => $fraud_limit,
		fraud_limit_type => $fraud_limit_type,
		month_lock => $month_lock,
		bind => \@bind_params,
	}) };

	$upsert_sth->execute(
		@bind_params
	) or FATAL "Error executing upsert cdr month period costs statement: ".$upsert_sth->errstr;

	$get_sth->execute() or FATAL "Error executing get cdr day period costs statement: ".$get_sth->errstr;
	my ($month_limit_exceeded,$month_customer_cost,$month_reseller_cost,$month_cdr_count) = $get_sth->fetchrow_array();
	if ($month_limit_exceeded) {
		INFO "contract ID $contract_id month period costs $month_customer_cost (customer), $month_reseller_cost (reseller) exceed $fraud_limit_type limit of $fraud_limit ($month_cdr_count cdrs)";
	} else {
		$month_lock = undef;
		DEBUG "contract ID $contract_id month period costs $month_customer_cost (customer), $month_reseller_cost (reseller) ($month_cdr_count cdrs)";
	}

	if (defined $contract_fraud_daily_limit and $contract_fraud_daily_limit > 0.0) {
		$fraud_limit = $contract_fraud_daily_limit;
		$fraud_limit_type = "contract";
		$daily_lock = $contract_fraud_daily_lock;
	} elsif (defined $profile_fraud_daily_limit and $profile_fraud_daily_limit > 0.0) {
		$fraud_limit = $profile_fraud_daily_limit;
		$fraud_limit_type = "billing_profile";
		$daily_lock = $profile_fraud_daily_lock;
	} else {
		$fraud_limit = undef;
		$fraud_limit_type = undef;
		$daily_lock = undef;
	}

	@bind_params = (
		$contract_id,
		"day",
		$day_period_date,
		$direction,
		$customer_cost,
		$reseller_cost,

		$fraud_use_reseller_rates,
		$reseller_cost, $fraud_limit,
		$customer_cost, $fraud_limit,

		$fraud_limit_type,
		$stime,
		$cdr_id,
		$stime,
		$cdr_id,

		$fraud_use_reseller_rates,
		$reseller_cost, $fraud_limit,
		$customer_cost, $fraud_limit,

		$customer_cost,
		$reseller_cost,

		$fraud_limit_type,

		$stime,$stime,$cdr_id,$cdr_id,
		$stime,$stime,$cdr_id,$cdr_id,
	);

	DEBUG sub { "day fraud check: ".(Dumper {
		fraud_limit => $fraud_limit,
		fraud_limit_type => $fraud_limit_type,
		daily_lock => $daily_lock,
		bind => \@bind_params,
	}) };

	$upsert_sth->execute(
		@bind_params
	) or FATAL "Error executing upsert cdr day period costs statement: ".$upsert_sth->errstr;

	$get_sth->execute() or FATAL "Error executing get cdr day period costs statement: ".$get_sth->errstr;
	my ($day_limit_exceeded,$day_customer_cost,$day_reseller_cost,$day_cdr_count) = $get_sth->fetchrow_array();
	if ($day_limit_exceeded) {
		INFO "contract ID $contract_id day period costs $day_customer_cost (customer), $day_reseller_cost (reseller) exceed $fraud_limit_type limit of $fraud_limit ($day_cdr_count cdrs)";
	} else {
		$daily_lock = undef;
		DEBUG "contract ID $contract_id day period costs $day_customer_cost (customer), $day_reseller_cost (reseller) ($day_cdr_count cdrs)";
	}

	return $month_lock // $daily_lock;

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

	my $cdr = shift;
	my $call_start_time = shift;
	my $call_end_time = shift;
	my $contract_id = shift;
	my $r_package_info = shift;

	DEBUG "catching up contract ID $contract_id balance rows";

	my $sth = $sth_get_contract_info;
	$sth->execute($contract_id) or FATAL "Error executing get info statement: ".$sth->errstr;
	my ($create_time,$modify,$contact_reseller_id,$package_id,$interval_unit,$interval_value,
		$start_mode,$carry_over_mode,$notopup_discard_intervals,$underrun_profile_threshold,
		$underrun_lock_threshold,$underrun_lock_level,$underrun_profiles_count,$class) = $sth->fetchrow_array();
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
	if (("create" eq $start_mode or "create_tz" eq $start_mode) && defined $create_time) {
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
				DEBUG sub { "last ratio = " . ($last_end + 1 - $create_time_aligned) . ' / ' . ($last_end + 1 - $last_start) . ", create_time = $create_time, create_time_aligned = $create_time_aligned"; };
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
				DEBUG sub { "cash balance = $cash_balance, last_cash_balance_int = $last_cash_balance_int, old_free_cash = $old_free_cash"; };
				$cash_balance += $last_cash_balance_int - $old_free_cash;
				DEBUG sub { "free cash refill: " . (($last_cash_balance_int - $old_free_cash) + ($profile->{int_free_cash} // 0.0)); };
			}
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
			if ($underrun_profiles_count > 0) {
				add_profile_mappings($contract_id,$stime,$package_id,'underrun',0);
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
				DEBUG "cash balance was decreased from $last_cash_balance to 0 and dropped below underrun lock threshold $underrun_lock_threshold";
				if (defined $underrun_lock_level) {
					set_subscriber_lock_level($contract_id,$underrun_lock_level,0);
					set_subscriber_status($contract_id,$underrun_lock_level,0);
					$bal->{underrun_lock_time} = $now;
				}
			}

			if (!$underrun_profiles_applied && defined $underrun_profile_threshold && $last_cash_balance >= $underrun_profile_threshold && 0.0 < $underrun_profile_threshold) {
				$underrun_profiles_applied = 1;
				DEBUG "cash balance was decreased from $last_cash_balance to 0 and dropped below underrun profile threshold $underrun_profile_threshold";
				if ($underrun_profiles_count > 0) {
					add_profile_mappings($contract_id,$call_start_time,$package_id,'underrun',0);
					$underrun_profiles_time = $now;
					$bal->{underrun_profile_time} = $now;
				}
			}
			update_contract_balance($cdr,[$bal])
				or FATAL "Error updating customer contract balance\n";
		}
	}

	$r_package_info->{id} = $package_id;
	$r_package_info->{class} = $class;
	$r_package_info->{underrun_profile_threshold} = $underrun_profile_threshold;
	$r_package_info->{underrun_lock_threshold} = $underrun_lock_threshold;
	$r_package_info->{underrun_lock_level} = $underrun_lock_level;
	$r_package_info->{underrun_lock_applied} = $underrun_lock_applied;
	$r_package_info->{underrun_profiles_applied} = $underrun_profiles_applied;
	$r_package_info->{underrun_profiles_count} = $underrun_profiles_count;

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

	catchup_contract_balance($cdr,int($start_time),int($start_time + $duration),$contract_id,$r_package_info);

	my $sth = $sth_get_cbalances;
	$sth->execute($contract_id, int($start_time))
		or FATAL "Error executing get contract balance statement: ".$sth->errstr;
	my $res = $sth->fetchall_arrayref({});
	$sth->finish;

	foreach my $bal (@$res) {
		# restore balances & create balances savepoint:
		$bal->{cash_balance} -= (get_balance_delta($cdr, $bal->{id}, "cash_balance") // 0.0);
		$bal->{cash_balance_old} = $bal->{cash_balance};
		$bal->{free_time_balance} -= (get_balance_delta($cdr, $bal->{id}, "free_time_balance") // 0);
		$bal->{free_time_balance_old} = $bal->{free_time_balance};
		$bal->{cash_balance_interval} -= (get_balance_delta($cdr, $bal->{id}, "cash_balance_interval") // 0.0);
		$bal->{cash_balance_interval_old} = $bal->{cash_balance_interval};
		$bal->{free_time_balance_interval} -= (get_balance_delta($cdr, $bal->{id}, "free_time_balance_interval") // 0);
		$bal->{free_time_balance_interval_old} = $bal->{free_time_balance_interval};
		push(@$r_balances,$bal);
	}

	return scalar @$res;

}

sub update_contract_balance {

	my $cdr = shift;
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
		set_balance_delta($cdr, $bal->{id}, "cash_balance", ($bal->{cash_balance} // 0.0) - ($bal->{cash_balance_old} // 0.0));
		set_balance_delta($cdr, $bal->{id}, "cash_balance_interval", $bal->{cash_balance_interval} - $bal->{cash_balance_interval_old});
		set_balance_delta($cdr, $bal->{id}, "free_time_balance", ($bal->{free_time_balance} // 0) - ($bal->{free_time_balance_old} // 0));
		set_balance_delta($cdr, $bal->{id}, "free_time_balance_interval", $bal->{free_time_balance_interval} - $bal->{free_time_balance_interval_old});
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
		$sth = $sth_billing_info_network;
		$sth->execute($contract_id, $start, $source_ip) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
		$label = " and address $source_ip";
	} else {
		$sth = $sth_billing_info;
		$sth->execute($contract_id, $start) or
			FATAL "Error executing billing info statement: ".$sth->errstr;
		$label = "";
	}

	my @res = $sth->fetchrow_array();
	FATAL "No billing info found for contract_id $contract_id\n" unless @res;

	$r_info->{contract_id} = $contract_id;
	$r_info->{profile_id} = $res[0];
	$r_info->{prepaid} = $res[1];
	$r_info->{int_charge} = $res[2];
	$r_info->{int_free_time} = $res[3];
	$r_info->{int_free_cash} = $res[4];
	$r_info->{int_unit} = $res[5];
	$r_info->{int_count} = $res[6];
	$r_info->{ignore_domain} = $res[7];

	DEBUG "contract ID $contract_id billing mapping is profile id $r_info->{profile_id} for time $start" . $label;

	$sth->finish;

	return 1;

}

sub get_profile_info {

	my $bpid = shift;
	my $type = shift;
	my $direction = shift;
	my $source = shift;
	my $destination = shift;
	my $lnp_number = shift; #force lnp fee lookup
	my $b_info = shift;
	my $start_time = shift;

	my @res;

	if (defined $lnp_number and $lnp_number =~ /^\d+$/) {
		# let's see if we find the number in our LNP database
		$sth_lnp_number->execute($lnp_number, $start_time)
			or FATAL "Error executing LNP number statement: ".$sth_lnp_number->errstr;
		my ($lnppid,$lnpnumbertype) = $sth_lnp_number->fetchrow_array();

		if ($lnppid) {
			# let's see if we have a billing fee entry for the LNP provider ID
			$sth_lnp_profile_info->execute($bpid, $type, $direction, 'lnp:'.$lnppid)
				or FATAL "Error executing LNP profile info statement: ".$sth_lnp_profile_info->errstr;
			@res = $sth_lnp_profile_info->fetchrow_array();
			FATAL "Error fetching LNP profile info: ".$sth_lnp_profile_info->errstr
				if $sth_lnp_profile_info->err;

			unless (@res) {
				if (length($lnpnumbertype)) {
					$sth_lnp_profile_info->execute($bpid, $type, $direction, 'lnpnumbertype:'.$lnpnumbertype)
						or FATAL "Error executing LNP profile info statement: ".$sth_lnp_profile_info->errstr;
					@res = $sth_lnp_profile_info->fetchrow_array();
					FATAL "Error fetching LNP profile info: ".$sth_lnp_profile_info->errstr
						if $sth_lnp_profile_info->err;
				}
			}
		}
	}

	my $sth = $sth_profile_info;

	unless (@res) {
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
	$b_info->{off_use_free_time} = $res[12];
	$b_info->{on_use_free_time} = $res[13];
	$b_info->{on_extra_second} = $res[14];
	$b_info->{on_extra_rate} = $res[15];
	$b_info->{off_extra_second} = $res[16];
	$b_info->{off_extra_rate} = $res[17];

	$sth->finish;

	return 1;
}

sub get_offpeak {

	my $bpid = shift;
	my $subscriber_contract_id = shift;
	my $start = shift;
	my $duration = shift;
	my $r_offpeaks = shift;

	my $sth;

	if ($subscriber_offpeak_tz) {
		$sth = $sth_offpeak_subscriber;
		$sth->execute(
			$subscriber_contract_id,$subscriber_contract_id,
			$start,
			$start,$duration,
			$bpid,
			$subscriber_contract_id,$subscriber_contract_id,
			$bpid,
			$start,$duration,
			$start
		) or FATAL "Error executing offpeak subscriber statement: ".$sth->errstr;
	} else {
		$sth = $sth_offpeak;
		$sth->execute(
			$start,
			$start,$duration,
			$bpid,
			$bpid,
			$start,$duration,
			$start
		) or FATAL "Error executing offpeak statement: ".$sth->errstr;
	}

	while(my @res = $sth->fetchrow_array())
	{
		my %e = ();
		$e{start} = $res[0];
		$e{end} = $res[1];
		push @$r_offpeaks, \%e;
	}

	return 1;

}

sub is_offpeak {

	my $start = shift;
	my $offset = shift;
	my $r_offpeaks = shift;

	my $secs = $start + $offset; # we have unix-timestamp as reference

	foreach my $r_o(@$r_offpeaks) {
		return 1 if($secs >= $r_o->{start} && $secs <= $r_o->{end});
	}

	return 0;

}

sub get_start_time {
	my $cdr = shift;

	if ($cdr->{is_fragmented}) {
		my $id;
		while (($id) = get_cdr_col_data($acc_relation_col_model_key,$cdr->{id},
				{ direction => 'source', provider => 'customer', relation => 'prev_fragment_id' })) {
			$sth_get_cdr->execute($id) or FATAL "Error executing get cdr statement: ".$sth_get_cdr->errstr;
			$cdr = $sth_get_cdr->fetchrow_hashref();
			WARNING "missing cdr fragment ID $id" unless $cdr;
		}
		DEBUG "first cdr fragment ID is $id" if $id;
	}

	return $cdr->{start_time};
}

sub check_shutdown {

	if ($shutdown) {
		WARNING 'Shutdown detected, aborting work in progress';
		return 1;
	}
	return 0;

}

sub get_unrated_cdrs {
	my $r_cdrs = shift;

	my $sth = $sth_unrated_cdrs;
	$sth->execute or die("Error executing unrated cdr statement: ".$sth->errstr);

	my @cdrs = ();

	my $nodename = get_hostname();
	#set to undef if corosync reports there is no other working node left:
	#$nodename = undef

	while (my $cdr = $sth->fetchrow_hashref()) {
		if (not $multi_master or not length($nodename) or $nodename eq 'spce') {
			push(@cdrs,$cdr);
		} elsif (substr($nodename,-1,1) eq '1' or substr($nodename,-1,1) eq 'a') {
			push(@cdrs,$cdr) if (
				(($cdr->{id} % 2) == 1
				and ($cdr->{id} % 4) == 3)
				or
				(($cdr->{id} % 2) == 0
				and ($cdr->{id} % 4) == 2)
			);
		} elsif (substr($nodename,-1,1) eq '2' or substr($nodename,-1,1) eq 'b') {
			push(@cdrs,$cdr) if (
				(($cdr->{id} % 2) == 1
				and ($cdr->{id} % 4) == 1)
				or
				(($cdr->{id} % 2) == 0
				and ($cdr->{id} % 4) == 0)
			);
		} else {
			push(@cdrs,$cdr);
			INFO "Unknown hostname '$nodename'";
		}
		check_shutdown() and return 0;
	}

	# the while above may have been interrupted because there is no
	# data left, or because there was an error. To decide what
	# happened, we have to query $sth->err()
	die("Error fetching unrated cdr's: ". $sth->errstr) if $sth->err;
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

sub get_balance_delta_field {
	my $field = shift;
	return unless $field;
	return 'cb' if $field eq 'cash_balance';
	return 'cbi' if $field eq 'cash_balance_interval';
	return 'ftb' if $field eq 'free_time_balance';
	return 'ftbi' if $field eq 'free_time_balance_interval';
}

sub get_balance_delta {

	my $cdr = shift;
	my $bal_id = shift;
	my $field = shift;
	unless ($cdr->{balance_delta_old}) {
		($cdr->{balance_delta_old}) = get_cdr_col_data($acc_tag_col_model_key,$cdr->{id},
			{ direction => 'source', provider => 'customer', tag => 'balance_delta' });
		if ($cdr->{balance_delta_old}) {
			my $deserialized = decode_json($cdr->{balance_delta_old});
			$cdr->{balance_delta_old} = $deserialized;
		}
		$cdr->{balance_delta_old} //= {};
	}
	if ($bal_id and $field = get_balance_delta_field($field)
		and exists $cdr->{balance_delta_old}->{$bal_id}
		and exists $cdr->{balance_delta_old}->{$bal_id}->{$field}) {
		return $cdr->{balance_delta_old}->{$bal_id}->{$field};
	}
	return;

}

sub set_balance_delta {

	my $cdr = shift;
	my $bal_id = shift;
	my $field = shift;
	my $val = shift;

	return unless $val;
	return unless $bal_id;
	return unless $field = get_balance_delta_field($field);

	unless ($cdr->{balance_delta}) {
		$cdr->{balance_delta} = {};
	}
	unless ($cdr->{balance_delta}->{$bal_id}) {
		$cdr->{balance_delta}->{$bal_id} = {};
	}
	$cdr->{balance_delta}->{$bal_id}->{$field} = $val;

}

sub save_balance_delta {

	my $cdr = shift;
	if ($cdr->{balance_delta}) {
		my $serialized = encode_json($cdr->{balance_delta});
		return write_cdr_col_data($acc_tag_col_model_key,$cdr,$cdr->{id},
			{ direction => 'source', provider => 'customer', tag => 'balance_delta' }, $serialized);
	}
	return 0;

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
		my $fraud_lock;
		if (not $dupdbh
			and $cdr->{source_account_id}) {
			unless ($cdr->{source_customer_billing_profile_id}) {
				my %billing_info = ();
				get_billing_info($cdr->{start_time}, $cdr->{source_account_id}, $cdr->{source_ip}, \%billing_info) or
					FATAL "Error getting source_customer billing info\n";
				$cdr->{source_customer_billing_profile_id} = $billing_info{profile_id};
			}
			$fraud_lock = add_period_costs(0,
				$cdr->{id},
				$cdr->{source_account_id},
				$cdr->{start_time},
				$cdr->{duration},
				$cdr->{source_customer_billing_profile_id},
				-1.0 * ($cdr->{source_customer_cost_old} || 0.0) + $cdr->{source_customer_cost},
				-1.0 * ($cdr->{source_reseller_cost_old} || 0.0) + $cdr->{source_reseller_cost},
			) if $cdr->{source_customer_billing_profile_id};
		}
		write_cdr_cols($cdr,$cdr->{id},
			$acc_cash_balance_col_model_key,
			$acc_time_balance_col_model_key,
			$acc_relation_col_model_key,
			$acc_tag_col_model_key);
		save_balance_delta($cdr);
		if ($dupdbh) {
			$sth_duplicate_cdr->execute(@$cdr{@cdr_fields})
			or FATAL "Error executing duplicate cdr statement: ".$sth_duplicate_cdr->errstr;
			my $dup_cdr_id = $dupdbh->{'mysql_insertid'};
			if ($dup_cdr_id) {
				DEBUG "local cdr ID $cdr->{id} was duplicated to duplication cdr ID $dup_cdr_id";
				if ($cdr->{source_account_id}) {
					unless ($cdr->{source_customer_billing_profile_id}) {
						my %billing_info = ();
						get_billing_info($cdr->{start_time}, $cdr->{source_account_id}, $cdr->{source_ip}, \%billing_info) or
							FATAL "Error getting source_customer billing info\n";
						$cdr->{source_customer_billing_profile_id} = $billing_info{profile_id};
					}
					$fraud_lock = add_period_costs(1,
						$dup_cdr_id,
						$cdr->{source_account_id},
						$cdr->{start_time},
						$cdr->{duration},
						$cdr->{source_customer_billing_profile_id},
						-1.0 * ($cdr->{source_customer_cost_old} || 0.0) + $cdr->{source_customer_cost},
						-1.0 * ($cdr->{source_reseller_cost_old} || 0.0) + $cdr->{source_reseller_cost},
					) if $cdr->{source_customer_billing_profile_id};
				}
				write_cdr_cols($cdr,$dup_cdr_id,
					$dup_cash_balance_col_model_key,
					$dup_time_balance_col_model_key,
					$dup_relation_col_model_key,
					$dup_tag_col_model_key);

				copy_cdr_col_data($acc_tag_col_model_key,$dup_tag_col_model_key,$cdr,$cdr->{id},$dup_cdr_id,
					{ direction => 'destination', provider => 'customer', tag => 'furnished_charging_info' });

				copy_cdr_col_data($acc_tag_col_model_key,$dup_tag_col_model_key,$cdr,$cdr->{id},$dup_cdr_id,
					{ direction => 'source', provider => 'customer', tag => 'header=P-Asserted-Identity' });

				copy_cdr_col_data($acc_tag_col_model_key,$dup_tag_col_model_key,$cdr,$cdr->{id},$dup_cdr_id,
					{ direction => 'source', provider => 'customer', tag => 'header=P-Preferred-Identity' });

				copy_cdr_col_data($acc_tag_col_model_key,$dup_tag_col_model_key,$cdr,$cdr->{id},$dup_cdr_id,
					{ direction => 'destination', provider => 'customer', tag => 'header=Diversion' });

				copy_cdr_col_data($acc_tag_col_model_key,$dup_tag_col_model_key,$cdr,$cdr->{id},$dup_cdr_id,
					{ direction => 'destination', provider => 'customer', tag => 'hg_ext_response' });

				copy_cdr_mos_data($cdr,$cdr->{id},$dup_cdr_id);

			} else {
				FATAL "cdr ID $cdr->{id} and col data could not be duplicated";
			}
		}
		if (defined $fraud_lock and $fraud_lock > 0) {
			set_subscriber_lock_level($cdr->{source_account_id},$fraud_lock,not $apply_fraud_lock);
			set_subscriber_status($cdr->{source_account_id},$fraud_lock,not $apply_fraud_lock);
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
	my $tag_col_model_key = shift;

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

			write_cdr_col_data($tag_col_model_key,$cdr,$cdr_id,
				{ direction => $dir, provider => $provider, tag => 'extra_rate' },
				$cdr->{$dir.'_'.$provider."_extra_rate"});

		}
	}

}

sub get_call_cost {

	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $contract_id = shift;
	my $subscriber_contract_id = shift;
	my $profile_id = shift;
	my $ignore_domain = shift;
	my $readonly = shift;
	my $prepaid = shift;
	my $r_profile_info = shift;
	my $r_package_info = shift;
	my $r_cost = shift;
	my $r_real_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $r_onpeak = shift;
	my $r_extra_rate = shift;
	my $r_balances = shift;

	my $src_user;
	if($offnet_anonymous_source_cli_fallback
		and $cdr->{source_user_id} eq "0"
		and $cdr->{source_cli} =~ /anonymous/i
		and $cdr->{source_user} =~ /^[+ 0-9]+$/) {
		$src_user = $cdr->{source_user};
	} else {
		$src_user = $cdr->{source_cli};
	}
	my $src_user_domain = $src_user.'@'.$cdr->{source_domain};
	my $dst_user = $cdr->{destination_user_in};
	my $dst_user_domain = $cdr->{destination_user_in}.'@'.$cdr->{destination_domain};

	DEBUG "calculating call cost for profile_id $profile_id with type $type, direction $direction, ".
		"src_user_domain $src_user_domain, dst_user_domain $dst_user_domain" unless $ignore_domain;

	if($ignore_domain or not get_profile_info($profile_id, $type, $direction, $src_user_domain, $dst_user_domain, $dst_user,
		$r_profile_info, $cdr->{start_time})) {
		DEBUG "trying user only for profile_id $profile_id with type $type, direction $direction, ".
			"src_user_domain $src_user, dst_user_domain $dst_user";
		unless(get_profile_info($profile_id, $type, $direction, $src_user, $dst_user, undef,
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

	my @offpeak = ();
	get_offpeak($profile_id, $subscriber_contract_id, $cdr->{_start_time},
		$cdr->{start_time} - $cdr->{_start_time} + $cdr->{duration}, \@offpeak) or
		FATAL "Error getting offpeak info\n";
	DEBUG sub { "offpeak info: " . Dumper \@offpeak; };

	$$r_cost = 0;
	$$r_real_cost = 0;
	$$r_free_time = 0;
	my $interval = 0;
	my $rate = 0;
	my $offset = 0;
	my $onpeak = 0;
	my $init = $cdr->{is_fragmented} // 0;
	my $extra_second;
	my $extra_rate;
	my $use_free_time;
	if (is_offpeak($cdr->{_start_time}, 0, \@offpeak)) {
		$extra_second = $r_profile_info->{off_extra_second};
		$extra_rate = $r_profile_info->{off_extra_rate} // 0.0;
		$use_free_time = $r_profile_info->{off_use_free_time};
	} else {
		$extra_second = $r_profile_info->{on_extra_second};
		$extra_rate = $r_profile_info->{on_extra_rate} // 0.0;
		$use_free_time = $r_profile_info->{on_use_free_time};
	}
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
		if(is_offpeak($cdr->{start_time}, $offset, \@offpeak)) {
			$$r_onpeak = 0;
		} else {
			$$r_onpeak = 1;
		}
	}

	while ($duration > 0) {
		DEBUG "try to rate remaining duration of $duration secs";

		if(is_offpeak($cdr->{start_time}, $offset, \@offpeak)) {
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
			$interval = $onpeak == 1 ?
				$r_profile_info->{on_follow_interval} : $r_profile_info->{off_follow_interval};
			$rate = $onpeak == 1 ?
				$r_profile_info->{on_follow_rate} : $r_profile_info->{off_follow_rate};
			DEBUG "add follow rate $rate per sec to costs";
		}
		$$r_onpeak = $onpeak unless defined $$r_onpeak;
		if ($split_peak_parts #break the cdr, if
			and not defined $cdr->{rating_duration} #is the first attempt to calculate,
			and defined($$r_onpeak) #it started with onpeak or offpeak in the first interval,
			and $$r_onpeak != $onpeak) { #and switched onpeak/offpeak in the next interval
			DEBUG (($$r_onpeak ? 'onpeak' : 'offpeak').' -> '.($onpeak ? 'onpeak' : 'offpeak').' transition, rating_duration = ' . $$r_rating_duration);
			#$split = 1;
			last;
		}
		$rate *= $interval;
		DEBUG "interval is $interval, so rate for this interval is $rate";

		#my @bals = grep {($_->{start_unix} + $offset) <= $cdr->{start_time}} @$r_balances;
		my $current_call_time = int($cdr->{start_time} + $offset);
		my @bals = grep {
			$_->{start_unix} <= $current_call_time &&
			(is_infinite_unix($_->{end_unix}) || $current_call_time <= $_->{end_unix})
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

		if ($use_free_time && $bal->{free_time_balance} >= $interval) {
			DEBUG "subtracting $interval sec from free_time_balance $$bal{free_time_balance} and skip costs for this interval";
			$$r_rating_duration += $interval;
			$duration -= $interval;
			$bal->{free_time_balance} -= $interval;
			$bal->{free_time_balance_interval} += $interval;
			$$r_free_time += $interval;
			next;
		}

		if ($use_free_time && $bal->{free_time_balance} > 0) {
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

		if (defined $extra_second) {
			my $extra_second_time = int($cdr->{_start_time}) + $extra_second;
			if ($extra_second_time >= $current_call_time
				and $extra_second_time < ($current_call_time + $interval)
				and ($current_call_time + int($duration)) >= $extra_second_time
				and int($cdr->{start_time}) <= $extra_second_time) {
				DEBUG "add extra second ($extra_second) cost $extra_rate to rate $rate";
				$rate += $extra_rate;
				$$r_extra_rate = $extra_rate;
				undef $extra_second;
			}
		}

		if (($rate > 0 || $prepaid) and $rate <= $bal->{cash_balance}) {
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

	if (defined $cdr->{rating_duration} # we are in the second attempt,
		and $cdr->{rating_duration} >= $cdr->{duration} # must be last, final fragment,
		and $cdr->{rating_duration} > $$r_rating_duration) { # set $$r_rating_duration to the max rating duration, if its not
		DEBUG "set rating_duration from $$r_rating_duration to rating_duration = $cdr->{rating_duration}";
		$$r_rating_duration = $cdr->{rating_duration}; # will result in identical rating durations, and the fragment will pass.
	} else {
		DEBUG ("rating_duration = $$r_rating_duration");
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
			if (not $readonly and $r_package_info->{underrun_profiles_count} > 0) {
				add_profile_mappings($contract_id,$cdr->{start_time} + $cdr->{duration},$r_package_info->{id},'underrun');
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
	# strictly take it from the (scheduled) billing profile:
	my $prepaid = (defined $billing_info ? $billing_info->{prepaid} : undef);
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
					DEBUG "duplicate prepaid_costs call_id = $prepaid_cost->{call_id}, source_user_id = $prepaid_cost->{source_user_id}, destination_user_id = $prepaid_cost->{destination_user_id}";
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
				#last;
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
	my $read_stmt = shift;

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

	$model->{read_sth} = { description => $read_stmt->{description}, };
	$model->{read_sth}->{sth} = $dbh->prepare($read_stmt->{sql})
		or FATAL "Error preparing ".$read_stmt->{description}." statement: ".$dbh->errstr;

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
	#   DEBUG 'no '.$model->{description_prefix}.' col data written for cdr id '.$cdr_id.", column '$virtual_col_name': ".join(', ',@vals);
	}
	return $sth->rows;

}

sub get_cdr_col_data {

	my $col_model_key = shift;
	my $cdr_id = shift;
	my $lookup = shift;
	FATAL "unknown column model key $col_model_key" unless exists $cdr_col_models{$col_model_key};
	my $model = $cdr_col_models{$col_model_key};
	my @bind_parms = ($cdr_id);
	my $virtual_col_name = '';
	foreach my $dimension (@{$model->{dimensions}}) {
		my $dimension_value = $lookup->{$dimension};
		unless ($dimension_value) {
			FATAL "missing '$dimension' dimension for writing ".$model->{description_prefix}." col data of ".$model->{description};
		}
		my $dictionary = $model->{dimension_dictionaries}->{$dimension};
		my $dimension_value_lookup = $dictionary->{$dimension_value};
		unless ($dimension_value_lookup) {
			FATAL "unknown '$dimension' col name '$dimension_value' for reading ".$model->{description_prefix}." col data of ".$model->{description};
		}
		push(@bind_parms,$dimension_value_lookup->{id});
		$virtual_col_name .= '_' if length($virtual_col_name) > 0;
		$virtual_col_name .= $lookup->{$dimension};
	}

	my $sth = $model->{read_sth}->{sth};
	$sth->execute(@bind_parms) or FATAL "Error executing ".$model->{read_sth}->{description}."statement: ".$sth->errstr;
	my @vals = $sth->fetchrow_array;

	return @vals;

}

sub copy_cdr_col_data {

	my $src_col_model_key = shift;
	my $dst_col_model_key = shift;
	my $cdr = shift;
	my $src_cdr_id = shift;
	my $dst_cdr_id = shift;
	my $lookup = shift;

	my @vals = get_cdr_col_data($src_col_model_key,$src_cdr_id,$lookup);

	return write_cdr_col_data($dst_col_model_key,$cdr,$dst_cdr_id,$lookup,@vals);

}

sub copy_cdr_mos_data {

	my $cdr = shift;
	my $src_cdr_id = shift;
	my $dst_cdr_id = shift;

	my $row_count = 0;
	$sth_mos_data->execute($src_cdr_id) or FATAL "Error executing mos data statement: ".$sth_mos_data->errstr;
	while (my $mos_data = $sth_mos_data->fetchrow_hashref()) {
		my @bind_values = ($dst_cdr_id);
		foreach my $mos_data_field (@mos_data_fields) {
			push(@bind_values,$mos_data->{$mos_data_field});
		}
		push(@bind_values,$cdr->{start_time});
		foreach my $mos_data_field (@mos_data_fields) {
			push(@bind_values,$mos_data->{$mos_data_field});
		}
		$sth_duplicate_mos_data->execute(@bind_values) or FATAL "Error executing duplicate mos data statement: ".$sth_duplicate_mos_data->errstr;
		if ($sth_duplicate_mos_data->rows == 1) {
			DEBUG 'mos data created or up to date for cdr id '.$src_cdr_id.': '.join(', ',@bind_values);
		} elsif ($sth_duplicate_mos_data->rows > 1) {
			DEBUG 'mos data updated for cdr id '.$src_cdr_id.': '.join(', ',@bind_values);
		}
		$row_count += 1;
	}
	return $row_count;

}

sub get_hostname {

	return '' unless length($hostname_filepath);

	my $fh;
	if (not open($fh, '<', $hostname_filepath)) {
	  DEBUG 'cannot open file ' . $hostname_filepath . ': ' . $!;
	  return '';
	}
	my @linebuffer = <$fh>;
	close $fh;
	my $hostname = $linebuffer[0];
	chomp $hostname;
	return $hostname;

}

sub get_customer_call_cost {

	my $cdr = shift;
	my $type = shift;
	my $direction = shift;
	my $readonly = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;
	my $real_cost = 0;
	my $extra_rate;

	my $dir;
	if($direction eq "out") {
		$dir = "source_";
	} else {
		$dir = "destination_";
	}

	my $contract_id = get_subscriber_contract_id($cdr->{$dir."user_id"});
	my $subscriber_contract_id = ('0' eq $cdr->{$dir."user_id"} ? $cdr->{$dir."provider_id"} : $contract_id);

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

	$cdr->{$dir."customer_billing_profile_id"} = $billing_info{profile_id};

	my $prepaid = get_prepaid($cdr, \%billing_info, $dir.'user_');
	my $outgoing_prepaid = ($prepaid == 1 && $direction eq "out");
	my $prepaid_cost_entry = undef;
	if ($outgoing_prepaid) {
		DEBUG "billing profile is prepaid";
		populate_prepaid_cost_cache();
		$prepaid_cost_entry = get_prepaid_cost($cdr);
	}

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,$subscriber_contract_id,
		$billing_info{profile_id}, $billing_info{ignore_domain}, $readonly || ($outgoing_prepaid && defined $prepaid_cost_entry), $prepaid,
		\%profile_info, \%package_info, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, \$extra_rate, \@balances)
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
	$cdr->{$dir."customer_extra_rate"} = $extra_rate;

	if ($outgoing_prepaid) { #prepaid out
		# overwrite the calculated costs with the ones from our table
		if (defined $prepaid_cost_entry) {
			$$r_cost = $prepaid_cost_entry->{cost}; #prepaid: update balance AND show full costs
			$$r_free_time = $prepaid_cost_entry->{free_time_used};
			drop_prepaid_cost($prepaid_cost_entry) unless $readonly;

			# it would be more safe to add *_balance_before/after columns to the prepaid_costs table,
			# instead of reconstructing the balance values:
			$cdr->{$dir."customer_cash_balance_before"} = truncate_cash_balance($cdr->{$dir."customer_cash_balance_before"} * 1.0 + $prepaid_cost_entry->{cost});
			$cdr->{$dir."customer_free_time_balance_before"} = truncate_free_time_balance($cdr->{$dir."customer_free_time_balance_before"} * 1.0 + $prepaid_cost_entry->{free_time_used});

		} else {
			# maybe another rateomat was faster and already processed+deleted it?
			# in that case we should bail out here.
			WARNING "no prepaid cost record found for call ID $cdr->{call_id}, applying calculated costs";
			if ((not $readonly) and $prepaid_update_balance) {
				update_contract_balance($cdr,\@balances)
					or FATAL "Error updating ".$dir."customer contract balance\n";
			}
			$$r_cost = $real_cost; #prepaid: update balance AND show full costs
			$cdr->{$dir."customer_cash_balance_after"} = $snapshot_bal->{cash_balance};
			$cdr->{$dir."customer_free_time_balance_after"} = $snapshot_bal->{free_time_balance};
		}
	} else { #postpaid in, postpaid out, prepaid in
		# we don't do prepaid for termination fees for now, so treat it as post-paid
		if($prepaid == 1 && $direction eq "in") { #prepaid in
			DEBUG "treat pre-paid billing profile as post-paid for termination fees";
			$$r_cost = $real_cost; #prepaid: always update balance AND show full costs
		} else { #postpaid in, postpaid out
			DEBUG "billing profile is post-paid, update contract balance";
			if ($use_customer_real_cost) {
				$$r_cost = $real_cost;
			}
		}
		unless ($readonly) {
			update_contract_balance($cdr,\@balances)
				or FATAL "Error updating ".$dir."customer contract balance\n";
		}
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
	my $readonly = shift;
	my $provider_info = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;
	my $real_cost = 0;
	my $extra_rate;

	my $dir;
	if($direction eq "out") {
		$dir = "source_";
	} else {
		$dir = "destination_";
	}

	my $contract_id = $provider_info->{billing}->{contract_id};
	my $subscriber_contract_id = ('0' eq $cdr->{$dir."user_id"} ? $cdr->{$dir."provider_id"} : get_subscriber_contract_id($cdr->{$dir."user_id"}));

	unless($provider_info->{billing}->{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		DEBUG "no billing info for ".$dir."provider contract ID $contract_id, skip";
		return -1;
	}

	my $provider_type;
	if ($provider_info->{package}->{class} eq "reseller") {
		$provider_type = "reseller_";
	} else {
		$provider_type = "carrier_";
	}

	my $prepaid = get_prepaid($cdr, $provider_info->{billing},$dir.'provider_');

	my %profile_info = ();
	get_call_cost($cdr, $type, $direction,$contract_id,$subscriber_contract_id,
		$provider_info->{billing}->{profile_id}, $provider_info->{billing}->{ignore_domain}, $readonly || $prepaid, $prepaid, # no underruns for providers with prepaid profile
		\%profile_info, $provider_info->{package}, $r_cost, \$real_cost, $r_free_time,
		$r_rating_duration, \$onpeak, \$extra_rate, $provider_info->{balances})
		or FATAL "Error getting ".$dir."provider call cost\n";

	my $snapshot_bal = get_snapshot_contract_balance($provider_info->{balances});

	$cdr->{$dir.$provider_type."package_id"} = $provider_info->{package}->{id};
	$cdr->{$dir.$provider_type."contract_balance_id"} = $snapshot_bal->{id};

	$cdr->{$dir.$provider_type."billing_fee_id"} = $profile_info{fee_id};
	$cdr->{$dir.$provider_type."billing_zone_id"} = $profile_info{zone_id};
	$cdr->{'frag_'.$provider_type.'onpeak'} = $onpeak if $split_peak_parts;
	$cdr->{$dir.$provider_type."extra_rate"} = $extra_rate;

	unless($prepaid == 1) {
		$cdr->{$dir.$provider_type."cash_balance_before"} = $snapshot_bal->{cash_balance_old};
		$cdr->{$dir.$provider_type."free_time_balance_before"} = $snapshot_bal->{free_time_balance_old};
		$cdr->{$dir.$provider_type."cash_balance_after"} = $snapshot_bal->{cash_balance_old};
		$cdr->{$dir.$provider_type."free_time_balance_after"} = $snapshot_bal->{free_time_balance_old};

		unless ($readonly) {
			update_contract_balance($cdr,$provider_info->{balances})
				or FATAL "Error updating ".$dir.$provider_type."provider contract balance\n";
		}

		$cdr->{$dir.$provider_type."cash_balance_after"} = $snapshot_bal->{cash_balance};
		$cdr->{$dir.$provider_type."free_time_balance_after"} = $snapshot_bal->{free_time_balance};

	} else {
		WARNING $dir.$provider_type."provider is prepaid\n";
		# there are no prepaid cost records for providers, so we cannot
		# restore the original balance and leave the fields empty

		# no balance update for providers with prepaid profile
	}

	if ($use_provider_real_cost) {
		$$r_cost = $real_cost;
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

	$cdr->{source_user_id} = '0' if lc($cdr->{source_user_id}) eq '<null>';
	$cdr->{destination_user_id} = '0' if lc($cdr->{destination_user_id}) eq '<null>';

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

	$cdr->{_start_time} = get_start_time($cdr);

	my @rating_durations;
	my $rating_attempts = 0;
	my $readonly;
	$cdr->{rating_duration} = undef;
RATING_DURATION_FOUND:
	$rating_attempts += 1;
	@rating_durations = ();
	$source_customer_cost = 0;
	$source_carrier_cost = 0;
	$source_reseller_cost = 0;
	$source_customer_free_time = 0;
	$source_carrier_free_time = 0;
	$source_reseller_free_time = 0;
	$destination_customer_cost = 0;
	$destination_carrier_cost = 0;
	$destination_reseller_cost = 0;
	$destination_customer_free_time = 0;
	$destination_carrier_free_time = 0;
	$destination_reseller_free_time = 0;
	$source_provider_info->{balances} = dclone(\@source_provider_balances);
	$destination_provider_info->{balances} = dclone(\@destination_provider_balances);
	$readonly = ($split_peak_parts ? ($rating_attempts == 1) : 0);
	if ($readonly) {
		DEBUG "### $rating_attempts. readonly pass ###";
	} else {
		DEBUG "### $rating_attempts. write pass ###";
	}
	#unless($destination_provider_billing_info{profile_info}) {
	#   FATAL "Missing billing profile for destination_provider_id ".$cdr->{destination_provider_id}." for cdr #".$cdr->{id}."\n";
	#}

	# call from local subscriber
	if($cdr->{source_user_id} ne "0") {
		DEBUG "call from local subscriber, source_user_id is $$cdr{source_user_id}";
		# if we have a call from local subscriber, the source provider MUST be a reseller
		if($source_provider_billing_info{profile_id} && $source_provider_package_info{class} ne "reseller") {
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
				get_provider_call_cost($cdr, $type, "in", $readonly,
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
			get_customer_call_cost($cdr, $type, "in", $readonly,
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
				get_provider_call_cost($cdr, $type, "out", $readonly,
							$destination_provider_info, \$source_carrier_cost, \$source_carrier_free_time,
							\$rating_durations[@rating_durations])
						or FATAL "Error getting source carrier cost for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing destination profile, so we can't calculate source_carrier_cost for destination_provider_billing_info ".(Dumper \%destination_provider_billing_info);
			}
		}

		# get reseller cost
		if($source_provider_billing_info{profile_id}) {
			get_provider_call_cost($cdr, $type, "out", $readonly,
						$source_provider_info, \$source_reseller_cost, \$source_reseller_free_time,
						\$rating_durations[@rating_durations])
				 or FATAL "Error getting source reseller cost for cdr ".$cdr->{id}."\n";
		} else {
			# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
			# in theory, all resellers MUST have a billing profile, so we could bail out here
		}

		# get customer cost
		get_customer_call_cost($cdr, $type, "out", $readonly,
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
				get_provider_call_cost($cdr, $type, "in", $readonly,
							$source_provider_info, \$destination_carrier_cost, \$destination_carrier_free_time,
							\$rating_durations[@rating_durations])
					or FATAL "Error getting destination carrier cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing source profile, so we can't calculate destination_carrier_cost for source_provider_billing_info ".(Dumper \%source_provider_billing_info);
			}
			if($destination_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching destination_reseller_cost based on source_provider_billing_info ".(Dumper \%destination_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "in", $readonly,
							$destination_provider_info, \$destination_reseller_cost, \$destination_reseller_free_time,
							\$rating_durations[@rating_durations])
					or FATAL "Error getting destination reseller cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				# up to 2.8, there is one hardcoded reseller id 1, which doesn't have a billing profile, so skip this step here.
				# in theory, all resellers MUST have a billing profile, so we could bail out here
				WARNING "missing destination profile, so we can't calculate destination_reseller_cost for destination_provider_billing_info ".(Dumper \%destination_provider_billing_info);
			}

			get_customer_call_cost($cdr, $type, "in", $readonly,
					\$destination_customer_cost, \$destination_customer_free_time,
					\$rating_durations[@rating_durations])
			or FATAL "Error getting destination customer cost for local destination_user_id ".
					$cdr->{destination_user_id}." for cdr ".$cdr->{id}."\n";
		} else {

			if($source_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching destination_carrier_cost based on source_provider_billing_info ".(Dumper \%source_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "in", $readonly,
							$source_provider_info, \$destination_carrier_cost, \$destination_carrier_free_time,
							\$rating_durations[@rating_durations])
					or FATAL "Error getting destination carrier cost for local destination_provider_id ".
							$cdr->{destination_provider_id}." for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing source profile, so we can't calculate destination_carrier_cost for source_provider_billing_info ".(Dumper \%source_provider_billing_info);
			}

			if($destination_provider_billing_info{profile_id}) {
				DEBUG sub { "fetching source_carrier_cost based on destination_provider_billing_info ".(Dumper \%destination_provider_billing_info) };
				get_provider_call_cost($cdr, $type, "out", $readonly,
							$destination_provider_info, \$source_carrier_cost, \$source_carrier_free_time,
							\$rating_durations[@rating_durations])
						or FATAL "Error getting source carrier cost for cdr ".$cdr->{id}."\n";
			} else {
				WARNING "missing destination profile, so we can't calculate source_carrier_cost for destination_provider_billing_info ".(Dumper \%destination_provider_billing_info);
			}

		}
	}

	if ($split_peak_parts) {
		# We require the onpeak/offpeak thresholds to be the same for all rating fee profiles used by any
		# one particular CDR, so that CDR fragmentations are uniform across customer/carrier/reseller/etc
		# entries. Mismatching onpeak/offpeak thresholds are a fatal error (which also results in a
		# transaction rollback).

		my %rating_durations;
		for my $rd (@rating_durations) {
			if (defined($rd)) {
				$rating_durations{$rd} = 1;
				$cdr->{rating_duration} //= 0;
				$cdr->{rating_duration} = $rd if $rd > $cdr->{rating_duration};
			}
		}
		if (scalar(keys(%rating_durations)) > 1) {
			DEBUG 'Inconsistent rating fragment durations '.join(', ',keys(%rating_durations))." for cdr ID $cdr->{id}";
			if ($rating_attempts > 1) {
				FATAL "Error getting consistent rating fragment for cdr ".$cdr->{id}.". Rating profiles don't match.";
			} else {
				DEBUG 'trying again';
				goto RATING_DURATION_FOUND;
			}
		} elsif ($rating_attempts == 1) { # coherent rating durations on first attempt
			goto RATING_DURATION_FOUND; # just do it again to write stuff
		}
		my $rating_duration = (keys(%rating_durations))[0] // $cdr->{duration};

		if ($rating_duration < $cdr->{duration}) {
			my $sth = $sth_create_cdr_fragment; # start_time is advanced, duration decreased
			$sth->execute($rating_duration, $rating_duration, $cdr->{id})
				or FATAL "Error executing create cdr fragment statement: ".$sth->errstr;
			if ($sth->rows > 0) {
				DEBUG "New rating fragment CDR with ".($cdr->{duration} - $rating_duration)." secs duration created from cdr ID $cdr->{id}";
				write_cdr_col_data($acc_relation_col_model_key,$cdr,$acctdbh->{'mysql_insertid'},
					{ direction => 'source', provider => 'customer', relation => 'prev_fragment_id' },
					$cdr->{id});
			} else {
				$rollback = 1;
				FATAL "cdr ID $cdr->{id} seems to be already processed by someone else";
			}
			$cdr->{is_fragmented} = 1;
			$cdr->{duration} = $rating_duration;
		}
	}

	$cdr->{source_carrier_cost} = $source_carrier_cost;
	$cdr->{source_reseller_cost_old} = $cdr->{source_reseller_cost};
	$cdr->{source_reseller_cost} = $source_reseller_cost;
	$cdr->{source_customer_cost_old} = $cdr->{source_customer_cost};
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

sub create_pidfile {

	my $pidfh;

	open $pidfh, '>>', $pidfile or FATAL "Can't open '$pidfile' for writing: $!\n";
	flock($pidfh, LOCK_EX | LOCK_NB) or FATAL "Unable to lock pidfile '$pidfile': $!\n";

	return $pidfh;

}

sub write_pidfile {

	my $pidfh = shift;

	seek $pidfh, 0, SEEK_SET;
	truncate $pidfh, 0;
	printflush $pidfh "$$\n";

}

sub notify_send {
	my $message = shift;

	if ($ENV{NOTIFY_SOCKET}) {
		my $addr = $ENV{NOTIFY_SOCKET} =~ s/^@/\0/r;
		my $sock = IO::Socket::UNIX->new(
			Type => SOCK_DGRAM(),
			Peer => $addr,
		) or warn "cannot connect to socket $ENV{NOTIFY_SOCKET}: $!\n";
		if ($sock) {
			$sock->autoflush(1);
			print { $sock } $message
				or warn "cannot send to socket $ENV{NOTIFY_SOCKET}: $!\n";
			close $sock;
		}
	} else {
		warn "NOTIFY_SOCKET not set\n";
	}
}

sub daemonize {

	my $pidfile = shift;
	my $pidfh;

	chdir '/' or FATAL "Can't chdir to /: $!\n";
	$pidfh = create_pidfile($pidfile);
	defined(my $pid = fork) or FATAL "Can't fork: $!\n";
	exit if $pid;
	setsid or FATAL "Can't start a new session: $!\n";
	write_pidfile($pidfh);

	return $pidfh;

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

sub _cps_delay {
	if ($cps_info->{delay} > 0.0) {
		INFO "Sleeping for ".sprintf("%.3f",$cps_info->{delay})." seconds";
		Time::HiRes::sleep($cps_info->{delay});
	}
}

sub _update_cps {
	my $num_of_cdrs = shift;

	$cps_info->{rated_old} = $cps_info->{rated};
	$cps_info->{rated} += $num_of_cdrs;
	$cps_info->{d_rated_old} = $cps_info->{d_rated};
	$cps_info->{d_rated} = $cps_info->{rated} - $cps_info->{rated_old};
	$cps_info->{dd_rated} = $cps_info->{d_rated} - $cps_info->{d_rated_old}; # if 2nd order is to be used.

	$cps_info->{t_old} = $cps_info->{t};
	$cps_info->{t} = Time::HiRes::time();
	$cps_info->{dt} = $cps_info->{t} - $cps_info->{t_old};

	if ($cps_info->{dt} > 0.0) {
		$cps_info->{cps} = $cps_info->{d_rated} / $cps_info->{dt};
		DEBUG sprintf("%.1f",$cps_info->{cps} )." CDRs per sec";
	} else {
		$cps_info->{cps} = ~0;
	}

	if ($cps_info->{d_rated} > 0) { # using first order for now.
		if (($cps_info->{delay} + $cps_info->{speedup}) > 0.0) {
			$cps_info->{delay} -= $cps_info->{speedup};
			DEBUG "reducing delay";
		}
	} else { #if ($cps_info->{dd_rated} < 0.0) {
		if ($cps_info->{delay} < ($loop_interval - $cps_info->{speeddown})) {
			$cps_info->{delay} += $cps_info->{speeddown};
			DEBUG "increasing delay";
		}
	#} else {

	}
}

sub main {

	my $pidfh;

	# Without autoflush logs are buffered due to
	# journald which is buffering Perl STDOUT
	# (STDERR flushed immediately which confusing)
	select->autoflush(1);

	INFO "Starting rate-o-mat.\n";

	if ($fork != 0) {
		$pidfh = daemonize($pidfile);
	} elsif ($pidfile) {
		$pidfh = create_pidfile($pidfile);
		write_pidfile($pidfh);
	}

	local $SIG{TERM} = \&signal_handler;
	local $SIG{INT} = \&signal_handler;
	local $SIG{QUIT} = \&signal_handler;
	local $SIG{HUP} = \&signal_handler;

	if ($maintenance_mode eq 'yes') {
		INFO "Up and doing nothing in the maintenance mode.\n";
		notify_send("READY=1\n");
		while (!$shutdown) {
			sleep(1);
		}
		exit(0);
	}

	DEBUG "Init DB on start...\n";
	init_db or FATAL "Error initializing database handlers\n";
	my $rated = 0;
	my $next_del = 10000;
	my %failed_counter_map = ();
	my $init = 0;

	INFO "Up and running.\n";
	notify_send("READY=1\n");

	BATCH: while (!$shutdown) {

		$log_fatal = 1;
		if ($init) {
			DEBUG "Init DB in loop...\n";
			init_db or FATAL "Error initializing database handlers\n";
		}
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
				if ($DBI::err and $DBI::err == 2006) {
					INFO "DB connection gone, retrying...";
					close_db();
					$init = 1;
					next BATCH;
				}
				FATAL "Error getting next bunch of CDRs: " . $error;
			}
		} else {
			WARNING "no-op loop since mandatory db connections are n/a";
		}

		$shutdown and last BATCH;

		my $rated_batch = 0;
		my $t;
		my $cdr_id;
		my $info_prefix;
		my $failed = 0;

		eval {
			## no critic (TestingAndDebugging::ProhibitNoWarnings)
			no warnings qw/ exiting /;
			CDR: foreach my $cdr (@cdrs) {
				$rollback = 0;
				$log_fatal = 0;
				$info_prefix = ($rated_batch + 1) . "/" . (scalar @cdrs) . " - ";
				eval {
					$t = Time::HiRes::time();
					$cdr_id = $cdr->{id};
					DEBUG "start rating CDR ID $cdr_id";
					begin_transaction($acctdbh);
					if ('unrated' ne lock_cdr($cdr)) {
						commit_transaction($acctdbh);
						check_shutdown() and last BATCH;
						next CDR;
					}
					# required to avoid contract_balances duplications during catchup:
					begin_transaction($billdbh,'READ COMMITTED');
					# row locks are released upon commit/rollback and have to cover
					# the whole transaction. thus locking contract rows for preventing
					# concurrent catchups will be our very first SQL statement in the
					# billingdb transaction:
					lock_contracts($cdr);
					begin_transaction($provdbh);
					begin_transaction($dupdbh);

					INFO $info_prefix."rate CDR ID ".$cdr->{id};
					rate_cdr($cdr, $type) && update_cdr($cdr);

					# we would need a XA/distributed transaction manager for this:
					commit_transaction($acctdbh);
					commit_transaction($billdbh);
					commit_transaction($provdbh);
					commit_transaction($dupdbh);

					$rated_batch++;
					delete $failed_counter_map{$cdr_id};
					debug_rating_time($t,$cdr_id,0);
					check_shutdown() and last BATCH;
					_update_cps(1); # unless ($rated_batch % 5);
					_cps_delay();
				};
				$error = $@;
				if ($error) {
					debug_rating_time($t,$cdr_id,1);
					if ($rollback) {
						INFO $info_prefix."rolling back changes for CDR ID $cdr_id";
						rollback_all();
						next CDR; #move on to the next cdr of the batch
					} else {
						$failed_counter_map{$cdr_id} = 0 if !exists $failed_counter_map{$cdr_id};
						if ($failed_counter_map{$cdr_id} < $failed_cdr_max_retries && !defined $DBI::err) {
							WARNING $info_prefix."rating CDR ID $cdr_id aborted " .
								($failed_counter_map{$cdr_id} > 0 ? " (retry $failed_counter_map{$cdr_id})" : "") .
								": " . $error;
							$failed_counter_map{$cdr_id} = $failed_counter_map{$cdr_id} + 1;
							$failed += 1;
							rollback_all();
							next CDR; #move on to the next cdr of the batch
						} else {
							die($error); #rethrow
						}
					}
				}
			}
		};
		$log_fatal = 1;
		$error = $@;
		if ($error) {
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
					close_db();
					$init = 1;
					next BATCH; #fetch new batch
				} elsif ($DBI::err == 1213) {
					INFO "Transaction concurrency problem, rolling back and retrying...";
					rollback_all();
					close_db();
					$init = 1;
					next BATCH; #fetch new batch
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
		unless (@cdrs) {
			_update_cps(0);
			_cps_delay();
		}
		if ($debug && $split_peak_parts && (scalar @cdrs) < 5) {
			sleep $loop_interval; #split peak parts testcase
		}

		$shutdown and last BATCH;

		if ($rated >= $next_del) { # not ideal imho
			$next_del = $rated + 10000;
			while ($sth_delete_old_prepaid->execute > 0) {
				WARNING $sth_delete_old_prepaid->rows;
			}
		}

		if ($failed > 0) {
			INFO "There were $failed failed CDRs, sleep $failed_cdr_retry_delay";
			sleep($failed_cdr_retry_delay);
		}

		close_db();
		$init = 1;

	}

	notify_send("STOPPING=1\n");
	INFO "Shutting down.\n";

	close $pidfh;
	unlink $pidfile;
}

sub close_db {
	DEBUG "Closing DB connections.\n";

	$sth_get_subscriber_contract_id->finish;
	$sth_billing_info_network->finish;
	$sth_billing_info->finish;
	$sth_profile_info->finish;
	$sth_profile_fraud_info->finish;
	$sth_contract_fraud_info->finish;
	$sth_upsert_cdr_period_costs->finish;
	$sth_get_cdr_period_costs->finish;
	$sth_offpeak->finish;
	$sth_offpeak_subscriber->finish;
	$sth_unrated_cdrs->finish;
	$sth_get_cdr->finish;
	$sth_lock_cdr->finish;
	$sth_update_cdr->finish;
	$split_peak_parts and $sth_create_cdr_fragment->finish;
	$sth_mos_data->finish;
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
	$sth_lock_billing_subscribers->finish;
	$sth_unlock_billing_subscribers->finish;
	$sth_get_provisioning_voip_subscribers and $sth_get_provisioning_voip_subscribers->finish;
	$sth_get_usr_preference_attribute and $sth_get_usr_preference_attribute->finish;
	$sth_get_usr_preference_value and $sth_get_usr_preference_value->finish;
	$sth_create_usr_preference_value and $sth_create_usr_preference_value->finish;
	$sth_update_usr_preference_value and $sth_update_usr_preference_value->finish;
	$sth_delete_usr_preference_value and $sth_delete_usr_preference_value->finish;
	$sth_duplicate_cdr and $sth_duplicate_cdr->finish;
	$sth_duplicate_mos_data and $sth_duplicate_mos_data->finish;
	$sth_duplicate_upsert_cdr_period_costs and $sth_duplicate_upsert_cdr_period_costs->finish;
	$sth_duplicate_get_cdr_period_costs and $sth_duplicate_get_cdr_period_costs->finish;
	foreach (keys %cdr_col_models) {
		my $model = $cdr_col_models{$_};
		$model->{write_sth}->{sth}->finish;
		$model->{read_sth}->{sth}->finish;
		foreach (values %{$model->{dimension_sths}}) {
			$_->{sth}->finish;
		}
	}

	$billdbh->disconnect;
	$acctdbh->disconnect;
	$provdbh and $provdbh->disconnect;
	$dupdbh and $dupdbh->disconnect;

}
