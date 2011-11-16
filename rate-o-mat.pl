#!/usr/bin/perl -w
use lib '/usr/share/ngcp-rate-o-mat';
use strict;
use DBI;
use POSIX qw(setsid mktime);
use Fcntl qw(LOCK_EX LOCK_NB SEEK_SET);
use IO::Handle;
use Sys::Syslog;
use Data::Dumper;

my $fork = 1;
my $pidfile = '/var/run/rate-o-mat.pid';
my $type = 'call';
my $loop_interval = $ENV{RATEOMAT_LOOP_INTERVAL} ? int $ENV{RATEOMAT_LOOP_INTERVAL} : 10;

my $log_ident = 'rate-o-mat';
my $log_facility = 'daemon';
my $log_opts = 'ndely,cons,pid,nowait';

# if split_peak_parts is set to true, rate-o-mat will create a separate
# CDR every time a peak time border is crossed for either the customer,
# the reseller or the carrier billing profile.
my $split_peak_parts = int($ENV{RATEOMAT_SPLIT_PEAK_PARTS} || 0);

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
my $BillDB_Pass = $ENV{RATEOMAT_BILLING_DB_PASS} || die "Missing billing DB password setting.";
# accounting database
my $AcctDB_Name = $ENV{RATEOMAT_ACCOUNTING_DB_NAME} || 'accounting';
my $AcctDB_Host = $ENV{RATEOMAT_ACCOUNTING_DB_HOST} || 'localhost';
my $AcctDB_Port = $ENV{RATEOMAT_ACCOUNTING_DB_PORT} ? int $ENV{RATEOMAT_ACCOUNTING_DB_PORT} : 3306;
my $AcctDB_User = $ENV{RATEOMAT_ACCOUNTING_DB_USER} || die "Missing accounting DB user setting.";
my $AcctDB_Pass = $ENV{RATEOMAT_ACCOUNTING_DB_PASS} || die "Missing accounting DB password setting.";

########################################################################

sub main;

my $shutdown = 0;
my $prepaid_costs;

my $billdbh;
my $acctdbh;
my $sth_billing_info;
my $sth_profile_info;
my $sth_offpeak_weekdays;
my $sth_offpeak_special;
my $sth_unrated_cdrs;
my $sth_update_cdr;
my $sth_update_cdr_split;
my $sth_create_cdr_fragment;
my $sth_provider_info;
my $sth_reseller_info;
my $sth_get_cbalance;
my $sth_update_cbalance;
my $sth_new_cbalance_week;
my $sth_new_cbalance_month;
my $sth_get_last_cbalance;
my $sth_lnp_number;
my $sth_lnp_profile_info;
my $sth_prepaid_costs;
my $sth_delete_prepaid_cost;

my $connect_interval = 3;

main;
exit 0;

########################################################################

sub FATAL
{
	my $msg = shift;
	chomp $msg;
	print "FATAL: $msg\n" if($fork != 1);
	unless(defined $DBI::err and $DBI::err == 2006)
	{
		$billdbh->rollback if defined $billdbh;
		$acctdbh->rollback if defined $acctdbh;
	}
	syslog('crit', $msg);
	die "$msg\n";
}

sub DEBUG
{
	my $msg = shift;
	chomp $msg;
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

sub set_start_unixtime
{
	my $start = shift;
	my $r_unix = shift;

	my ($y, $m, $d, $H, $M, $S) = $start =~ /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/;
	$$r_unix = mktime($S, $M, $H, $d, $m-1, $y-1900, 0, 0, -1);

	return 0;
}

sub set_start_strtime
{
	my $start = shift;
	my $r_str = shift;

	my ($y, $m, $d, $H, $M, $S) = (localtime($start))[5,4,3,2,1,0];
	$y += 1900;
	$m += 1;

	$$r_str = "$y-$m-$d $H:$M:$S";
	return 0;
}

sub connect_billdbh
{
	do {
		INFO "Trying to connect to billing db...";
		$billdbh = DBI->connect("dbi:mysql:database=$BillDB_Name;host=$BillDB_Host;port=$BillDB_Port", $BillDB_User, $BillDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 0, PrintWarn => 0});
	} while(!defined $billdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);
	
	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($billdbh);
	INFO "Successfully connected to billing db...";
}

sub connect_acctdbh
{
	do {
		INFO "Trying to connect to accounting db...";
		$acctdbh = DBI->connect("dbi:mysql:database=$AcctDB_Name;host=$AcctDB_Host;port=$AcctDB_Port", $AcctDB_User, $AcctDB_Pass, {AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 0, PrintWarn => 0});
	} while(!defined $acctdbh && ($DBI::err == 2002 || $DBI::err == 2003) && !$shutdown && sleep $connect_interval);

	FATAL "Error connecting to db: ".$DBI::errstr
		unless defined($acctdbh);
	INFO "Successfully connected to accounting db...";
}


sub init_db
{
	connect_billdbh;
	connect_acctdbh;

	$sth_billing_info = $billdbh->prepare(
		"SELECT a.contract_id, b.billing_profile_id, ".
		"d.prepaid, d.interval_charge, d.interval_free_time, d.interval_free_cash, ".
		"d.interval_unit, d.interval_count ".
		"FROM billing.voip_subscribers a, billing.billing_mappings b, ".
		"billing.billing_profiles d ".
		"WHERE a.uuid = ? AND a.contract_id = b.contract_id ".
		"AND ( b.start_date IS NULL OR b.start_date <= FROM_UNIXTIME(?) ) ".
		"AND ( b.end_date IS NULL OR b.end_date >= FROM_UNIXTIME(?) ) ".
		"AND b.billing_profile_id = d.id ".
		"ORDER BY b.start_date DESC ".
		"LIMIT 1"
	) or FATAL "Error preparing billing info statement: ".$billdbh->errstr;
	
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
		"SELECT id, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, use_free_time ".
		"FROM billing.billing_fees_history WHERE billing_profile_id = ? ".
		"AND bf_id IS NOT NULL AND type = ? AND ? REGEXP(destination) ".
		"ORDER BY LENGTH(destination) DESC LIMIT 1"
	) or FATAL "Error preparing profile info statement: ".$billdbh->errstr;

	$sth_lnp_profile_info = $billdbh->prepare(
		"SELECT id, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval, ".
		"billing_zones_history_id, use_free_time ".
		"FROM billing.billing_fees_history WHERE billing_profile_id = ? ".
		"AND bf_id IS NOT NULL AND type = ? AND destination = ? ".
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
		"SELECT id, call_id, ".
		"source_user_id, source_provider_id, ".
		"destination_user_id, destination_provider_id, ".
		"destination_user, destination_domain, ".
		"destination_user_in, destination_domain_in, ".
		"start_time, duration, call_status, IF(is_fragmented IS NULL, 0, is_fragmented) AS is_fragmented ".
		"FROM accounting.cdr WHERE rating_status = 'unrated' ".
		"ORDER BY start_time ASC LIMIT 100 " # ."FOR UPDATE"
	) or FATAL "Error preparing unrated cdr statement: ".$acctdbh->errstr;

	$sth_update_cdr = $acctdbh->prepare(
		"UPDATE accounting.cdr SET ".
		"carrier_cost = ?, reseller_cost = ?, customer_cost = ?, ".
		"carrier_free_time = ?, reseller_free_time = ?, customer_free_time = ?, ".
		"rated_at = now(), rating_status = ?, ".
		"carrier_billing_fee_id = ?, reseller_billing_fee_id = ?, customer_billing_fee_id = ?, ".
		"carrier_billing_zone_id = ?, reseller_billing_zone_id = ?, customer_billing_zone_id = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update cdr statement: ".$acctdbh->errstr;

	if($split_peak_parts) {
		$sth_update_cdr_split = $acctdbh->prepare(
			"UPDATE accounting.cdr SET ".
			"carrier_cost = ?, reseller_cost = ?, customer_cost = ?, ".
			"carrier_free_time = ?, reseller_free_time = ?, customer_free_time = ?, ".
			"rated_at = now(), rating_status = ?, ".
			"carrier_billing_fee_id = ?, reseller_billing_fee_id = ?, customer_billing_fee_id = ?, ".
			"carrier_billing_zone_id = ?, reseller_billing_zone_id = ?, customer_billing_zone_id = ?, ".
			"frag_carrier_onpeak = ?, frag_reseller_onpeak = ?, frag_customer_onpeak = ?, is_fragmented = ?, ".
			"duration = ? ".
			"WHERE id = ?"
		) or FATAL "Error preparing update cdr statement: ".$acctdbh->errstr;

		$sth_create_cdr_fragment = $acctdbh->prepare(
			"INSERT INTO accounting.cdr
			            (source_user_id,source_provider_id,source_user,source_domain,
			             source_cli,source_clir,destination_user_id,destination_provider_id,
			             destination_user,destination_domain,destination_user_dialed,
			             destination_user_in,destination_domain_in,call_type,call_status,call_code,
			             start_time,duration,call_id,is_fragmented)
			      SELECT source_user_id,source_provider_id,source_user,source_domain,
			             source_cli,source_clir,destination_user_id,destination_provider_id,
			             destination_user,destination_domain,destination_user_dialed,
			             destination_user_in,destination_domain_in,call_type,call_status,call_code,
			             start_time + INTERVAL ? SECOND,duration - ?,call_id,is_fragmented
			        FROM accounting.cdr
			       WHERE id = ?
			") or FATAL "Error preparing create cdr fragment statement: ".$acctdbh->errstr;
	}

	$sth_provider_info = $billdbh->prepare(
		"SELECT p.class, bm.billing_profile_id ".
		"FROM billing.products p, billing.billing_mappings bm ".
		"WHERE bm.contract_id = ? AND bm.product_id = p.id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= ?) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= ?)"
	) or FATAL "Error preparing provider info statement: ".$billdbh->errstr;

	$sth_reseller_info = $billdbh->prepare(
		"SELECT bm.billing_profile_id ".
		"FROM billing.billing_mappings bm, billing.voip_subscribers vs, ".
		"billing.contracts c ".
		"WHERE vs.uuid = ? AND vs.contract_id = c.id ".
		"AND c.reseller_id = bm.contract_id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= ?) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= ?)"
	) or FATAL "Error preparing reseller info statement: ".$billdbh->errstr;
	
	$sth_get_cbalance = $billdbh->prepare(
		"SELECT id, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, start ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"end >= FROM_UNIXTIME(?) ORDER BY start ASC"
	) or FATAL "Error preparing get contract balance statement: ".$billdbh->errstr;
	
	$sth_get_last_cbalance = $billdbh->prepare(
		"SELECT id, end, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"start <= FROM_UNIXTIME(?) AND end <= FROM_UNIXTIME(?) ORDER BY end DESC LIMIT 1"
	) or FATAL "Error preparing get last contract balance statement: ".$billdbh->errstr;
	
	$sth_new_cbalance_week = $billdbh->prepare(
		"INSERT INTO billing.contract_balances VALUES(NULL, ?, ?, ?, ?, ?, ".
		"DATE_ADD(?, INTERVAL 1 SECOND), DATE_ADD(?, INTERVAL ? WEEK) )"
	) or FATAL "Error preparing create contract balance statement: ".$billdbh->errstr;

	$sth_new_cbalance_month = $billdbh->prepare(
		"INSERT INTO billing.contract_balances VALUES(NULL, ?, ?, ?, ?, ?, ".
		"DATE_ADD(?, INTERVAL 1 SECOND), ".
		"FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_DAY(DATE_ADD(?, INTERVAL ? MONTH)))), ".
		"NULL)"
	) or FATAL "Error preparing create contract balance statement: ".$billdbh->errstr;
	
	$sth_update_cbalance = $billdbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$billdbh->errstr;

	$sth_prepaid_costs = $acctdbh->prepare(
		"SELECT * FROM prepaid_costs"
	) or FATAL "Error preparing prepaid costs statement: ".$acctdbh->errstr;

	$sth_delete_prepaid_cost = $acctdbh->prepare(
		"DELETE FROM prepaid_costs WHERE id = ?"
	) or FATAL "Error preparing delete prepaid costs statement: ".$acctdbh->errstr;

	return 1;
}

sub create_contract_balance
{
	my $latest_end = shift;
	my $cdr = shift;
	my $binfo = shift;
	my $r_res = shift;

	my $sth = $sth_get_last_cbalance;
	$sth->execute($binfo->{contract_id}, $cdr->{start_time}, $cdr->{start_time})
		or FATAL "Error executing get contract balance statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();

	# TODO: we could just create a new one, we just have to know when to start
	FATAL "No contract balance for contract id ".$binfo->{contract_id}." starting earlier than '".
		$cdr->{start_time}."' found\n"
		unless(@res);
	
	my $last_id = $res[0];
	my $last_end = $res[1];
	my $last_cash_balance = $res[2];
	my $last_cash_balance_int = $res[3];
	my $last_free_balance = $res[4];
	my $last_free_balance_int = $res[5];

	# break recursion if we got the last result as last time
	return 1 if($latest_end eq $last_end);


	my %last_profile = ();
	get_billing_info($last_end, $cdr->{source_user_id}, \%last_profile) or
		FATAL "Error getting billing info for date '".$last_end."' and uuid '".$cdr->{source_user_id}."'\n";

	my %current_profile = ();
	my $last_end_unix;
	set_start_unixtime($last_end, \$last_end_unix);
	$last_end_unix += 1;
	my $current_date;
	set_start_strtime($last_end_unix, \$current_date);
	get_billing_info($current_date, $cdr->{source_user_id}, \%current_profile) or
		FATAL "Error getting billing info for date '".$current_date."' and uuid '".$cdr->{source_user_id}."'\n";

	my $new_free_balance = $last_free_balance + $current_profile{int_free_time} - 
		($last_profile{int_free_time} - $last_free_balance_int);
	my $new_free_balance_int = 0;
	
	my $new_cash_balance = $last_cash_balance + $current_profile{int_free_cash} - 
		($last_profile{int_free_cash} - $last_cash_balance_int);
	my $new_cash_balance_int = 0;

	if($binfo->{int_unit} eq "week")
	{
		$sth = $sth_new_cbalance_week;
	}
	elsif($binfo->{int_unit} eq "month")
	{
		$sth = $sth_new_cbalance_month;
	}
	else
	{
		FATAL "Invalid interval unit '".$binfo->{int_unit}."' in profile id ".
			$binfo->{profile_id};
	}

	$sth->execute($binfo->{contract_id}, $new_cash_balance, $new_cash_balance_int,
		$new_free_balance, $new_free_balance_int, 
		$last_end, 
		$last_end, $binfo->{int_count})
		or FATAL "Error executing new contract balance statement: ".$sth->errstr;

	$r_res->{id} = $billdbh->last_insert_id(undef, undef, undef, undef);
	$r_res->{cash_balance} = $new_cash_balance;
	$r_res->{cash_balance_interval} = $new_cash_balance_int;
	$r_res->{free_time_balance} = $new_free_balance;
	$r_res->{free_time_balance_interval} = $new_free_balance_int;

	return create_contract_balance($last_end, $cdr, $binfo, $r_res);
}

sub get_contract_balance
{
	my $cdr = shift;
	my $binfo = shift;
	my $pinfo = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_duration = shift;
	my $r_balances = shift;

	my $sth = $sth_get_cbalance;
	$sth->execute(
		$binfo->{contract_id}, $cdr->{start_time})
		or FATAL "Error executing get contract balance statement: ".$sth->errstr;
	my $res = $sth->fetchall_arrayref({});

	unless(@$res)
	{
		my %new_row = ();
		create_contract_balance('invalid', $cdr, $binfo, \%new_row)
			or FATAL "Failed to create new contract balance\n";
		push @$res, \%new_row;
	}

	for(my $i = 0; $i < @$res; ++$i)
	{
		my $row = $res->[$i];

		my %balance = ();
		$balance{id} = $row->{id};
		$balance{cash_balance} = $row->{cash_balance};
		$balance{cash_balance_interval} = $row->{cash_balance_interval};
		$balance{free_time_balance} = $row->{free_time_balance};
		$balance{free_time_balance_interval} = $row->{free_time_balance_interval};

		if($i == 0)
		{
			if($binfo->{prepaid} == 1)
			{
				WARNING "TODO: do we need to process prepaid balances here?";
				### should have been handled during call - why are we here anyway?
			}
			else
			{
				if($pinfo->{use_free_time} && $balance{free_time_balance} > 0)
				{
					$balance{free_time_balance} -= $$r_duration;
					if($balance{free_time_balance} >= 0) {
						$balance{free_time_balance_interval} += $$r_duration;
						$$r_cost = 0;
						$$r_free_time += $$r_duration;
					} else {   # partial free-time payment
						$balance{free_time_balance} *= -1;
						$$r_cost *= $balance{free_time_balance} / $$r_duration;
						$balance{free_time_balance_interval} += $$r_duration - $balance{free_time_balance};
						$$r_free_time += $$r_duration - $balance{free_time_balance};
						$balance{free_time_balance} = 0;
					}
				}
				if($$r_cost and $balance{cash_balance} > 0)
				{
					$balance{cash_balance} -= $$r_cost;
					if($balance{cash_balance} >= 0) {
						$balance{cash_balance_interval} += $$r_cost;
						$$r_cost = 0;
					} else {  # partial free-cash payment
						$balance{cash_balance} *= -1;
						$balance{cash_balance_interval} += $$r_cost - $balance{cash_balance};
						$$r_cost = $balance{cash_balance};
						$balance{cash_balance} = 0;
					}
				}
			}
		}

		if($i < @$res - 1)
		{
			# TODO: shift calculated values to next balance
			# if call falls in an old balance
		}


		#print "contract balance:\n";
		#print Dumper \%balance;
	
		$sth = $sth_update_cbalance;
		$sth->execute(
			$balance{cash_balance}, $balance{cash_balance_interval},
			$balance{free_time_balance}, $balance{free_time_balance_interval},
			$balance{id})
			or FATAL "Error executing update contract balance statement: ".$sth->errstr;

		push @$r_balances, \%balance;
	}

	return 1;
}

sub update_contract_balance
{
	my $cdr = shift;
	my $binfo = shift;
	my $pinfo = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_duration = shift;

	my @balances = ();

	get_contract_balance($cdr, $binfo, $pinfo, $r_cost, $r_free_time, $r_duration, \@balances)
		or FATAL "Error getting contract balances\n";

	# the above does the update as well, so we're done here
	

	return 1;
}

sub get_billing_info
{
	my $start = shift;
	my $uid = shift;
	my $r_info = shift;

	my $sth = $sth_billing_info;

	$sth->execute($uid, $start, $start) or
		FATAL "Error executing billing info statement: ".$sth->errstr;
	my @res = $sth->fetchrow_array();
	FATAL "No billing info found for uuid '".$uid."'\n" unless @res;

	$r_info->{contract_id} = $res[0];
	$r_info->{profile_id} = $res[1];
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
	my $destination_class = shift;
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
			$sth_lnp_profile_info->execute($bpid, $type, 'lnp:'.$lnppid)
				or FATAL "Error executing LNP profile info statement: ".$sth_lnp_profile_info->errstr;
			@res = $sth_lnp_profile_info->fetchrow_array();
			FATAL "Error fetching LNP profile info: ".$sth_lnp_profile_info->errstr
				if $sth_lnp_profile_info->err;
		}
	}

	my $sth = $sth_profile_info;

	unless(@res) {
		$sth->execute($bpid, $type, $destination)
			or FATAL "Error executing profile info statement: ".$sth->errstr;
		@res = $sth->fetchrow_array();
	}

	return 0 unless @res;
	
	$b_info->{fee_id} = $res[0];
	$b_info->{pattern} = $res[1];
	$b_info->{on_init_rate} = $res[2];
	$b_info->{on_init_interval} = $res[3] == 0 ? 1 : $res[3]; # prevent loops
	$b_info->{on_follow_rate} = $res[4];
	$b_info->{on_follow_interval} = $res[5] == 0 ? 1 : $res[5];
	$b_info->{off_init_rate} = $res[6];
	$b_info->{off_init_interval} = $res[7] == 0 ? 1 : $res[7];
	$b_info->{off_follow_rate} = $res[8];
	$b_info->{off_follow_interval} = $res[9] == 0 ? 1 : $res[9];
	$b_info->{zone_id} = $res[10];
	$b_info->{use_free_time} = $res[11];
	
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

sub get_unrated_cdrs
{
	my $r_cdrs = shift;

	my $sth = $sth_unrated_cdrs;
	$sth->execute
		or FATAL "Error executing unrated cdr statement: ".$sth->errstr;

	while(my $res = $sth->fetchrow_hashref())
	{
		push @$r_cdrs, $res;
	}

	# the while above may have been interupted because there is no
	# data left, or because there was an error. To decide what
	# happened, we have to query $sth->err()
	FATAL "Error fetching unrated cdr's: ". $sth->errstr
		if $sth->err;

	return 1;
}

sub update_cdr
{
	my $cdr = shift;

	if($split_peak_parts) {

		my $sth = $sth_update_cdr_split;
		$sth->execute($cdr->{carrier_cost}, $cdr->{reseller_cost}, $cdr->{customer_cost},
			$cdr->{carrier_free_time}, $cdr->{reseller_free_time}, $cdr->{customer_free_time},
			'ok',
			$cdr->{carrier_billing_fee_id}, $cdr->{reseller_billing_fee_id}, $cdr->{customer_billing_fee_id},
			$cdr->{carrier_billing_zone_id}, $cdr->{reseller_billing_zone_id}, $cdr->{customer_billing_zone_id},
			$cdr->{frag_carrier_onpeak}, $cdr->{frag_reseller_onpeak}, $cdr->{frag_customer_onpeak}, $cdr->{is_fragmented}, $cdr->{duration},
			$cdr->{id})
			or FATAL "Error executing update cdr statement: ".$sth->errstr;

	} else {

		my $sth = $sth_update_cdr;
		$sth->execute($cdr->{carrier_cost}, $cdr->{reseller_cost}, $cdr->{customer_cost},
			$cdr->{carrier_free_time}, $cdr->{reseller_free_time}, $cdr->{customer_free_time},
			'ok',
			$cdr->{carrier_billing_fee_id}, $cdr->{reseller_billing_fee_id}, $cdr->{customer_billing_fee_id},
			$cdr->{carrier_billing_zone_id}, $cdr->{reseller_billing_zone_id}, $cdr->{customer_billing_zone_id},
			$cdr->{id})
			or FATAL "Error executing update cdr statement: ".$sth->errstr;
	}

	return 1;
}

sub update_failed_cdr
{
	my $cdr = shift;

	if($split_peak_parts) {
		my $sth = $sth_update_cdr_split;
		$sth->execute(undef, undef, undef, 'failed', undef, undef, undef, undef, undef, undef,
		              undef, undef, undef, $cdr->{is_fragmented}, $cdr->{duration}, $cdr->{id})
			or FATAL "Error executing update cdr statement: ".$sth->errstr;
	} else {
		my $sth = $sth_update_cdr;
		$sth->execute(undef, undef, undef, 'failed', undef, undef, undef, undef, undef, undef, $cdr->{id})
			or FATAL "Error executing update cdr statement: ".$sth->errstr;
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

	return 1;
}

sub get_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $destination_class = shift;
	my $profile_id = shift;
	my $domain_first = shift;
	my $r_profile_info = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $r_onpeak = shift;

	$$r_rating_duration = 0; # ensure we start with zero length

	my $dst_user;
	my $dst_domain;
	my $first;
	my $second;

	if($destination_class eq "pstnpeering" || $destination_class eq "sippeering")
	{
		$dst_user = $cdr->{destination_user};
		$dst_domain = $cdr->{destination_user}.'@'.$cdr->{destination_domain};
	}
	else
	{
		$dst_user = $cdr->{destination_user_in};
		$dst_domain = $cdr->{destination_user_in}.'@'.$cdr->{destination_domain_in};
	}
	
	if($domain_first == 1)
	{
		$first = $dst_domain;
		$second = $dst_user;
	}
	else
	{
		$first = $dst_user;
		$second = $dst_domain;
	}



	unless(get_profile_info($profile_id, $type, $destination_class, $first, 
		$r_profile_info, $cdr->{start_time}))
	{
		unless(get_profile_info($profile_id, $type, $destination_class, $second, 
			$r_profile_info, $cdr->{start_time}))
		{
			FATAL "No fee info for profile $profile_id and user '$dst_user' ".
			      "or domain '$dst_domain' found\n";
			$$r_cost = 0;
			$$r_free_time = 0;
			return 1;
		}
	}

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
	$$r_free_time = 0;
	my $interval = 0;
	my $rate = 0;
	my $offset = 0;
	my $onpeak = 0;
	my $init = 0;
	my $duration = $cdr->{duration};

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
		}
		else
		{
			last if $split_peak_parts and $$r_onpeak != $onpeak;

			$interval = $onpeak == 1 ? 
				$r_profile_info->{on_follow_interval} : $r_profile_info->{off_follow_interval};
			$rate = $onpeak == 1 ? 
				$r_profile_info->{on_follow_rate} : $r_profile_info->{off_follow_rate};
		}

		$$r_cost += $rate * $interval;
		$duration -= $interval;
		$offset += $interval;
		$$r_rating_duration += $interval;
	}

	return 1;
}

sub get_customer_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $destination_class = shift;
	my $domain_first = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;

	my %billing_info = ();
	get_billing_info($cdr->{start_time}, $cdr->{source_user_id}, \%billing_info) or
		FATAL "Error getting billing info\n";
	#print Dumper \%billing_info;

	unless($billing_info{profile_id}) {
		$$r_rating_duration = $cdr->{duration};
		return -1;
	}

	my %profile_info = ();
	get_call_cost($cdr, $type, $destination_class, $billing_info{profile_id}, 
		$domain_first, \%profile_info, $r_cost, $r_free_time, $r_rating_duration, \$onpeak)
		or FATAL "Error getting customer call cost\n";

	$cdr->{customer_billing_fee_id} = $profile_info{fee_id};
	$cdr->{customer_billing_zone_id} = $profile_info{zone_id};
	$cdr->{frag_customer_onpeak} = $onpeak if $split_peak_parts;

	unless($billing_info{prepaid} == 1)
	{
		update_contract_balance($cdr, \%billing_info, \%profile_info, $r_cost, $r_free_time,
				$r_rating_duration)
			or FATAL "Error updating customer contract balance\n";
	}
	else {
		# overwrite the calculated costs with the ones from our table
		if (!$prepaid_costs) {
			$sth_prepaid_costs->execute()
				or FATAL "Error executing get prepaid costs statement: ".$sth_prepaid_costs->errstr;
			$prepaid_costs = $sth_prepaid_costs->fetchall_hashref('call_id');
		}
		if (exists($prepaid_costs->{$cdr->{call_id}})) {
			my $entry = $prepaid_costs->{$cdr->{call_id}};
			$$r_cost = $entry->{cost};
			$$r_free_time = $entry->{free_time_used};
			$sth_delete_prepaid_cost->execute($entry->{id});
			delete($prepaid_costs->{$cdr->{call_id}});
		}
	}

	return 1;
}

sub get_provider_call_cost
{
	my $cdr = shift;
	my $type = shift;
	my $domain_first = shift;
	my $r_info = shift;
	my $r_cost = shift;
	my $r_free_time = shift;
	my $r_rating_duration = shift;
	my $onpeak;

	my %profile_info = ();
	get_call_cost($cdr, $type, $r_info->{class}, 
		$r_info->{profile_id}, $domain_first, \%profile_info, $r_cost, $r_free_time,
		$r_rating_duration, \$onpeak)
		or FATAL "Error getting provider call cost\n";
 
	if($r_info->{class} eq "reseller")
	{
		$cdr->{reseller_billing_fee_id} = $profile_info{fee_id};
		$cdr->{reseller_billing_zone_id} = $profile_info{zone_id};
		$cdr->{frag_reseller_onpeak} = $onpeak if $split_peak_parts;
	}
	else
	{
		$cdr->{carrier_billing_fee_id} = $profile_info{fee_id};
		$cdr->{carrier_billing_zone_id} = $profile_info{zone_id};
		$cdr->{frag_carrier_onpeak} = $onpeak if $split_peak_parts;
	}
	
	return 1;
}

sub rate_cdr
{
	my $cdr = shift;
	my $type = shift;

	my $customer_cost = 0;
	my $carrier_cost = 0;
	my $reseller_cost = 0;
	my $customer_free_time = 0;
	my $carrier_free_time = 0;
	my $reseller_free_time = 0;
	
	unless($cdr->{call_status} eq "ok")
	{
		$cdr->{carrier_cost} = $carrier_cost;
		$cdr->{reseller_cost} = $reseller_cost;
		$cdr->{customer_cost} = $customer_cost;
		$cdr->{carrier_free_time} = $carrier_free_time;
		$cdr->{reseller_free_time} = $reseller_free_time;
		$cdr->{customer_free_time} = $customer_free_time;
		return 1;
	}

	if($cdr->{source_user_id} eq "0")
	{
		# caller is not local
		# TODO: should there be an incoming profile to calculate termination fees?
	
		$cdr->{carrier_cost} = $carrier_cost;
		$cdr->{reseller_cost} = $reseller_cost;
		$cdr->{customer_cost} = $customer_cost;
		$cdr->{carrier_free_time} = $carrier_free_time;
		$cdr->{reseller_free_time} = $reseller_free_time;
		$cdr->{customer_free_time} = $customer_free_time;
		return 1;
	}

	# caller is local

	my %provider_info = ();
	my %reseller_info = ();
	my $dst_class;
	my $domain_first = 0;
	my $rating_duration;
	my $fragmentation = 0;

	if($cdr->{destination_provider_id} eq "0")
	{
		# call to voicebox or conference?
		WARNING "CDR id ".$cdr->{id}." has no destination provider id\n";

		$dst_class = 'reseller';

	} else {

		get_provider_info($cdr->{destination_provider_id}, $cdr->{start_date},
			\%provider_info)
			or FATAL "Error getting destination provider info\n";

		if($provider_info{class} eq "reseller")
		{
			$dst_class = 'reseller';
			$domain_first = 1; # priorize domain over user to correctly rate onnet-calls

			if($provider_info{profile_id}) {
				# only calculate reseller cost, carrier cost is 0 (hosting-onnet)
				get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$reseller_cost, \$reseller_free_time, \$rating_duration)
					or FATAL "Error getting reseller cost for cdr ".$cdr->{id}."\n";
				if($split_peak_parts and $cdr->{duration} > $rating_duration) {
					DEBUG "reseller rating_duration: $rating_duration, cdr->duration: $$cdr{duration}.\n";
					$cdr->{duration} = $rating_duration;
					$cdr->{is_fragmented} = 1;
					$fragmentation = 1;
				}
			}
		}
		elsif($provider_info{class} eq "sippeering" || $provider_info{class} eq "pstnpeering")
		{
			$dst_class = $provider_info{class};
			$domain_first = 0; # for calls leaving our system, priorize user over domain

			if($provider_info{profile_id}) {
				# carrier cost can be calculated directly with available billing profile
				get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$carrier_cost, \$carrier_free_time, \$rating_duration)
					or FATAL "Error getting carrier cost for cdr ".$cdr->{id}."\n";
				if($split_peak_parts and $cdr->{duration} > $rating_duration) {
					DEBUG "carrier rating_duration: $rating_duration, cdr->duration: $$cdr{duration}.\n";
					$cdr->{duration} = $rating_duration;
					$cdr->{is_fragmented} = 1;
					$fragmentation = 1;
				}
			}

			# for reseller we first have to find the billing profile
			%reseller_info = ();
			get_reseller_info($cdr->{source_user_id}, $cdr->{start_date},
				\%reseller_info)
				or FATAL "Error getting source reseller info\n";

			if($reseller_info{profile_id}) {
				get_provider_call_cost($cdr, $type, $domain_first, \%reseller_info, \$reseller_cost, \$reseller_free_time, \$rating_duration)
					or FATAL "Error getting reseller cost for cdr ".$cdr->{id}."\n";

				if($split_peak_parts and $cdr->{duration} > $rating_duration) {
					DEBUG "reseller rating_duration: $rating_duration, cdr->duration: $$cdr{duration}.\n";
					$cdr->{duration} = $rating_duration;
					$cdr->{is_fragmented} = 1;
					$fragmentation = 1;
					get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$carrier_cost, \$carrier_free_time, \$rating_duration)
						or FATAL "Error getting carrier cost again for cdr ".$cdr->{id}."\n";
					if($cdr->{duration} != $rating_duration) {
						FATAL "Error getting stable rating fragment for cdr ".$cdr->{id}.". Carrier and reseller profiles don't match.\n";
					}
				} elsif($rating_duration > $cdr->{duration} and $fragmentation) {
					FATAL "Error getting stable rating fragment for cdr ".$cdr->{id}.". Reseller and carrier profiles don't match.\n";
				}
			}
		}
		else
		{
			FATAL "Destination provider id ".$cdr->{destination_provider_id}." has invalid ".
				"class '".$provider_info{class}."' in cdr ".$cdr->{id}."\n";
		}
	}
		
	get_customer_call_cost($cdr, $type, $dst_class, $domain_first, \$customer_cost, \$customer_free_time, \$rating_duration)
		or FATAL "Error getting customer cost for cdr ".$cdr->{id}."\n";

	if($split_peak_parts and $cdr->{duration} > $rating_duration) {
		DEBUG "customer rating_duration: $rating_duration, cdr->duration: $$cdr{duration}.\n";
		$cdr->{duration} = $rating_duration;
		$cdr->{is_fragmented} = 1;
		if($cdr->{destination_provider_id} ne "0") {
			if($dst_class eq 'reseller') {
				if($provider_info{profile_id}) {
					get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$reseller_cost, \$reseller_free_time, \$rating_duration)
						or FATAL "Error getting reseller cost again for cdr ".$cdr->{id}."\n";
				}
			} else {
				if($provider_info{profile_id}) {
					get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$carrier_cost, \$carrier_free_time, \$rating_duration)
						or FATAL "Error getting carrier cost again for cdr ".$cdr->{id}."\n";
					if($cdr->{duration} != $rating_duration) {
						FATAL "Error getting stable rating fragment for cdr ".$cdr->{id}.
				      		". Customer and carrier profiles don't match.\n";
					}
				}
				if($reseller_info{profile_id}) {
					get_provider_call_cost($cdr, $type, $domain_first, \%reseller_info, \$reseller_cost, \$reseller_free_time, \$rating_duration)
						or FATAL "Error getting reseller cost again for cdr ".$cdr->{id}."\n";
				}
			}
			if($cdr->{duration} != $rating_duration) {
				FATAL "Error getting stable rating fragment for cdr ".$cdr->{id}.
			      	". Customer and reseller profiles don't match.\n";
			}
		}
	} elsif($rating_duration > $cdr->{duration} and $fragmentation) {
		DEBUG "rating_duration: $rating_duration, cdr->duration: $$cdr{duration}.\n";
		FATAL "Error getting stable rating fragment for cdr ".$cdr->{id}.
		      ". Customer and reseller/carrier profiles don't match.\n";
	}

	if($split_peak_parts and $fragmentation) {
		my $sth = $sth_create_cdr_fragment;
		$sth->execute($rating_duration, $rating_duration, $cdr->{id})
			or FATAL "Error executing create cdr fragment statement: ".$sth->errstr;
	}

	$cdr->{carrier_cost} = $carrier_cost;
	$cdr->{reseller_cost} = $reseller_cost;
	$cdr->{customer_cost} = $customer_cost;
	$cdr->{carrier_free_time} = $carrier_free_time;
	$cdr->{reseller_free_time} = $reseller_free_time;
	$cdr->{customer_free_time} = $customer_free_time;

	return 1;
}

sub daemonize 
{
	my $pidfile = shift;

	chdir '/' or FATAL "Can't chdir to /: $!\n";
	open STDIN, '/dev/null' or FATAL "Can't read /dev/null: $!\n";
	#open STDOUT, "|-", "logger -t $log_ident" or FATAL "Can't open logger output stream: $!\n";
	#open STDOUT, '>/dev/null' or FATAL "Can't write to /dev/null: $!\n";
	open STDERR, '>&STDOUT' or FATAL "Can't dup stdout: $!\n";
	open PID, ">>$pidfile" or FATAL "Can't open '$pidfile' for writing: $!\n";
	flock(PID, LOCK_EX | LOCK_NB) or FATAL "Unable to lock pidfile '$pidfile': $!\n";
	defined(my $pid = fork) or FATAL "Can't fork: $!\n";
	exit if $pid;
	setsid or FATAL "Can't start a new session: $!\n";
	seek PID, 0, SEEK_SET;
	truncate PID, 0;
	printflush PID "$$\n";
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

	INFO "Up and running.\n";
	while(!$shutdown)
	{
		$billdbh->ping || init_db;
		$acctdbh->ping || init_db;
		undef($prepaid_costs);

		my @cdrs = ();
		eval { get_unrated_cdrs(\@cdrs); };
		if($@) 
		{
			if($DBI::err == 2006)
			{
				INFO "DB connection gone, retrying...";
				next;
			}
			FATAL "Error getting next bunch of CDRs: " . $@;
		}

		unless(@cdrs)
		{
			DEBUG "No new CDRs to rate, sleep $loop_interval";
			sleep($loop_interval);
			next;
		}

		$billdbh->begin_work or FATAL "Error starting transaction: ".$billdbh->errstr;
		$acctdbh->begin_work or FATAL "Error starting transaction: ".$acctdbh->errstr;

		eval 
		{
			foreach my $cdr(@cdrs)
			{
				DEBUG "rate cdr #".$cdr->{id}."\n";
				rate_cdr($cdr, $type)
				    && update_cdr($cdr);
				$rated++;
			}
		};
		if($@)
		{
			if(defined $DBI::err)
			{
				INFO "Caught DBI:err ".$DBI::err, "\n";
				if($DBI::err == 2006)
				{
					INFO "DB connection gone, retrying...";
					next;
				}
			}
			FATAL "Error rating CDR batch: " . $@;
		}

		$billdbh->commit or FATAL "Error committing cdrs: ".$billdbh->errstr;
		$acctdbh->commit or FATAL "Error committing cdrs: ".$acctdbh->errstr;

		DEBUG "$rated CDRs rated so far.\n";
	}

	INFO "Shutting down.\n";

	$sth_billing_info->finish;
	$sth_profile_info->finish;
	$sth_offpeak_weekdays->finish;
	$sth_offpeak_special->finish;
	$sth_unrated_cdrs->finish;
	$sth_update_cdr->finish;
	if($split_peak_parts) {
		$sth_update_cdr_split->finish;
		$sth_create_cdr_fragment->finish;
	}
	$sth_provider_info->finish;
	$sth_reseller_info->finish;
	$sth_get_cbalance->finish;
	$sth_update_cbalance->finish;
	$sth_new_cbalance_week->finish;
	$sth_new_cbalance_month->finish;
	$sth_get_last_cbalance->finish;
	$sth_lnp_number->finish;
	$sth_lnp_profile_info->finish;


	$billdbh->disconnect;
	$acctdbh->disconnect;
	closelog;
	close PID;
	unlink $pidfile;
}
