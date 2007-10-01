#!/usr/bin/perl -w
use strict;
use DBI;
use POSIX qw(setsid mktime);
use Fcntl qw(LOCK_EX LOCK_NB);
use Sys::Syslog;
use Data::Dumper;

my $fork = 0;
my $pidfile = '/var/run/rate-o-mat.pid';
my $type = 'call';
my $loop_interval = 10;

my $log_ident = 'rate-o-mat';
my $log_facility = 'daemon';
my $log_opts = 'ndely,cons,pid,nowait';

########################################################################

sub main;

my $shutdown = 0;

my $dbh;
my $sth_billing_info;
my $sth_profile_info;
my $sth_offpeak_weekdays;
my $sth_offpeak_special;
my $sth_unrated_cdrs;
my $sth_update_cdr;
my $sth_provider_info;
my $sth_reseller_info;
my $sth_get_cbalance;
my $sth_update_cbalance;
my $sth_new_cbalance_week;
my $sth_new_cbalance_month;
my $sth_get_last_cbalance;
my $sth_is_freetime;

main;
exit 0;

########################################################################

sub FATAL
{
	my $msg = shift;
	chomp $msg;
	print "FATAL: $msg\n" if($fork != 1);
	syslog('crit', $msg);
	closelog();
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


sub init_db
{
	$dbh = DBI->connect("dbi:mysql:database=billing;host=localhost", "soap", "s:wMP4Si") 
		or FATAL "Error connecting do db: ".$DBI::errstr;

	$sth_billing_info = $dbh->prepare(
		"SELECT a.contract_id, b.billing_profile_id, ".
		"d.prepaid, d.interval_charge, d.interval_free_time, d.interval_free_cash, ".
		"d.interval_unit, d.interval_count ".
		"FROM billing.voip_subscribers a, billing.billing_mappings b, ".
		"billing.billing_profiles d ".
		"WHERE a.uuid = ?  AND a.contract_id = b.contract_id ".
		"AND ( b.start_date IS NULL OR b.start_date <= ?) ".
		"AND ( b.end_date IS NULL OR b.end_date >= ? ) ".
		"AND b.billing_profile_id = d.id ".
		"ORDER BY b.start_date DESC ".
		"LIMIT 1"
	) or FATAL "Error preparing billing info statement: ".$dbh->errstr;
	
	$sth_profile_info = $dbh->prepare(
		"SELECT id, destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval ".
		"FROM billing.billing_fees WHERE billing_profile_id = ? ".
		"AND type = ? AND ? REGEXP(destination) ".
		"ORDER BY LENGTH(destination) DESC LIMIT 1"
	) or FATAL "Error preparing profile info statement: ".$dbh->errstr;

	$sth_offpeak_weekdays = $dbh->prepare(
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing.billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(?) <= WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"AND weekday >= WEEKDAY(?) ".
		"AND weekday <= WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"UNION ".
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing.billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(?) > WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"AND (weekday >= WEEKDAY(?) ".
		"OR weekday <= WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)))"
	) or FATAL "Error preparing weekday offpeak statement: ".$dbh->errstr;

	$sth_offpeak_special = $dbh->prepare(
		"SELECT UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end) ".
		"FROM billing.billing_peaktime_special ".
		"WHERE billing_profile_id = ? ".
		"AND ( ".
		"start <= ? AND end >= ? ".
		"OR start >= ? AND end <= DATE_ADD(?, INTERVAL ? SECOND) ".
		"OR start <= DATE_ADD(?, INTERVAL ? SECOND) AND end >= DATE_ADD(?, INTERVAL ? SECOND) ".
		")"
	) or FATAL "Error preparing special offpeak statement: ".$dbh->errstr;

	$sth_unrated_cdrs = $dbh->prepare(
		"SELECT id, ".
		"source_user_id, source_provider_id, ".
		"destination_user_id, destination_provider_id, ".
		"destination_user, destination_domain, ".
		"destination_user_in, destination_domain_in, ".
		"start_time, duration, call_status ".
		"FROM accounting.cdr WHERE rating_status = 'unrated' ".
		"ORDER BY start_time ASC LIMIT 10000"
	) or FATAL "Error preparing unrated cdr statement: ".$dbh->errstr;

	$sth_update_cdr = $dbh->prepare(
		"UPDATE accounting.cdr SET ".
		"carrier_cost = ?, reseller_cost = ?, customer_cost = ?, ".
		"rated_at = now(), rating_status = ?, billing_fee_id = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update cdr statement: ".$dbh->errstr;

	$sth_provider_info = $dbh->prepare(
		"SELECT p.class, bm.billing_profile_id ".
		"FROM billing.products p, billing.billing_mappings bm ".
		"WHERE bm.contract_id = ? AND bm.product_id = p.id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= ?) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= ?)"
	) or FATAL "Error preparing provider info statement: ".$dbh->errstr;

	$sth_reseller_info = $dbh->prepare(
		"SELECT bm.billing_profile_id ".
		"FROM billing.billing_mappings bm, billing.voip_subscribers vs, ".
		"billing.contracts c ".
		"WHERE vs.uuid = ? AND vs.contract_id = c.id ".
		"AND c.reseller_id = bm.contract_id ".
		"AND (bm.start_date IS NULL OR bm.start_date <= ?) ".
		"AND (bm.end_date IS NULL OR bm.end_date >= ?)"
	) or FATAL "Error preparing reseller info statement: ".$dbh->errstr;
	
	$sth_get_cbalance = $dbh->prepare(
		"SELECT id, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, start ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"start <= ? AND end >= ? ".
		"UNION ALL ".
		"SELECT id, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval, start ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"start > ? ORDER BY start ASC"

	) or FATAL "Error preparing get contract balance statement: ".$dbh->errstr;
	
	$sth_get_last_cbalance = $dbh->prepare(
		"SELECT id, end, cash_balance, cash_balance_interval, ".
		"free_time_balance, free_time_balance_interval ".
		"FROM billing.contract_balances ".
		"WHERE contract_id = ? AND ".
		"start <= ? AND end <= ? ORDER BY end DESC LIMIT 1"
	) or FATAL "Error preparing get last contract balance statement: ".$dbh->errstr;
	
	$sth_new_cbalance_week = $dbh->prepare(
		"INSERT INTO billing.contract_balances VALUES(NULL, ?, ?, ?, ?, ?, ".
		"DATE_ADD(?, INTERVAL 1 SECOND), DATE_ADD(?, INTERVAL ? WEEK) )"
	) or FATAL "Error preparing create contract balance statement: ".$dbh->errstr;

	$sth_new_cbalance_month = $dbh->prepare(
		"INSERT INTO billing.contract_balances VALUES(NULL, ?, ?, ?, ?, ?, ".
		"DATE_ADD(?, INTERVAL 1 SECOND), ".
		"FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_DAY(DATE_ADD(?, INTERVAL ? MONTH))))".
		")"
	) or FATAL "Error preparing create contract balance statement: ".$dbh->errstr;
	
	$sth_update_cbalance = $dbh->prepare(
		"UPDATE billing.contract_balances SET ".
		"cash_balance = ?, cash_balance_interval = ?, ".
		"free_time_balance = ?, free_time_balance_interval = ? ".
		"WHERE id = ?"
	) or FATAL "Error preparing update contract balance statement: ".$dbh->errstr;

	$sth_is_freetime = $dbh->prepare(
		"SELECT id FROM billing.billing_free_time_destinations ".
		"WHERE billing_profile_id = ? AND billing_fee_id = ?"
	) or FATAL "Error preparing freetime statement: ".$dbh->errstr;

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
		or FATAL "Error executing get contract balance statement: ".$dbh->errstr;
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
		or FATAL "Error executing new contract balance statement: ".$dbh->errstr;

	$r_res->{id} = $dbh->last_insert_id(undef, undef, undef, undef);
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
	my $cost = shift;
	my $rduration = shift;
	my $r_balances = shift;

	my $sth = $sth_get_cbalance;
	$sth->execute(
		$binfo->{contract_id}, $cdr->{start_time}, $cdr->{start_time},
		$binfo->{contract_id}, $cdr->{start_time})
		or FATAL "Error executing get contract balance statement: ".$dbh->errstr;
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
			$sth = $sth_is_freetime;
			$sth->execute($binfo->{profile_id}, $pinfo->{fee_id})
				or FATAL "Error executing freetime statement: ".$dbh->errstr;
			my @r = $sth->fetchrow_array();

			if($binfo->{prepaid} == 1)
			{
				WARNING "TODO: do we need to process prepaid balances here?";
			}
			else
			{
				if(@r && $balance{free_time_balance} >= $$rduration)
				{
					$balance{free_time_balance} -= $$rduration;
					$balance{free_time_balance_interval} += $$rduration;
					$$cost = 0;
				}
				else
				{
					# TODO: also decrement cash balance? Is this possible for post-paid?
					$balance{cash_balance_interval} += $$cost;
				}
			}
		}

		if($i < @$res - 1)
		{
			# TODO: shift calculated values to next balance
			# if call falls in an old balance
		}


		print "contract balance:\n";
		print Dumper \%balance;
	
		$sth = $sth_update_cbalance;
		$sth->execute(
			$balance{cash_balance}, $balance{cash_balance_interval},
			$balance{free_time_balance}, $balance{free_time_balance_interval},
			$balance{id})
			or FATAL "Error executing update contract balance statement: ".$dbh->errstr;

		push @$r_balances, \%balance;
	}

	return 1;
}

sub update_contract_balance
{
	my $cdr = shift;
	my $binfo = shift;
	my $pinfo = shift;
	my $cost = shift;
	my $rduration = shift;

	my @balances = ();

	get_contract_balance($cdr, $binfo, $pinfo, $cost, $rduration, \@balances)
		or FATAL "Error getting contract balances\n";

	# the above does the update as well, so we're done here
	

	return 1;
}

sub get_billing_info
{
	my $start_str = shift;
	my $uid = shift;
	my $r_info = shift;

	my $sth = $sth_billing_info;

	$sth->execute($uid, $start_str, $start_str) or
		FATAL "Error executing billing info statement: ".$dbh->errstr;
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

	my $sth = $sth_profile_info;

	$sth->execute($bpid, $type, $destination)
		or FATAL "Error executing profile info statement: ".$dbh->errstr;
	
	my @res = $sth->fetchrow_array();
	return 0 unless @res;
	
	$b_info->{fee_id} = $res[0];
	$b_info->{pattern} = $res[1];
	$b_info->{on_init_rate} = $res[2];
	$b_info->{on_init_interval} = $res[3] == 0 ? 1 : $res[3]; # prevent loops
	$b_info->{on_follow_rate} = $res[4];
	$b_info->{on_follow_interval} = $res[5] == 0 ? 1 : $res[5];
	$b_info->{off_init_rate} = $res[6];
	$b_info->{off_init_interval} = $res[7] == 0 ? 1 : $res[7];;
	$b_info->{off_follow_rate} = $res[8];
	$b_info->{off_follow_interval} = $res[9] == 0 ? 1 : $res[9];;
	
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
	) or FATAL "Error executing weekday offpeak statement: ".$dbh->errstr;

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
	) or FATAL "Error executing special offpeak statement: ".$dbh->errstr;

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
		or FATAL "Error executing unrated cdr statement: ".$dbh->errstr;

	while(my @res = $sth->fetchrow_array())
	{
		my %cdr = ();
		$cdr{id} = $res[0];
		$cdr{source_user_id} = $res[1];
		$cdr{source_provider_id} = $res[2];
		$cdr{destination_user_id} = $res[3];
		$cdr{destination_provider_id} = $res[4];
		$cdr{destination_user} = $res[5];
		$cdr{destination_domain} = $res[6];
		$cdr{destination_user_in} = $res[7];
		$cdr{destination_domain_in} = $res[8];
		$cdr{start_time} = $res[9];
		$cdr{duration} = $res[10];
		$cdr{call_status} = $res[11];

		push @$r_cdrs, \%cdr;
	}

	return 1;
}

sub update_cdr
{
	my $cdr = shift;

	my $sth = $sth_update_cdr;
	$sth->execute($cdr->{carrier_cost}, $cdr->{reseller_cost}, $cdr->{customer_cost},
		'ok', $cdr->{billing_fee_id}, $cdr->{id})
		or FATAL "Error executing update cdr statement: ".$dbh->errstr;
	return 1;
}

sub update_failed_cdr
{
	my $cdr = shift;

	my $sth = $sth_update_cdr;
	$sth->execute('NULL', 'NULL', 'NULL', 'failed', 'NULL', $cdr->{id})
		or FATAL "Error executing update cdr statement: ".$dbh->errstr;
	return 1;
}

sub get_provider_info
{
	my $pid = shift;
	my $start = shift;
	my $r_info = shift;

	my $sth = $sth_provider_info;
	$sth->execute($pid, $start, $start)
		or FATAL "Error executing provider info statement: ".$dbh->errstr;
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
		or FATAL "Error executing reseller info statement: ".$dbh->errstr;
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
	my $r_rating_duration = shift;

	my $dst_user;
	my $dst_domain;
	my $first;
	my $second;

	if($destination_class eq "pstnpeering" || $destination_class eq "sippeering")
	{
		$dst_user = $cdr->{destination_user};
		$dst_domain = '@'.$cdr->{destination_domain};
	}
	else
	{
		$dst_user = $cdr->{destination_user_in};
		$dst_domain = '@'.$cdr->{destination_domain_in};
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



	my $start_unixtime;
	set_start_unixtime($cdr->{start_time}, \$start_unixtime);
	
	unless(get_profile_info($profile_id, $type, $destination_class, $first, 
		$r_profile_info))
	{
		unless(get_profile_info($profile_id, $type, $destination_class, $second, 
			$r_profile_info))
		{
			WARNING "No fee info for profile $profile_id and user '$dst_user' ".
				"or domain '$dst_domain' found\n";
			$$r_cost = 0;
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
	my $interval = 0;
	my $rate = 0;
	my $offset = 0;
	my $onpeak = 0;
	my $init = 0;
	my $duration = $cdr->{duration};

	while($duration > 0)
	{
		if(is_offpeak_special($start_unixtime, $offset, \@offpeak_special))
		{
			#print "offset $offset is offpeak-special\n";
			$onpeak = 0;
		}
		elsif(is_offpeak_weekday($start_unixtime, $offset, \@offpeak_weekdays))
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
		}
		else
		{
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
	my $rating_duration = 0;

	my %billing_info = ();
	get_billing_info($cdr->{start_time}, $cdr->{source_user_id}, \%billing_info) or
		FATAL "Error getting billing info\n";
	#print Dumper \%billing_info;

	my %profile_info = ();
	get_call_cost($cdr, $type, $destination_class, $billing_info{profile_id}, 
		$domain_first, \%profile_info, $r_cost, \$rating_duration)
		or FATAL "Error getting customer call cost\n";

	unless($billing_info{prepaid} == 1)
	{
		update_contract_balance($cdr, \%billing_info, \%profile_info, $r_cost, \$rating_duration)
			or FATAL "Error updating customer contract balance\n";
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
	my $rating_duration = 0;

	my %profile_info = ();
	get_call_cost($cdr, $type, $r_info->{class}, 
		$r_info->{profile_id}, $domain_first, \%profile_info, $r_cost, \$rating_duration)
		or FATAL "Error getting provider call cost\n";
	
	# TODO: also update carrier/reseller balance (we're missing the billing_info, right?)

	return 1;
}

sub rate_cdr
{
	my $cdr = shift;
	my $type = shift;

	my $customer_cost = 0;
	my $carrier_cost = 0;
	my $reseller_cost = 0;
	
	unless($cdr->{call_status} eq "ok")
	{
		$cdr->{carrier_cost} = $carrier_cost;
		$cdr->{reseller_cost} = $reseller_cost;
		$cdr->{customer_cost} = $customer_cost;
		return 1;
	}

	if($cdr->{source_user_id} eq "0")
	{
		# caller is not local

		if($cdr->{source_provider_id} == 0)
		{
			WARNING "CDR id ".$cdr->{id}." has no source uid/pid!\n";
			update_failed_cdr($cdr);
			return 1;
		}
		if($cdr->{destination_user_id} eq "0")
		{
			# a relay? must not happen!
			WARNING "CDR id ".$cdr->{id}." has wether source nor destination uid/pid!\n";
			update_failed_cdr($cdr);
			return 1;
		}

		# TODO: should there be an incoming profile to calculate termination fees?
	
		$customer_cost = 0;
		$carrier_cost = 0;
		$reseller_cost = 0;
	}
	else
	{
		# caller is local

		if($cdr->{source_provider_id} == 0)
		{
			WARNING "CDR id ".$cdr->{id}." has no source provider id\n";
			update_failed_cdr($cdr);
			return 1;
		}
		if($cdr->{destination_provider_id} == 0)
		{
			WARNING "CDR id ".$cdr->{id}." has no destination provider id\n";
			update_failed_cdr($cdr);
			return 1;
		}

		my %provider_info = ();
		get_provider_info($cdr->{destination_provider_id}, $cdr->{start_date},
			\%provider_info)
			or FATAL "Error getting destination provider info\n";


		my $dst_class;
		my $domain_first = 0;

		if($provider_info{class} eq "reseller")
		{
			$dst_class = 'reseller';
			$domain_first = 1; # priorize domain over user to correctly rate onnet-calls

			# only calculate reseller cost, carrier cost is 0 (hosting-onnet)
			get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$reseller_cost)
				or FATAL "Error getting reseller cost for cdr ".$cdr->{id}."\n";
			$carrier_cost = 0;
		}
		elsif($provider_info{class} eq "sippeering" || $provider_info{class} eq "pstnpeering")
		{
			$dst_class = $provider_info{class};
			$domain_first = 0; # for calls leaving our system, priorize user over domain

			# carrier cost can be calculated directly with available billing profile
			get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$carrier_cost)
				or FATAL "Error getting carrier cost for cdr ".$cdr->{id}."\n";
	
			# for reseller we first have to find the billing profile
			%provider_info = ();
			get_reseller_info($cdr->{source_user_id}, $cdr->{start_date},
				\%provider_info)
				or FATAL "Error getting source reseller info\n";
			get_provider_call_cost($cdr, $type, $domain_first, \%provider_info, \$reseller_cost)
				or FATAL "Error getting reseller cost for cdr ".$cdr->{id}."\n";
		}
		else
		{
			FATAL "Destination provider id ".$cdr->{destination_provider_id}." has invalid ".
				"class '".$provider_info{class}."' in cdr ".$cdr->{id}."\n";
		}
			
		get_customer_call_cost($cdr, $type, $dst_class, $domain_first, \$customer_cost)
			or FATAL "Error getting customer cost for cdr ".$cdr->{id}."\n";
	}


	$cdr->{carrier_cost} = $carrier_cost;
	$cdr->{reseller_cost} = $reseller_cost;
	$cdr->{customer_cost} = $customer_cost;

	# TODO: there should be an id for every of the three costs!?
	$cdr->{billing_fee_id} = 0;

	return 1;
}

sub daemonize 
{
	my $pidfile = shift;

	chdir '/' or FATAL "Can't chdir to /: $!\n";
	open STDIN, '/dev/null' or FATAL "Can't read /dev/null: $!\n";
	#open STDOUT, "|-", "logger -t $log_ident" or FATAL "Can't open logger output stream: $!\n";
	#open STDOUT, '>/dev/null' or FATAL "Can't write to /dev/null: $!\n";
	defined(my $pid = fork) or FATAL "Can't fork: $!\n";
	exit if $pid;
	setsid or FATAL "Can't start a new session: $!\n";
	open STDERR, '>&STDOUT' or FATAL "Can't dup stdout: $!\n";
	open PID, ">$pidfile" or FATAL "Can't write to pidfile '$pidfile': $!\n";
	flock(PID, LOCK_EX | LOCK_NB) || FATAL "Unable to lock pidfile '$pidfile': $!\n";
	print PID "$$\n";
	close PID;
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
		my @cdrs = ();
		get_unrated_cdrs(\@cdrs)
			or FATAL "Error getting next bunch of CDRs\n";
		unless(@cdrs)
		{
			sleep($loop_interval);
			next;
		}

		foreach my $cdr(@cdrs)
		{
			DEBUG "rate cdr #".$cdr->{id}."\n";
			rate_cdr($cdr, $type)
				or FATAL "Error rating CDR id ".$cdr->{id}."\n";
			update_cdr($cdr)
				or FATAL "Error updating CDR id ".$cdr->{id}."\n";
			$rated++;
		}

		DEBUG "$rated CDRs rated so far.\n";
	}

	INFO "Shutting down.\n";

	$sth_billing_info->finish;
	$sth_profile_info->finish;
	$sth_offpeak_weekdays->finish;
	$sth_offpeak_special->finish;
	$sth_unrated_cdrs->finish;
	$sth_update_cdr->finish;
	$sth_provider_info->finish;
	$sth_reseller_info->finish;
	$sth_get_cbalance->finish;
	$sth_update_cbalance->finish;
	$sth_new_cbalance_week->finish;
	$sth_new_cbalance_month->finish;
	$sth_get_last_cbalance->finish;
	$sth_is_freetime->finish;


	$dbh->disconnect;
	closelog;
}
