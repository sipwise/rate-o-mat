#!/usr/bin/perl -w
use strict;
use DBI;
use POSIX;

use Data::Dumper;

my $type = 'call';

my $dbh = DBI->connect("dbi:mysql:database=billing;host=localhost", "soap", "s:wMP4Si") 
	or die "Error connecting do db: $DBI::errstr\n";

my $sth_billing_info = $dbh->prepare(
	"SELECT a.contract_id, b.billing_profile_id, c.cash_balance, ".
	"c.free_time_balance, d.prepaid ".
	"FROM billing.voip_subscribers a, billing.billing_mappings b, ".
	"billing.contract_balances c, billing.billing_profiles d ".
	"WHERE a.uuid = ?  AND a.contract_id = b.contract_id ".
	"AND ( b.start_date IS NULL OR b.start_date <= ?) ".
	"AND ( b.end_date IS NULL OR b.end_date >= ? ) ".
	"AND a.contract_id = c.contract_id ".
	"AND b.billing_profile_id = d.id ".
	"ORDER BY b.start_date DESC ".
	"LIMIT 1"
) or die "Error preparing billing info statement: $dbh->errstr\n";
	
my $sth_profile_info = $dbh->prepare(
	"SELECT id, destination, ".
	"onpeak_init_rate, onpeak_init_interval, ".
	"onpeak_follow_rate, onpeak_follow_interval, ".
	"offpeak_init_rate, offpeak_init_interval, ".
	"offpeak_follow_rate, offpeak_follow_interval ".
	"FROM billing.billing_fees WHERE billing_profile_id = ? ".
	"AND type = ? AND ? REGEXP(destination) ".
	"ORDER BY LENGTH(destination) DESC LIMIT 1"
) or die "Error preparing profile info statement: $dbh->errstr\n";

my $sth_offpeak_weekdays = $dbh->prepare(
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
	"OR weekday <= WEEKDAY(DATE_ADD(?, INTERVAL ?SECOND)))"
) or die "Error preparing weekday offpeak statement: $dbh->errstr\n";

my $sth_offpeak_special = $dbh->prepare(
	"SELECT UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end) ".
	"FROM billing.billing_peaktime_special ".
	"WHERE billing_profile_id = ? ".
	"AND ( ".
	"start <= ? AND end >= ? ".
	"OR start >= ? AND end <= DATE_ADD(?, INTERVAL ? SECOND) ".
	"OR start <= DATE_ADD(?, INTERVAL ? SECOND) AND end >= DATE_ADD(?, INTERVAL ? SECOND) ".
	")"
) or die "Error preparing special offpeak statement: $dbh->errstr\n";

my $sth_unrated_cdrs = $dbh->prepare(
	"SELECT id, ".
	"source_user_id, source_provider_id, ".
	"destination_user_id, destination_provider_id, destination_user, destination_domain, ".
	"start_time, duration ".
	"FROM accounting.cdr WHERE rated_at IS NULL AND call_status = 'ok' ".
	"LIMIT 10000"
) or die "Error preparing unrated cdr statement: $dbh->errstr\n";

my $sth_update_cdr = $dbh->prepare(
	"UPDATE accounting.cdr SET ".
	"carrier_cost = ?, reseller_cost = ?, customer_cost = ?, ".
	"rated_at = now(), billing_fee_id = ? ".
	"WHERE id = ?"
) or die "Error preparing update cdr statement: $dbh->errstr\n";


sub get_billing_info
{
	my $start_str = shift;
	my $uid = shift;
	my $r_info = shift;

	my $sth = $sth_billing_info;

	$sth->execute($uid, $start_str, $start_str) or
		die "Error executing billing info statement: $dbh->errstr\n";
	my @res = $sth->fetchrow_array();
	die "No billing info found for uuid '".$uid."'\n" unless @res;

	$r_info->{contract_id} = $res[0];
	$r_info->{profile_id} = $res[1];
	$r_info->{cash_balance} = $res[2];
	$r_info->{free_time} = $res[3];
	$r_info->{prepaid} = $res[4];
	
	$sth->finish;
	
	return 1;
}

sub get_profile_info
{
	my $bpid = shift;
	my $type = shift;
	my $destination = shift;
	my $b_info = shift;

	my $sth = $sth_profile_info;

	$sth->execute($bpid, $type, $destination)
		or die "Error executing profile info statement: $dbh->errstr\n";
	
	my @res = $sth->fetchrow_array();
	die "No profile info found for profile id '".$bpid."' and destination '".
		$destination."' (".$type.")\n" unless @res;
	
	$b_info->{fee_id} = $res[0];
	$b_info->{pattern} = $res[1];
	$b_info->{on_init_rate} = $res[2];
	$b_info->{on_init_interval} = $res[3];
	$b_info->{on_follow_rate} = $res[4];
	$b_info->{on_follow_interval} = $res[5];
	$b_info->{off_init_rate} = $res[6];
	$b_info->{off_init_interval} = $res[7];
	$b_info->{off_follow_rate} = $res[8];
	$b_info->{off_follow_interval} = $res[9];
	
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
	) or die "Error executing weekday offpeak statement: $dbh->errstr\n";

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
	) or die "Error executing special offpeak statement: $dbh->errstr\n";

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

sub set_start_unixtime
{
	my $start = shift;
	my $r_unix = shift;

	my ($y, $m, $d, $H, $M, $S) = $start =~ /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/;
	$$r_unix = mktime($S, $M, $H, $d, $m-1, $y-1900, 0, 0, -1);

	return 0;
}

sub get_unrated_cdrs
{
	my $r_cdrs = shift;

	my $sth = $sth_unrated_cdrs;
	$sth->execute
		or die "Error executing unrated cdr statement: $dbh->errstr\n";

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
		$cdr{start_time} = $res[7];
		$cdr{duration} = $res[8];

		push @$r_cdrs, \%cdr;
	}

	return 1;
}

sub update_cdr
{
	my $cdr = shift;

	my $sth = $sth_update_cdr;
	$sth->execute($cdr->{carrier_cost}, $cdr->{reseller_cost}, $cdr->{customer_cost},
		$cdr->{billing_fee_id}, $cdr->{id})
		or die "Error executing update cdr statement: $dbh->errstr\n";
	return 1;
}

sub rate_cdr
{
	my $cdr = shift;
	my $type = shift;

	my $start_unixtime;
	set_start_unixtime($cdr->{start_time}, \$start_unixtime);

	#print "unix-time=$start_unixtime\n";

	# TODO: not only customer cost, also reseller and carrier cost!
	
	# TODO: distinguish between incoming and outgoing calls (no customer rating if
	# source_user_id = 0

	# TODO: onnet-calls must be rated differently, since it's mostly free

	my %billing_info = ();
	get_billing_info($cdr->{start_time}, $cdr->{source_user_id}, \%billing_info) or
		die "Error getting billing info\n";
	#print Dumper \%billing_info;

	my %profile_info = ();
	get_profile_info($billing_info{profile_id}, $type, $cdr->{destination_user}, 
		\%profile_info) or
		die "Error getting profile info\n";
	#print Dumper \%profile_info;

	my @offpeak_weekdays = ();
	get_offpeak_weekdays($billing_info{profile_id}, $cdr->{start_time}, 
		$cdr->{duration}, \@offpeak_weekdays) or
		die "Error getting weekdays offpeak info\n";
	#print Dumper \@offpeak_weekdays;

	my @offpeak_special = ();
	get_offpeak_special($billing_info{profile_id}, $cdr->{start_time}, 
		$cdr->{duration}, \@offpeak_special) or
		die "Error getting special offpeak info\n";
	#print Dumper \@offpeak_special;

	my $cost = 0;
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
				$profile_info{on_init_interval} : $profile_info{off_init_interval};
			$rate = $onpeak == 1 ? 
				$profile_info{on_init_rate} : $profile_info{off_init_rate};
		}
		else
		{
			$interval = $onpeak == 1 ? 
				$profile_info{on_follow_interval} : $profile_info{off_follow_interval};
			$rate = $onpeak == 1 ? 
				$profile_info{on_follow_rate} : $profile_info{off_follow_rate};
		}

		$cost += $rate;
		$duration -= $interval;
		$offset += $interval;

		#print "int=$interval, rate=$rate, cost=$cost\n";
	}

	# TODO: set proper costs here:

	$cdr->{carrier_cost} = $cost;
	$cdr->{reseller_cost} = $cost;
	$cdr->{customer_cost} = $cost;
	$cdr->{billing_fee_id} = $profile_info{fee_id};

	return 1;
}


my $shutdown = 0;
my $loop_interval = 10;
my $rated = 0;

while(!$shutdown)
{
	my @cdrs = ();
	get_unrated_cdrs(\@cdrs)
		or die "Error getting next bunch of CDRs\n";
	unless(@cdrs)
	{
		sleep($loop_interval);
		next;
	}

	foreach my $cdr(@cdrs)
	{
		print "rate cdr #".$cdr->{id}."\n";
		rate_cdr($cdr, $type)
			or die "Error rating CDR id ".$cdr->{id}."\n";
		update_cdr($cdr)
			or die "Error updating CDR id ".$cdr->{id}."\n";
		$rated++;
	}

	print "$rated CDRs rated so far.\n";
}

$dbh->disconnect;
