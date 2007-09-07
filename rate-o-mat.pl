#!/usr/bin/perl -w
use strict;
use DBI;
use POSIX;

use Data::Dumper;

my $start = '2007-09-05 07:58:00';
my $duration = 490;
my $uuid = '1959bbe9-9680-4089-b898-30002f6b542d';
my $type = 'call';
#my $destination = '4369910811299';
my $destination = '43650123456';

my $dbh;

sub get_billing_info
{
	my $start_str = shift;
	my $uid = shift;
	my $r_info = shift;

	my $sth = $dbh->prepare(
		"SELECT a.contract_id, b.billing_profile_id, c.cash_balance, ".
		"c.free_time_balance, d.prepaid ".
		"FROM voip_subscribers a, billing_mappings b, ".
		"contract_balance c, billing_profiles d ".
		"WHERE a.uuid = ?  AND a.contract_id = b.contract_id ".
		"AND ( b.start_date IS NULL OR b.start_date <= ?) ".
		"AND ( b.end_date IS NULL OR b.end_date >= ? ) ".
		"AND a.contract_id = c.contract_id ".
		"AND b.billing_profile_id = d.id ".
		"ORDER BY b.start_date DESC ".
		"LIMIT 1"
	) or die "Error preparing billing info statement: $dbh->errstr\n";

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
	
	my $sth = $dbh->prepare(
		"SELECT destination, ".
		"onpeak_init_rate, onpeak_init_interval, ".
		"onpeak_follow_rate, onpeak_follow_interval, ".
		"offpeak_init_rate, offpeak_init_interval, ".
		"offpeak_follow_rate, offpeak_follow_interval ".
		"FROM billing_fees WHERE billing_profile_id = ? ".
		"AND type = ? AND ? REGEXP(destination) ".
		"ORDER BY LENGTH(destination) DESC LIMIT 1"
	) or die "Error preparing profile info statement: $dbh->errstr\n";

	$sth->execute($bpid, $type, $destination)
		or die "Error executing profile info statement: $dbh->errstr\n";
	
	my @res = $sth->fetchrow_array();
	die "No profile info found for profile id '".$bpid."' and destination '".
		$destination."' (".$type.")\n" unless @res;
	
	$b_info->{pattern} = $res[0];
	$b_info->{on_init_rate} = $res[1];
	$b_info->{on_init_interval} = $res[2];
	$b_info->{on_follow_rate} = $res[3];
	$b_info->{on_follow_interval} = $res[4];
	$b_info->{off_init_rate} = $res[5];
	$b_info->{off_init_interval} = $res[6];
	$b_info->{off_follow_rate} = $res[7];
	$b_info->{off_follow_interval} = $res[8];
	
	$sth->finish;

	return 1;
}

sub get_offpeak_weekdays
{
	my $bpid = shift;
	my $start = shift;
	my $duration = shift;
	my $r_offpeaks = shift;

	my $sth = $dbh->prepare(
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(?) <= WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"AND weekday >= WEEKDAY(?) ".
		"AND weekday <= WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"UNION ".
		"SELECT weekday, TIME_TO_SEC(start), TIME_TO_SEC(end) ".
		"FROM billing_peaktime_weekdays ".
		"WHERE billing_profile_id = ? ".
		"AND WEEKDAY(?) > WEEKDAY(DATE_ADD(?, INTERVAL ? SECOND)) ".
		"AND (weekday >= WEEKDAY(?) ".
		"OR weekday <= WEEKDAY(DATE_ADD(?, INTERVAL ?SECOND)))"
	) or die "Error preparing weekday offpeak statement: $dbh->errstr\n";

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

	my $sth = $dbh->prepare(
		"SELECT UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end) ".
		"FROM billing_peaktime_special ".
		"WHERE billing_profile_id = ? ".
		"AND ( ".
		"start <= ? AND end >= ? ".
		"OR start >= ? AND end <= DATE_ADD(?, INTERVAL ? SECOND) ".
		"OR start <= DATE_ADD(?, INTERVAL ? SECOND) AND end >= DATE_ADD(?, INTERVAL ? SECOND) ".
		")"
	) or die "Error preparing special offpeak statement: $dbh->errstr\n";

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

$dbh = DBI->connect("dbi:mysql:database=billing;host=localhost", "soap", "s:wMP4Si") 
	or die "Error connecting do db: $DBI::errstr\n";

my $start_unixtime;
set_start_unixtime($start, \$start_unixtime);

#print "unix-time=$start_unixtime\n";

my %billing_info = ();
get_billing_info($start, $uuid, \%billing_info) or
	die "Error getting billing info\n";
#print Dumper \%billing_info;

my %profile_info = ();
get_profile_info($billing_info{profile_id}, $type, $destination, \%profile_info) or
	die "Error getting profile info\n";
#print Dumper \%profile_info;

my @offpeak_weekdays = ();
get_offpeak_weekdays($billing_info{profile_id}, $start, $duration, \@offpeak_weekdays) or
	die "Error getting weekdays offpeak info\n";
#print Dumper \@offpeak_weekdays;

my @offpeak_special = ();
get_offpeak_special($billing_info{profile_id}, $start, $duration, \@offpeak_special) or
	die "Error getting special offpeak info\n";
#print Dumper \@offpeak_special;

my $cost = 0;
my $interval = 0;
my $rate = 0;
my $offset = 0;
my $onpeak = 0;
my $init = 0;

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

print "\ncost=$cost\n";

$dbh->disconnect;
