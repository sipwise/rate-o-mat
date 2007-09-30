#!/usr/bin/perl -w
use strict;
use DBI;

use Data::Dumper;

# TODO: only calls allowed yet
my $type = 'call';

my $file = $ARGV[0];
my $provider_id = $ARGV[1];

die "Usage: $0 <csv-file> <provilder-id>\n"
	unless(defined $file && defined $provider_id 
	&& $provider_id =~ /^[0-9]+$/);

my $dbh = DBI->connect("dbi:mysql:database=billing;host=localhost", "soap", "s:wMP4Si") 
	or die "Error connecting do db: $DBI::errstr\n";
my $sth = $dbh->prepare("set autocommit=0")
	or die "Error preparing autocommit=0 statement: $dbh->errstr\n";
$sth->execute
	or die "Error setting autocommit=0: $dbh->errstr\n";
$sth->finish;
my $sth_rollback = $dbh->prepare("rollback")
	or die "Error preparing rollback statement: $dbh->errstr\n";

my $sth_fee = $dbh->prepare("insert into billing_fees values(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	or die "Error preparing billing fees statement: $dbh->errstr\n";

# TODO: reseller_id is only NULL for carriers and resellers
my $sth_profile = $dbh->prepare("insert into billing_profiles values(NULL, NULL, ?, ?, ?, ?, ?, ?, ?)")
	or die "Error preparing billing fees statement: $dbh->errstr\n";
my $sth_freetime = $dbh->prepare("insert into billing_free_time_destinations values(NULL, ?, ?)")
	or die "Error preparing billing fees statement: $dbh->errstr\n";

open FH, $file or die "Error opening CSV file '$file': $!\n";

my $line = 0;
my $effective_line = 0;

my $init_interval = 0;
my $follow_interval = 0;
my $prepaid = 0;
my $int_charge = 0;
my $int_freetime = 0;
my $int_freecash = 0;
my $int_unit;
my $int_length = 0;
my $p_name;
my $profile_id = 0;

sub FATAL
{
	my $msg = shift;
	chomp $msg;

	$sth_rollback->execute
		or die "Error rolling back DB entries, manual cleanup needed!\n";
	die $msg."\n";
}

sub round
{
	my $c = shift;
	return sprintf("%.4f", $c);
}


while(<FH>)
{
	$line++;
	chomp;
	next if (/^\#/ || /^\s*$/);
	$effective_line++;

	if($effective_line == 1)
	{
		my ($pulsing, $payment);
		s/\'|\"|\s//g;
		($p_name, $pulsing, $payment, $int_charge, $int_freetime, $int_freecash, $int_unit, $int_length) 
			= split /\;/;
		
		die "No profile name given in line $line: $_\n" 
			unless(defined $p_name && length($p_name) > 0);
		
		($init_interval, $follow_interval) = split /\,/, $pulsing;
		die "Invalid interval values in line $line: $_\n" 
			unless(defined $init_interval && $init_interval =~ /^[0-9]+$/ &&
				defined $follow_interval && $follow_interval =~ /^[0-9]+$/);
		
		if(defined $payment && $payment eq "prepaid")
		{
			$prepaid = 1;
		}
		elsif(defined $payment && $payment eq "postpaid")
		{
			$prepaid = 0;
		}
		else
		{
			die "Invalid payment method '$payment' in line $line, must be 'prepaid' or 'postpaid'\n";
		}
		
		die "Invalid interval charge value '$int_charge' in $line: $_\n" 
			unless(defined $int_charge && $int_charge =~ /^[0-9]+(\.[0-9]+)?$/);
		
		die "Invalid interval freetime value '$int_freetime' in $line: $_\n" 
			unless(defined $int_freetime && $int_freetime =~ /^[0-9]+$/);
		
		die "Invalid interval free cash value '$int_freecash' in $line: $_\n" 
			unless(defined $int_freecash && $int_freecash =~ /^[0-9]+(\.[0-9]+)?$/);
		
		die "Invalid interval unit value '$int_unit' in $line: $_\n" 
			unless(defined $int_unit && ($int_unit eq "month" || $int_unit eq "week"));
		
		die "Invalid interval length value '$int_length' in $line: $_\n" 
			unless(defined $int_length && $int_length =~ /^[0-9]+$/);

		# TODO: do insert here
		$sth_profile->execute($p_name, $prepaid, $int_charge, $int_freetime, $int_freecash,
			$int_unit, $int_length)
			or die "Error executing profile statement: $dbh->errstr\n";

		$profile_id = $dbh->last_insert_id(undef, undef, undef, undef)
			or FATAL "Error getting last insert id: $dbh->errstr\n";

		next;
	}

	# from here, we need to call "FATAL" instead of "die" to perform rollback in case of an error

	s/\'|\"|\s//g;
	my ($dst, $on, $off, $shot, $free) = split /\;/;
	FATAL "Invalid value in line $line: $_\n" 
		unless(defined $dst && defined $on && defined $off && defined $shot && defined $free);

	if($shot eq "yes" || $shot eq "ja" || $shot eq "true" || int($shot) == 1)
	{
		$shot = 1;
	}
	elsif($shot eq "no" || $shot eq "nein" || $shot eq "false" || int($shot) == 0)
	{
		$shot = 0;
	}
	else
	{
		FATAL "Invalid shot value in line $line: $shot\n";
	}

	if($free eq "yes" || $free eq "ja" || $free eq "true" || int($free) == 1)
	{
		$free = 1;
	}
	elsif($free eq "no" || $free eq "nein" || $free eq "false" || int($free) == 0)
	{
		$free = 0;
	}
	else
	{
		FATAL "Invalid freetime value in line $line: $free\n";
	}

	FATAL "Invalid on-peak value in line $line: $on\n"
		unless($on =~ /^[0-9]+(\.[0-9]+)?$/);
	FATAL "Invalid off-peak value in line $line: $off\n"
		unless($off =~ /^[0-9]+(\.[0-9]+)?$/);

	my @alldest = split /\,/, $dst;
	foreach my $d(@alldest)
	{
		FATAL "Invalid destination value in line $line: $dst\n"
			unless($d =~ /^[0-9]+$/ || $d =~ /^\@.+$/);
		my @dset = ();
		unless($shot)
		{
			# destination, type, 
			# on-init-rate, on-init-int, on-fol-rate, on-fol-int,
			# off-init-rate, off-init-int, off-fol-rate, off-fol-rate,
			# in-freetime
			@dset = (
				'^'.$d.'.+$', $type, 
				round($on*100/60), $init_interval, round($on*100/60), $follow_interval,
				round($off*100/60), $init_interval, round($off*100/60), $follow_interval,
				$free
			);

		}
		else
		{
			# destination, type, 
			# on-rate, 1, 0, 1,
			# off-rate, 1, 0, 1,
			# in-freetime
			@dset = (
				'^'.$d.'.+$', $type, 
				round($on*100/60), 1, 0, 1,
				round($off*100/60), 1, 0, 1,
				$free
			);
		}

		$sth_fee->execute($profile_id, 
			$dset[0], $dset[1], $dset[2],
			$dset[3], $dset[4], $dset[5],
			$dset[6], $dset[7], $dset[8],
			$dset[9])
			or FATAL "Error executing fee statement: ".$dbh->errstr;
		if($free == 1)
		{
			my $fee_id = $dbh->last_insert_id(undef, undef, undef, undef)
				or FATAL "Error getting last insert id of fee entry: ".$dbh->errstr;
			$sth_freetime->execute($profile_id, $fee_id)
				or FATAL "Error executing freetime statement: ".$dbh->errstr;
		}
	}
}
close FH;

$sth = $dbh->prepare("commit")
	or FATAL "Error preparing commit statement: $dbh->errstr\n";
$sth->execute
	or FATAL "Error committing session: $dbh->errstr\n";


$sth->finish;
$sth_rollback->finish;
$sth_fee->finish;
$dbh->disconnect;

