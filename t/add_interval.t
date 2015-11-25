
use strict;
use Time::Local qw(timegm timelocal);
use POSIX qw(mktime);

use Test::More;

my $t1 = '2015-10-13 00:00:00';
my $t2 = from_epoch(add_interval('day',30,to_epoch($t1),undef,undef));
is($t2,'2015-11-12 00:00:00');

$t1 = '2015-03-29 00:00:00';
$t2 = from_epoch(add_interval('day',1,to_epoch($t1),undef,undef));
is($t2,'2015-03-30 00:00:00');

done_testing();
exit;

sub to_epoch {
    my $date = shift;
    if ($date =~ /([0-9]{1,4})-([0-9]{1,2})-([0-9]{1,2}) ([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})/) {
        return mktime($6,$5,$4,$3,$2-1,$1-1900);
    }
    return undef;
}

sub from_epoch {
 
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(shift);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec;
    
}

sub align_dst {
	my $from_time = shift;
	my $to_time = shift;
	my @from = localtime($from_time);
    my $from_is_dst = pop @from;
	my @to = localtime($to_time);
    my $to_is_dst = pop @to;
	if ($from_is_dst != $to_is_dst) {
		# if the balance interval spans over a winter->summer or summer->winter DST transition,
		# e.g. a 30day interval will result as something like
		#  2015-10-13 00:00:00-2015-11-11 22:59:59
		# but we want DateTime's behaviour for day-based intervals, to get
		#  2015-10-13 00:00:00-2015-11-11 23:59:59
		# instead.
		# see http://search.cpan.org/~drolsky/DateTime-1.21/lib/DateTime.pm#Adding_a_Duration_to_a_Datetime
        
        #calculate the DST offset in seconds:
        my $gmt_offset_from = timegm(@from) - timelocal(@from);
        my $gmt_offset_to = timegm(@to) - timelocal(@to);
        my $dst_offset = $gmt_offset_from - $gmt_offset_to;
        
        $to_time += $dst_offset;
	}
	return $to_time;
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
		$to_time = $from_time + 7*24*60*60 * $count;
		$to_time = align_dst($from_time,$to_time);
	} elsif($unit eq "month") {
		my ($from_year,$from_month,$from_day,$from_hour,$from_minute,$from_second) = (localtime($from_time))[5,4,3,2,1,0];
		$from_month += $count;
		while ($from_month >= 12) {
			$from_month -= 12;
			$from_year++;
		}
		$to_time = mktime($from_second,$from_minute,$from_hour,$from_day,$from_month,$from_year);
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