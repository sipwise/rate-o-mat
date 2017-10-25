
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

#use Time::Local qw(timegm timelocal);
use POSIX qw(mktime);
use Test::More;

### testcase outline:
### unit test for rateomat's add_interval sub
###
### this tests check the add_interval method in detail,
### in particular mktime's rollover with args like month=99

my $t1 = '2015-99-99 99:99:99';
my $t2 = from_epoch(to_epoch($t1));
is($t2,'2023-06-11 04:40:39');

$t1 = '2015-10-13 00:00:00';
$t2 = from_epoch(add_interval('day',30,to_epoch($t1),undef,undef));
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
