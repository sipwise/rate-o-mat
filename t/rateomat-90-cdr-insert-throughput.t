
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### test performance of various cdr bulk insert techniques

my $init_secs = 60;
my $follow_secs = 30;

my $number_of_callers = 10;
my $number_of_callees = 10;
my $number_of_cdrs = 20000;
my @call_durations = ( 3, 5, 10, 30, 60, 120, 200, 600 );

my $date = Utils::Api::get_now()->ymd();
my $t = Utils::Api::datetime_from_string($date . ' 07:30:00');
Utils::Api::set_time($t);
my $provider = create_provider('test.com');
my $profile = $provider->{subscriber_fees}->[0]->{profile};
my $balance = 0;
my @callers = map { Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 888, ac => $_.'<n>', sn => '<t>' }); } (1..$number_of_callers);
my @callees = map { Utils::Api::setup_subscriber($provider,$profile,$balance,{ cc => 989, ac => $_.'<n>', sn => '<t>' }); } (1..$number_of_callees);

foreach my $dk (0..1) {
    foreach my $bulk (0..1) {
        foreach my $lid (0..1) {
            my $label = "inserting $number_of_cdrs rows: commit per " . ($bulk ? 'block' : 'row (auto-commit)') . ($dk ? ' + disable keys' : '') . ($lid ? ' + last_insert_id' : '');
            #diag($label);
            my ($cols,$vals) = create_test_set();
            my $dbh;
            eval {
                $dbh = Utils::Rateomat::_connect_accounting_db();
                #$dbh->{AutoCommit} = ($bulk ? 0 : 1);
                my $sth = get_sth($dbh,'cdr',$cols);
                my $id;
                my $t1 = time();
                $dbh->do("ALTER TABLE cdr DISABLE KEYS") if $dk;
                Utils::Rateomat::_begin_transaction() if $bulk;
                foreach my $row (@$vals) {
                    $sth->execute(@$row);
                    $id = $dbh->last_insert_id('accounting','accounting','cdr','id') if $lid;
                }
                Utils::Rateomat::_commit_transaction() if $bulk;
                $dbh->do("ALTER TABLE cdr ENABLE KEYS") if $dk;
                diag($label . ' takes ' . (time() - $t1) . ' secs');
                $sth->finish();
            };
            if ($@) {
                diag($@);
                eval { Utils::Rateomat::_rollback_transaction($dbh); };
                eval { $dbh->do("ALTER TABLE cdr ENABLE KEYS") if $dk; };
            }
            Utils::Rateomat::_disconnect_db($dbh);
        }
    }

    {
        my $label = "inserting $number_of_cdrs rows: grouped insert" . ($dk ? ' + disable keys' : '');
        #diag($label);
        my ($cols,$vals) = create_test_set();
        $vals = [ map { @{$_}; } @$vals ];
        my $dbh;
        eval {
            $dbh = Utils::Rateomat::_connect_accounting_db();
            #$dbh->{AutoCommit} = ($bulk ? 0 : 1);
            my $sth = get_sth($dbh,'cdr',$cols,$number_of_cdrs);
            my $id;
            my $t1 = time();
            $dbh->do("ALTER TABLE cdr DISABLE KEYS") if $dk;
            $sth->execute(@$vals);
            $dbh->do("ALTER TABLE cdr ENABLE KEYS") if $dk;
            diag($label . ' takes ' . (time() - $t1) . ' secs');
            $sth->finish();
        };
        if ($@) {
            diag($@);
            eval { Utils::Rateomat::_rollback_transaction($dbh); };
            eval { $dbh->do("ALTER TABLE cdr ENABLE KEYS") if $dk; };
        }
        Utils::Rateomat::_disconnect_db($dbh);
    }
}

done_testing();
exit;

sub get_sth {
    my ($dbh,$table,$keys,$groups) = @_;
    $groups //= 1;
    my $tuple = ',(' . substr(',?' x scalar @$keys,1) . ')';
    return $dbh->prepare('INSERT INTO ' . $table . ' (' .
			join(',', @$keys) .
			') VALUES ' . substr($tuple x $groups,1));
}

sub create_test_set {
    my @cdr_test_data = ();
    my @keys;
    foreach my $i (1..$number_of_cdrs) {
        my $caller = $callers[int(rand $number_of_callers)];
        my $callee = $callees[int(rand $number_of_callees)];
        my %cdr = %{ Utils::Rateomat::prepare_cdr($caller->{subscriber},undef,$caller->{reseller},
			$callee->{subscriber},undef,$callee->{reseller},
			'192.168.0.1',$t->epoch + $i,$call_durations[int(rand scalar @call_durations )]) };
        @keys = keys %cdr unless scalar @keys;
        push(@cdr_test_data,[ @cdr{@keys} ]);
    }
    #diag("test set created");
    return (\@keys,\@cdr_test_data);
}

sub create_provider {
    my $domain = shift;
    return Utils::Api::setup_provider($domain,
        [ #subscriber rates:
            { prepaid => 0,
              fees => [{ #outgoing:
                direction => 'out',
                destination => '^8882.+',
                onpeak_init_rate        => 6,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 6,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            },
            { #incoming:
                direction => 'in',
                destination => '.',
                source => '^8881.+',
                onpeak_init_rate        => 5,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 5,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            }]},
            { prepaid => 1,
              fees => [{ #outgoing:
                direction => 'out',
                destination => '^8882.+',
                onpeak_init_rate        => 4,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 4,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            },
            { #incoming:
                direction => 'in',
                destination => '.',
                source => '^8881.+',
                onpeak_init_rate        => 3,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 3,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            }]},
        ],
        undef, # no billing networks in this test suite
        # provider rate:
        { prepaid => 0,
              fees => [{ #outgoing:
                direction => 'out',
                destination => '^888.+',
                onpeak_init_rate        => 2,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 2,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            },
            { #incoming:
                direction => 'in',
                destination => '.',
                source => '^888.+',
                onpeak_init_rate        => 1,
                onpeak_init_interval    => $init_secs,
                onpeak_follow_rate      => 1,
                onpeak_follow_interval  => $follow_secs,
                offpeak_init_rate        => 1,
                offpeak_init_interval    => $init_secs,
                offpeak_follow_rate      => 1,
                offpeak_follow_interval  => $follow_secs,
            }]},
    );
}