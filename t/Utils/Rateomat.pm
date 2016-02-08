package Utils::Rateomat;

use threads 'exit' => 'threads_only';

use strict;
use DBI;
use Test::More;
#use IPC::System::Simple qw(capturex);
use Data::Dumper;
use Time::HiRes qw();


require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
	run_rateomat
	run_rateomat_threads
	create_cdrs
	get_cdrs
	get_cdrs_by_call_id
	prepare_cdr
	check_cdr
	check_cdrs
	delete_cdrs
	create_prepaid_costs_cdrs
	get_prepaid_costs_cdrs
	prepare_prepaid_costs_cdr
	check_prepaid_costs_cdr
	check_prepaid_costs_cdrs
	delete_prepaid_costs_cdrs
	get_usr_preferences
	generate_call_id
);

$ENV{RATEOMAT_BILLING_DB_USER} //= 'root';
$ENV{RATEOMAT_PROVISIONING_DB_USER} //= 'root';
$ENV{RATEOMAT_ACCOUNTING_DB_USER} //= 'root';

$ENV{RATEOMAT_DEBUG} //= 1;
$ENV{RATEOMAT_DAEMONIZE} = 0;
my $rateomat_timeout = 5;
my $rateomat_pl = $ENV{RATEOMAT_PL} // '../rate-o-mat.pl';

my $accountingdb_name = $ENV{RATEOMAT_ACCOUNTING_DB_NAME} // 'accounting';
my $accountingdb_host = $ENV{RATEOMAT_ACCOUNTING_DB_HOST} // 'localhost';
my $accountingdb_port = $ENV{RATEOMAT_ACCOUNTING_DB_PORT} // '3306';
my $accountingdb_user = $ENV{RATEOMAT_ACCOUNTING_DB_USER} or die('Missing accounting DB user setting.');
my $accountingdb_pass = $ENV{RATEOMAT_ACCOUNTING_DB_PASS};

my $provisioningdb_name = $ENV{RATEOMAT_PROVISIONING_DB_NAME} // 'accounting';
my $provisioningdb_host = $ENV{RATEOMAT_PROVISIONING_DB_HOST} // 'localhost';
my $provisioningdb_port = $ENV{RATEOMAT_PROVISIONING_DB_PORT} // '3306';
my $provisioningdb_user = $ENV{RATEOMAT_PROVISIONING_DB_USER} or die('Missing provisioning DB user setting.');
my $provisioningdb_pass = $ENV{RATEOMAT_PROVISIONING_DB_PASS};

my %cdr_map = ();
my %prepaid_costs_cdr_map = ();

END {
	my @ids = keys %cdr_map;
	if ((scalar @ids) > 0) {
		my $deleted_cdrs = delete_cdrs(\@ids);
		diag('teardown - cdr IDs ' . join(', ',map { $_->{id}; } @$deleted_cdrs) . ' deleted');
	}
	@ids = keys %prepaid_costs_cdr_map;
	if ((scalar @ids) > 0) {
		my $deleted_prepaid_costs = delete_prepaid_costs_cdrs(\@ids);
		diag('teardown - cdr IDs ' . join(', ',map { $_->{cdr}->{id}; } @$deleted_prepaid_costs) . ' and prepaid costs records deleted');
	}
}

sub run_rateomat {
	my $timeout = shift;
	$timeout //= $rateomat_timeout;
	diag('running rate-o-mat at ' . $rateomat_pl . ($timeout ? ' for '.$timeout.' secs' : ''));
	eval {
		local $SIG{ALRM} = sub {
			my $sig = 'HUP';
			diag("timeout for rate-o-mat, sending $sig signal to active process");
			kill 'HUP', 0;
		} if defined $timeout;
		alarm $timeout if defined $timeout;
		die($!) if !defined do $rateomat_pl;
		alarm 0 if defined $timeout;
		return 1;
	};
	alarm 0 if defined $timeout;
	if ($@ && $@ !~ /^interrupted system call/i) {
		diag($@);
		return 0;
	}
	return 1;
}

sub run_rateomat_threads {
	my ($num_of_threads,$timeout) = @_;
	$timeout //= $rateomat_timeout;
	$num_of_threads //= 1;
	my @workers = ();
	foreach my $i (0..$num_of_threads - 1) {
		push(@workers,threads->create(sub {
				my $tid = threads->tid();
				diag("running rate-o-mat at $rateomat_pl as thread $tid" . ($timeout ? ' for '.$timeout.' secs' : ''));
				local $SIG{KILL} = sub {
						diag("timeout for rate-o-mat thread $tid");
						threads->exit();
					} if defined $timeout;
				eval {
					die($!) if !defined do $rateomat_pl;
					return 1;
				};
				if ($@) {
					diag("rate-o-mat thread $tid: " . $@);
					return 0;
				}
				return 1;
			}));
	}
	my @threads = @workers;
	push(@threads,threads->create(sub {
		my $tid = threads->tid();
		#diag("timeout thread $tid started");
		#for (my $i = $timeout; $i > 0; $i--) {
		#	sleep(1);
		#	diag("timeout thread $tid: $i secs left");
		#}
		sleep($timeout);
		foreach my $w (@workers) {
			$w->kill('KILL');
			#diag('rate-o-mat thread ' . $w->tid() . ' killed');
		}
		return 1;
	})) if defined $timeout;
	my $result = 1;
	foreach my $t (@threads) {
		my $t_result = $t->join();
		$result &= (defined $t_result ? $t_result : 1);
		#diag('thread ' . $t->tid() . ' joined');
	}
	return $result;
}

sub _get_cli {
	my $subscriber = shift;
	my $pn = $subscriber->{primary_number};
	if ($pn) {
		return $pn->{cc} . $pn->{ac} . $pn->{sn};
	}
	return $subscriber->{username};
}

sub _random_string {
	my ($length,@chars) = @_;
	return join('',@chars[ map{ rand @chars } 1 .. $length ]);
}

sub prepare_cdr {
	my ($source_subscriber,
		$source_peering_subsriber_info,
		$source_reseller,
		$dest_subscriber,
		$dest_peering_subsriber_info,
		$dest_reseller,
		$source_ip,
		$time,
		$duration,
		@overrides) = @_;
	return {
		#id                                     => ,
		#update_time                            => ,
		source_user_id                          => ($source_subscriber ? $source_subscriber->{uuid} : '0'),
		source_provider_id                      => $source_reseller->{contract_id},
		#source_external_subscriber_id          => ,
		#source_external_contract_id            => ,
		source_account_id                       => ($source_subscriber ? $source_subscriber->{customer_id} : '0'),
		source_user                             => ($source_subscriber ? $source_subscriber->{username} : $source_peering_subsriber_info->{username}),
		source_domain                           => ($source_subscriber ? $source_subscriber->{domain} : $source_peering_subsriber_info->{domain}),
		source_cli                              => ($source_subscriber ? _get_cli($source_subscriber) : $source_peering_subsriber_info->{username}),
		#source_clir                            => '0',
		source_ip                               => $source_ip,
		#source_gpp0                            => ,
		#source_gpp1                            => ,
		#source_gpp2                            => ,
		#source_gpp3                            => ,
		#source_gpp4                            => ,
		#source_gpp5                            => ,
		#source_gpp6                            => ,
		#source_gpp7                            => ,
		#source_gpp8                            => ,
		#source_gpp9                            => ,
		destination_user_id                     => ($dest_subscriber ? $dest_subscriber->{uuid} : '0'),
		destination_provider_id                 => $dest_reseller->{contract_id},
		#destination_external_subscriber_id     => ,
		#destination_external_contract_id       => ,
		destination_account_id                  => ($dest_subscriber ? $dest_subscriber->{customer_id} : '0'),
		destination_user                        => ($dest_subscriber ? $dest_subscriber->{username} : $dest_peering_subsriber_info->{username}),
		destination_domain                      => ($dest_subscriber ? $dest_subscriber->{domain} : $source_peering_subsriber_info->{domain}),
		destination_user_dialed                 => ($dest_subscriber ? $dest_subscriber->{username} : $source_peering_subsriber_info->{username}),
		destination_user_in                     => ($dest_subscriber ? $dest_subscriber->{username} : $source_peering_subsriber_info->{username}),
		destination_domain_in                   => ($dest_subscriber ? $dest_subscriber->{domain} : $source_peering_subsriber_info->{domain}),
		#destination_gpp0                       => ,
		#destination_gpp1                       => ,
		#destination_gpp2                       => ,
		#destination_gpp3                       => ,
		#destination_gpp4                       => ,
		#destination_gpp5                       => ,
		#destination_gpp6                       => ,
		#destination_gpp7                       => ,
		#destination_gpp8                       => ,
		#destination_gpp9                       => ,
		#peer_auth_user                         => ,
		#peer_auth_realm                        => ,
		call_type                               => 'call',
		call_status                             => 'ok',
		call_code                               => '200',
		init_time                               => $time,
		start_time                              => $time,
		duration                                => $duration,
		call_id                                 => generate_call_id(),
		#source_carrier_cost                    => ,
		#source_reseller_cost                   => ,
		#source_customer_cost                   => ,
		#source_carrier_free_time               => ,
		#source_reseller_free_time              => ,
		#source_customer_free_time              => ,
		#source_carrier_billing_fee_id          => ,
		#source_reseller_billing_fee_id         => ,
		#source_customer_billing_fee_id         => ,
		#source_carrier_billing_zone_id         => ,
		#source_reseller_billing_zone_id        => ,
		#source_customer_billing_zone_id        => ,
		#destination_carrier_cost               => ,
		#destination_reseller_cost              => ,
		#destination_customer_cost              => ,
		#destination_carrier_free_time          => ,
		#destination_reseller_free_time         => ,
		#destination_customer_free_time         => ,
		#destination_carrier_billing_fee_id     => ,
		#destination_reseller_billing_fee_id    => ,
		#destination_customer_billing_fee_id    => ,
		#destination_carrier_billing_zone_id    => ,
		#destination_reseller_billing_zone_id   => ,
		#destination_customer_billing_zone_id   => ,
		#frag_carrier_onpeak                    => ,
		#frag_reseller_onpeak                   => ,
		#frag_customer_onpeak                   => ,
		#is_fragmented                          => ,
		#split                                  => ,
		#rated_at                               => ,
		#rating_status                          => 'unrated',
		#exported_at                            => ,
		#export_status                          => ,
		@overrides
	};
}

sub prepare_prepaid_costs_cdr {

	my ($source_subscriber,
		$source_peering_subsriber_info,
		$source_reseller,
		$dest_subscriber,
		$dest_peering_subsriber_info,
		$dest_reseller,
		$source_ip,
		$time,
		$duration,
		$prepaid_cost,
		$prepaid_free_time_used,
		@overrides) = @_;
	my $cdr = prepare_cdr($source_subscriber,
		$source_peering_subsriber_info,
		$source_reseller,
		$dest_subscriber,
		$dest_peering_subsriber_info,
		$dest_reseller,
		$source_ip,
		$time,
		$duration,
		@overrides);
	my ($S, $M, $H, $d, $m, $Y) = localtime($time);
	return { 	cdr => $cdr,
				prepaid_costs => {
					#id =>
					call_id => $cdr->{call_id},
					cost => $prepaid_cost,
					free_time_used => $prepaid_free_time_used // 0,
					timestamp => sprintf("%04d-%02d-%02d %02d:%02d:%02d", $Y + 1900,$m + 1, $d, $H, $M, $S),
				},
		};
}

sub generate_call_id {
	return '*TEST*'._random_string(26,'a'..'z','A'..'Z',0..9,'-','.');
}

sub check_cdrs {
	my ($label,%expected_map) = @_;
	my $ok = 1;
	foreach my $id (keys %expected_map) {
		$ok = check_cdr($label,$id,$expected_map{$id}) && $ok;
	}
	return $ok;
}

sub check_cdr {

	my ($label,$id,$expected_cdr) = @_;

	my $got_cdr = get_cdrs($id);
	my $ok = 1;
	foreach my $field (keys %$expected_cdr) {
		$ok = is($got_cdr->{$field},$expected_cdr->{$field},$label . $field . ' = ' . $expected_cdr->{$field}) && $ok;
	}
	diag(Dumper({result_cdr => $got_cdr})) if !$ok;
	return $ok;

}

sub check_prepaid_costs_cdrs {
	my ($label,$prepaid_costs_exists,%expected_map) = @_;
	my $ok = 1;
	foreach my $id (keys %expected_map) {
		$ok = check_prepaid_costs_cdr($label,$id,$expected_map{$id},$prepaid_costs_exists) && $ok;
	}
	return $ok;
}

sub check_prepaid_costs_cdr {

	my ($label,$id,$expected_cdr,$prepaid_costs_exists) = @_;

	my $got_prepaid_costs_cdr = get_prepaid_costs_cdrs($id);
	my $ok = 1;
	foreach my $field (keys %$expected_cdr) {
		$ok = is($got_prepaid_costs_cdr->{cdr}->{$field},$expected_cdr->{$field},$label . $field . ' = ' . $expected_cdr->{$field}) && $ok;
	}
	$ok = is(defined $got_prepaid_costs_cdr->{prepaid_costs} ? '1' : '0',$prepaid_costs_exists,$label . 'prepaid cost record ' . ($prepaid_costs_exists ? 'exists' : 'does not exist')) && $ok;
	diag(Dumper({result_cdr => $got_prepaid_costs_cdr})) if !$ok;
	return $ok;

}

sub create_cdrs {
	my ($cdrs) = @_;
	my $is_ary = 'ARRAY' eq ref $cdrs;
	my $result = ($is_ary ? [] : undef);
	my $dbh;
	eval {
		$dbh = _connect_accounting_db();
		if ($is_ary) {
			_begin_transaction($dbh);
			$result = _create_cdrs($dbh,$cdrs);
			_commit_transaction($dbh);
		} else {
			$result = _create_cdr($dbh,@_);
		}
	};
	if ($@) {
		diag($@);
		eval { _rollback_transaction($dbh); } if $is_ary;
	}
	_disconnect_db($dbh);
	return $result;
}

sub create_prepaid_costs_cdrs {
	my ($cdrs) = @_;
	my $is_ary = 'ARRAY' eq ref $cdrs;
	my $result = ($is_ary ? [] : undef);
	my $dbh;
	eval {
		$dbh = _connect_accounting_db();
		if ($is_ary) {
			_begin_transaction($dbh);
			$result = _create_prepaid_costs_cdrs($dbh,$cdrs);
			_commit_transaction($dbh);
		} else {
			$result = _create_prepaid_costs_cdr($dbh,@_);
		}
	};
	if ($@) {
		diag($@);
		eval { _rollback_transaction($dbh); } if $is_ary;
	}
	_disconnect_db($dbh);
	return $result;
}

sub _create_cdr {
	my ($dbh,%values) = @_;
	if ($dbh) {
		my $id = _insert($dbh,'accounting.cdr',\%values);
		return _get_cdr($dbh,$id,1) if ok($id,'cdr id '.$id.' created');;
	}
	return undef;
}

sub _create_prepaid_costs_cdr {
	my ($dbh,$values) = @_;
	if ($dbh) {
		if (_insert($dbh,'accounting.prepaid_costs',$values->{prepaid_costs})) {
			my $id = _insert($dbh,'accounting.cdr',$values->{cdr});
			return _get_prepaid_costs_cdr($dbh,$id,1) if ok($id,'cdr id '.$id.' with prepaid costs record created');
		} else {
			fail('creating prepaid costs for call_id '.$values->{prepaid_costs}->{call_id}.' record');
		}
	}
	return { prepaid_costs => undef, cdr => undef };
}

sub _create_cdrs {
	my ($dbh,$cdrs) = @_;
	if ($dbh) {
		my @ids = ();
		foreach my $values (@$cdrs) {
			my $id = _insert($dbh,'accounting.cdr',$values);
			push(@ids,$id) if $id;
		}
		return _get_cdrs($dbh,\@ids,1) if ok((scalar @ids) == (scalar @$cdrs),'cdrs id '.join(', ',@ids).' created');
	};
	return [];
}

sub _create_prepaid_costs_cdrs {
	my ($dbh,$prepaid_costs_cdrs) = @_;
	if ($dbh) {
		my @ids = ();
		foreach my $values (@$prepaid_costs_cdrs) {
			if (_insert($dbh,'accounting.prepaid_costs',$values->{prepaid_costs})) {
				my $id = _insert($dbh,'accounting.cdr',$values->{cdr});
				push(@ids,$id) if $id;
			} else {
				fail('creating prepaid costs for call_id '.$values->{prepaid_costs}->{call_id}.' record');
			}
		}
		return _get_prepaid_costs_cdrs($dbh,\@ids,1) if ok((scalar @ids) == (scalar @$prepaid_costs_cdrs),'cdrs id '.join(', ',@ids).' with prepaid costs records created');
	};
	return [];
}

sub get_cdrs {
	my $ids = shift;
	my $is_ary = 'ARRAY' eq ref $ids;
	my $result = ($is_ary ? [] : undef);
	eval {
		my $dbh = _connect_accounting_db();
		$result = _get_cdrs($dbh,$ids) if $is_ary;
		$result = _get_cdr($dbh,$ids) if !$is_ary;
		_disconnect_db($dbh);
	};
	if ($@) {
		diag($@);
	}
	return $result;
}

sub get_cdrs_by_call_id {
	my $call_id = shift;
	my $result = [];
	eval {
		my $dbh = _connect_accounting_db();
		$result = _get_cdrs_by_callid($dbh,$call_id,1);
		_disconnect_db($dbh);
	};
	if ($@) {
		diag($@);
	}
	return $result;
}

sub delete_cdrs {
	my $ids = shift;
	my $is_ary = 'ARRAY' eq ref $ids;
	my $result = ($is_ary ? [] : undef);
	my $dbh;
	eval {
		$dbh = _connect_accounting_db();
		_begin_transaction($dbh);
		$result = _delete_cdrs($dbh,$ids) if $is_ary;
		$result = _delete_cdrs($dbh,[ $ids ])->[0] if !$is_ary;
		_commit_transaction($dbh);
	};
	if ($@) {
		diag($@);
		eval { _rollback_transaction($dbh); };
	}
	_disconnect_db($dbh);
	return $result;
}

sub delete_prepaid_costs_cdrs {
	my $ids = shift;
	my $is_ary = 'ARRAY' eq ref $ids;
	my $result = ($is_ary ? [] : undef);
	my $dbh;
	eval {
		$dbh = _connect_accounting_db();
		_begin_transaction($dbh);
		$result = _delete_prepaid_costs_cdrs($dbh,$ids) if $is_ary;
		$result = _delete_prepaid_costs_cdrs($dbh,[ $ids ])->[0] if !$is_ary;
		_commit_transaction($dbh);
	};
	if ($@) {
		diag($@);
		eval { _rollback_transaction($dbh); };
	}
	_disconnect_db($dbh);
	return $result;
}

sub _get_cdr {
	my ($dbh,$id,$created) = @_;
	my $cdr = undef;
	if ($dbh) {
		my $sth = $dbh->prepare('SELECT * FROM accounting.cdr WHERE id = ?')
				or die("Error preparing select cdr statement: ".$dbh->errstr);
		$sth->execute($id);
		$cdr = $sth->fetchrow_hashref();
		$cdr_map{$id} = $cdr if $cdr && $created;
		$sth->finish();
	}
	return $cdr;
}

sub _get_prepaid_costs_cdr {
	my ($dbh,$id,$created) = @_;
	my $prepaid_costs_cdr = undef;
	if ($dbh) {
		my $sth = $dbh->prepare('SELECT * FROM accounting.cdr WHERE id = ?')
				or die("Error preparing select cdr statement: ".$dbh->errstr);
		$sth->execute($id);
		my $cdr = $sth->fetchrow_hashref();
		$sth->finish();
		if ($cdr) {
			$sth = $dbh->prepare('SELECT * FROM accounting.prepaid_costs WHERE call_id = ?')
					or die("Error preparing select prepaid_costs statement: ".$dbh->errstr);
			$sth->execute($cdr->{call_id});
			my $prepaid_costs = $sth->fetchrow_hashref();
			$prepaid_costs_cdr = { cdr => $cdr, prepaid_costs => $prepaid_costs };
			$prepaid_costs_cdr_map{$cdr->{id}} = $prepaid_costs_cdr if $created;
			$sth->finish();
		}
	}
	return $prepaid_costs_cdr;
}

sub _get_cdrs {
	my ($dbh,$ids,$created) = @_;
	my $cdrs = [];
	if ($dbh) {
		my $sth = $dbh->prepare('SELECT * FROM accounting.cdr WHERE id IN ('.substr(',?' x scalar @$ids,1).') ORDER BY id')
				or die("Error preparing select cdrs statement: ".$dbh->errstr);
		$sth->execute(@$ids);
		$cdrs = $sth->fetchall_arrayref({});
		$sth->finish();
		if ($created) {
			foreach my $cdr (@$cdrs) {
				$cdr_map{$cdr->{id}} = $cdr;
			}
		}
	}
	return $cdrs;
}

sub _get_prepaid_costs_cdrs {
	my ($dbh,$ids,$created) = @_;
	my @result = ();
	if ($dbh) {
		my $sth = $dbh->prepare('SELECT * FROM accounting.cdr WHERE id IN ('.substr(',?' x scalar @$ids,1).') ORDER BY id')
				or die("Error preparing select cdrs statement: ".$dbh->errstr);
		$sth->execute(@$ids);
		my $cdrs = $sth->fetchall_arrayref({});
		$sth->finish();
		my $prepaid_costs;
		my $prepaid_costs_cdr;
		foreach my $cdr (@$cdrs) {
			$sth = $dbh->prepare('SELECT * FROM accounting.prepaid_costs WHERE call_id = ?')
				or die("Error preparing select prepaid_costs statement: ".$dbh->errstr);
			$sth->execute($cdr->{call_id});
			$prepaid_costs = $sth->fetchrow_hashref();
			$sth->finish();
			$prepaid_costs_cdr = { cdr => $cdr, prepaid_costs => $prepaid_costs };
			$prepaid_costs_cdr_map{$cdr->{id}} = $prepaid_costs_cdr if $created;
			push(@result,$prepaid_costs_cdr);
		}
	}
	return \@result;
}

sub _get_cdrs_by_callid {
	my ($dbh,$call_id,$created) = @_;
	my $cdrs = [];
	if ($dbh) {
		my $sth = $dbh->prepare('SELECT * FROM accounting.cdr WHERE call_id = ? ORDER BY id')
				or die("Error preparing select cdrs by call id statement: ".$dbh->errstr);
		$sth->execute($call_id);
		$cdrs = $sth->fetchall_arrayref({});
		$sth->finish();
		if ($created) {
			foreach my $cdr (@$cdrs) {
				$cdr_map{$cdr->{id}} = $cdr;
			}
		}
	}
	return $cdrs;
}

sub _insert {
	my ($dbh,$table,$values) = @_;
	my $id = undef;
	if ($dbh) {
		my $sth = $dbh->prepare('INSERT INTO ' . $table . ' (' .
			join(',', keys %$values) .
			') VALUES (' .
			substr(',?' x scalar keys %$values,1) .
			')'
		) or die("Error preparing insert statement: ".$dbh->errstr);
		$sth->execute(values %$values);
		$id = $dbh->{'mysql_insertid'};
		$sth->finish();
	}
	return $id;
}
sub _delete_cdrs {
	my ($dbh,$ids) = @_;
	my $deleted = [];
	if ($dbh) {
		$deleted = _get_cdrs($dbh,$ids);
		my $sth = $dbh->prepare('DELETE FROM accounting.cdr WHERE id IN ('.substr(',?' x scalar @$ids,1).')')
			or die("Error preparing delete cdrs statement: ".$dbh->errstr);
		$sth->execute(@$ids);
		$sth->finish();
		foreach my $id (@$ids) {
			delete $cdr_map{$id};
		}
	}
	return $deleted;
}

sub _delete_prepaid_costs_cdrs {
	my ($dbh,$ids) = @_;
	my $deleted = [];
	if ($dbh) {
		$deleted = _get_prepaid_costs_cdrs($dbh,$ids);
		my $sth = $dbh->prepare('DELETE cdr,prepaid_costs FROM '.
								'accounting.cdr AS cdr '.
								'LEFT JOIN accounting.prepaid_costs AS prepaid_costs ON cdr.call_id = prepaid_costs.call_id '.
								'WHERE cdr.id IN ('.substr(',?' x scalar @$ids,1).')')
			or die("Error preparing delete cdrs with prepaid costs statement: ".$dbh->errstr);
		$sth->execute(@$ids);
		$sth->finish();
		foreach my $id (@$ids) {
			delete $prepaid_costs_cdr_map{$id};
		}
	}
	return $deleted;
}

sub get_usr_preferences {
	my ($subscriber,$attribute) = @_;
	my $usr_prefs = [];
	my $dbh;
	eval {
		$dbh = _connect_provisioning_db();
		_begin_transaction($dbh);
		my $sth = $dbh->prepare('SELECT * FROM provisioning.voip_preferences WHERE attribute = ?')
				or die("Error preparing select voip preferences statement: ".$dbh->errstr);
		$sth->execute($attribute);
		my $pref = $sth->fetchrow_hashref();
		$sth->finish();
		if (defined $pref && $pref->{id}) {
			$sth = $dbh->prepare('SELECT id FROM provisioning.voip_subscribers WHERE uuid = ? LIMIT 1')
				or die("Error preparing select voip subscribers statement: ".$dbh->errstr);
			$sth->execute($subscriber->{uuid});
			my ($prov_subscriber_id) = $sth->fetchrow_array();
			$sth->finish();
			if ($prov_subscriber_id) {
				$sth = $dbh->prepare('SELECT * FROM provisioning.voip_usr_preferences WHERE subscriber_id = ? AND attribute_id = ?')
					or die("Error preparing select voip usr preferences statement: ".$dbh->errstr);
				$sth->execute($prov_subscriber_id,$pref->{id});
				$usr_prefs = $sth->fetchall_arrayref({});
				$sth->finish();
			} else {
				die("cannot find prov subscriber '$subscriber->{uuid}'");
			}
		} else {
			die("cannot find attribute '$attribute'");
		}
		_commit_transaction($dbh);
		_disconnect_db($dbh);
	};
	if ($@) {
		diag($@);
		eval { _rollback_transaction($dbh); };
	}
	_disconnect_db($dbh);
	return $usr_prefs;
}

sub _connect_accounting_db {
	my $dbh = DBI->connect("dbi:mysql:database=$accountingdb_name;host=$accountingdb_host;port=$accountingdb_port",
		$accountingdb_user, $accountingdb_pass,
		{AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	die("Error connecting to accounting db: ".$DBI::errstr."\n") unless defined($dbh);
	return $dbh;
}

sub _connect_provisioning_db {
	my $dbh = DBI->connect("dbi:mysql:database=$provisioningdb_name;host=$provisioningdb_host;port=$provisioningdb_port",
		$provisioningdb_user, $provisioningdb_pass,
		{AutoCommit => 1, mysql_auto_reconnect => 0, mysql_no_autocommit_cmd => 0, PrintError => 1, PrintWarn => 0});
	die("Error connecting to provisioning db: ".$DBI::errstr."\n") unless defined($dbh);
	return $dbh;
}

sub _disconnect_db {
	my $dbh = shift;
	if ($dbh) {
		$dbh->disconnect();
	}
}

sub _begin_transaction {
	my ($dbh,$isolation_level) = @_;
	if ($dbh) {
		if ($isolation_level) {
			$dbh->do('SET TRANSACTION ISOLATION LEVEL '.$isolation_level) or die("Error setting transaction isolation level: ".$dbh->errstr);
		}
		$dbh->begin_work or die("Error starting transaction: ".$dbh->errstr);
	}
}

sub _commit_transaction {
	my $dbh = shift;
	if ($dbh) {
		#capture result to force list context and prevent good old komodo perl5db.pl bug:
		my @wa = $dbh->commit or die("Error committing: ".$dbh->errstr);
	}
}

sub _rollback_transaction {
	my $dbh = shift;
	if ($dbh) {
		my @wa = $dbh->rollback or die("Error rolling back: ".$dbh->errstr);
	}
}

1;
