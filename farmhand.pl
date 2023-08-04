#!/usr/bin/perl

use strict;
use warnings;

use lib "$ENV{HOME}/FarmHand/lib/";
use FarmHand;

my $mpi = FarmHand->new(debug_level     => 3,
			controller_node => 0,
		     );

my $handler = sub {my $data = shift; return ++$data};

if ($mpi->is_controller) {

	my @data = qw( 1 2 3 4 5 6 7 8 9 10);

	for my $datum (@data) {

		my ($results, $error) = $mpi->run_controller($datum);

		print STDERR "farmhand.pl $results\t$error\n";

		print "$results\n";

	}

	while (my $node = $mpi->next_node) {

		my ($results, $error) = $mpi->cleanup_node($node);
		print "$results\n";
	}
}
else {

	$mpi->run_worker($handler);
}

