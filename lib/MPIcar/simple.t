#!/usr/bin/perl
use strict;

use Test::More 'no_plan'; # tests => 10;

use lib '/home/bmoore/Projects/08-10-20_MPI_Module/';
use MPIcar::Simple;

#BEGIN {
#	use lib '..';
#	#TEST 1
#	use_ok('MPIcar::Simple');
#}

my $path = $0;
$path =~ s/[^\/]+$//;
$path ||= '.';
chdir($path);

my $worker_handler  = sub {$_++};
my $results_handler = sub {print "$_\n"};
my $error_handler   = sub {print "$_\n"};
my $data = [qw( 1 2 3 4 5 6 7 8 9 10)];


my $mpi = MPIcar::Simple->new();

# Test 2
isa_ok($mpi, 'MPIcar::Simple');

#-------------------------------------------------------------------------------

# Test 3
my @methods = qw(run run_controller run_worker activate_node run_handler
		 run_results_handler run_error_handler worker_handler
		 results_handler error_handler communicator data_available
		 debug debug_level get_error get_results get_worker_node
		 message_length mpi_data mpi_error mpi_result node_is_active
		 rank recieve_data request_line requesting_data there_is_data
		 mpi_recieve mpi_send return_results controller_node
		 shutdown_node size throw
		);
can_ok($mpi, @methods);

# Test 4
$mpi->data($data);
ok(ref $mpi->{data} eq 'ARRAY', "Test data");

# Test 5
$mpi->worker_handler($worker_handler);
ok(ref $mpi->worker_handler eq 'CODE', "Test worker_handler");

# Test 6
$mpi->results_handler($results_handler);
ok(ref $mpi->results_handler eq 'CODE', "Test results_handler");

# Test 7
$mpi->error_handler($error_handler);
ok(ref $mpi->error_handler eq 'CODE', "Test error_handler");


=head2

run
run_controller
run_worker
activate_node
run_handler
run_results_handler
run_error_handler
worker_handler
results_handler
error_handler
communicator
data_available
debug
debug_level
get_error
get_results
get_worker_node
message_length
mpi_data
mpi_error
mpi_result
node_is_active
rank
recieve_data
request_line
requesting_data
there_is_data
mpi_recieve
mpi_send
return_results
controller_node
shutdown_node
size
throw

=cut

=head3
# Various other ways to say "ok"
ok($this eq $that, $test_name);

is  ($this, $that,    $test_name);
isnt($this, $that,    $test_name);

# Rather than print STDERR "# here's what went wrong\n"
diag("here's what went wrong");

like  ($this, qr/that/, $test_name);
unlike($this, qr/that/, $test_name);

cmp_ok($this, '==', $that, $test_name);

is_deeply($complex_structure1, $complex_structure2, $test_name);

can_ok($module, @methods);
isa_ok($object, $class);

pass($test_name);
fail($test_name);

BAIL_OUT($why);
=cut
