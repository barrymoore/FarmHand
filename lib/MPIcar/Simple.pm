package MPIcar::Simple;

use strict;
use warnings;

use lib "$ENV{HOME}/FarmHand/lib/";

use  MPIcar qw(:all);
use Carp;

use vars qw($VERSION);
$VERSION = '0.01';

=head1 NAME

MPIcar::Simple

=head1 VERSION

This document describes MPIcar::Simple version 0.01

=head1 SYNOPSIS

  use MPIcar::Simple;

  my $data = some_data # Data can be either an array reference or an iterator object
  my $iterator_method = 'next'; # The method to be called on $data if it is an object reference.

  my $chpc = MPIcar::Simple->new(data            => $data,
				 iterator_method => $iterator_method,
				 worker_handler  => \&worker_handler,
				 results_handler => \&results_handler,
				 error_handler   => \&error_handler);

  $chpc->run;

  sub worker_handler  { #Code to execute on each worker node }
  sub results_handler { #Code to execute by the controller node on the results returned from worker nodes }
  sub error_handler   { #Code to execute by the controller node on the error returned from worker nodes }

=head1 DESCRIPTION

A subclass of MPIcar that provides a perl interface to the MPI library
at the Center for High Performance Computing, University of Utah.
This module allows the user to define a data set as an array
reference, or an iterator object.  An instance of an MPIcar::Simple
class is created, and is informed about the data set, a callback
function (handler) to be called by worker nodes on each element of the
data set, and two call back functions (handlers) to be called by the controller
node on the result and error return values returned by the worker node.

An example from bioinformatics would be to pass in a file containing
multiple nucleotide sequences as the data set, pass in a
worker_handler to align each sequence against a nucleotide sequence
database using blast, and then to return the results and errors of
those alignments back to the controller to be appended to a collective
output file.

=head1 METHODS

=cut

#-----------------------------------------------------------------------------

=head2 new

     Title   : new
     Usage   : MPIcar::Simple->new();
     Function: Creates a MPIcar::Simple object;
     Returns : An MPIcar::Simple object.
     Args    : See the associated methods for more details.
	       controller_node, data_available, message_length, mpi_data,
	       mpi_error, mpi_result, requesting_data, data, iterator_method,
	       worker_handler, result_handler, error_handler

=cut

sub new {
	my ($class, %args) = @_;
	my $self = {};
	bless $self, $class;
	$self->_initialize_args(\%args);

	$self->debug("Started: $self->rank\n");

	return $self;
}

#-----------------------------------------------------------------------------

sub _initialize_args {
	my ($self, $args) = @_;

	my @data_types = keys %{$args};

	for my $type (@data_types) {
		$args->{$type} && $self->$type($args->{$type});
       }
}

#-----------------------------------------------------------------------------

=head2 run

 Title   : run
 Usage   : $self->run;
 Function: Run the worker callback function on all of the elements of @{$data}
 Returns : undef.
 Args    : An array reference, and a code reference

=cut

sub run {
	my $self = shift;

	PI_Init(); #initiate the MPI

	if ($self->rank == $self->controller_node){
		$self->run_controller;
	}
	else {
		$self->run_worker;
	}
}
#-----------------------------------------------------------------------------

=head2 run_controller

 Title   : run_controller
 Usage   : $self->run_controller($data);
 Function: Run the controller which will iterate over the elements of data,
	   and send the data to worker nodes.
 Returns : undef
 Args    : An array referenece

=cut

sub run_controller {
	my $self = shift;

	# Signal that there is work to be done.
	my $there_is_data = 1;

	my $data = $self->data;
	while (my $datum = ref $data eq 'ARRAY' ? shift @{$data} : $data->{$self->iterator_method}) {

		# Recieve message from any node that needs more data
		my $worker_node = $self->get_worker_node;

		# If this node has had work in the past then collect that work and print
		if ($self->node_is_active($worker_node)) {

			# Receive results and then errors
			my $results = $self->get_results($worker_node);
			my $error   = $self->get_error($worker_node);

			$self->run_results_handler($results);
			$self->run_error_handler($error);
		}

		# Keep track of which nodes have been sent work.
		$self->activate_node($worker_node);

		# Send data to worker node
		$self->mpi_send($worker_node, $datum);
	}

	#Collect and close down all nodes.
	for my $node (0 .. $self->size) {

		next if $node == $self->controller_node;

		my $worker_node = $self->get_worker_node;

		if ($self->node_is_active($worker_node)) {

			# Receive results and then errors
			my ($results, $error) = $self->get_results($worker_node);

			$self->run_results_handler($results);
			$self->run_error_handler($error);
		}

		# Send undef value for data_available flag to worker
		# signaling it to terminate.
		$self->shutdown_node;
	}
}
#-----------------------------------------------------------------------------
#=head2 run_worker
#
# Title   : run_worker
# Usage   : $self->run_worker($code_ref);
# Function: Run a worker node which will execute a callback function on each $data
#	   passed to it.
# Returns : undef
# Args    : A code referenece
#
#=cut

sub run_worker {
	my $self = shift;

	my $there_is_data;

	while (my $data = $self->recieve_data) {

		my ($results, $error) = $self->run_handler($data);
		$self->return_results($results, $error);

	}
}
#-----------------------------------------------------------------------------

=head2 activate_node

 Title   : activate_node
 Usage   : $self->activate_node($node);
 Function: Flag a worker node as active
 Returns : undef
 Args    : An integer representing a node;

=cut

sub activate_node {
	my ($self, $node) = @_;
	$self->{active_nodes}[$node]++;
	return undef;
}
#-----------------------------------------------------------------------------

=head2 run_worker_handler

 Title   : run_worker_handler
 Usage   : ($result, $error) = $self->run_worker_handler($data)
 Function: Run the worker callback function on $data
 Returns : Two scalar strings representing the results and the error from the
	   callback.
 Args    : A scalar containing a data to be operated on.  This can be a
	   reference to any perl data structure that can be serialized by
	   Storable.  Storable wont touch it at this point, but the data
	   has to be serialized to be passed to the worker.

=cut

sub run_handler {
	my ($self, $data) = @_;
	my $code = $self->worker_handler;
	my ($result, $error) = eval{&{$code}($data)};
	return ($result, $error);
}
#-----------------------------------------------------------------------------
=head2 run_results_handler

 Title   : run_results_handler
 Usage   : $self->run_results_handler($results)
 Function: Run the results callback function on $results
 Returns : undef
 Args    : A scalar containing the results recieved from the worker node.
	   This can be a reference to any perl data structure that can be
	   serialized by Storable.  Storable wont touch it at this point,
	   but the data has to be serialized to be passed from the worker.

=cut

sub run_results_handler {
	my ($self, $results) = @_;
	my $code = $self->results_handler;
	eval{&{$code}($results)};
	return undef;
}
#-----------------------------------------------------------------------------
=head2 run_error_handler

 Title   : run_error_handler
 Usage   : $self->run_error_handler($error)
 Function: Run the error callback function on $error
 Returns : undef
 Args    : A scalar containing the error recieved from the worker node.
	   This can be a reference to any perl data structure that can be
	   serialized by Storable.  Storable wont touch it at this point,
	   but the data has to be serialized to be passed from the worker.

=cut

sub run_error_handler {
	my ($self, $error) = @_;
	my $code = $self->error_handler;
	eval{&{$code}($error)};
	return undef;
}
#-----------------------------------------------------------------------------

=head2 worker_handler

 Title   : worker_handler
 Usage   : $code_reference = $self->worker_handler(\&worker_subroutine)
 Function: Get/set the callback function for the worker nodes.
 Returns : A code reference.
 Args    : A code reference.

=cut

sub worker_handler {
	my ($self, $code_ref) = @_;
	$self->throw("Not a code reference in worker_handler: $code_ref")
	  unless ref $code_ref eq 'CODE';;
	$self->{worker_handler} = $code_ref if $code_ref;
	return $self->{worker_handler};
}
#-----------------------------------------------------------------------------

=head2 results_handler

 Title   : results_handler
 Usage   : $code_reference = $self->handler(\&results_subroutine)
 Function: Get/set the results callback function for the controller node.
 Returns : A code reference.
 Args    : A code reference.

=cut

sub results_handler {
	my ($self, $code_ref) = @_;
	$self->throw("Not a code reference in results_handler: $code_ref")
	  unless ref $code_ref eq 'CODE';;
	$self->{results_handler} = $code_ref if $code_ref;
	return $self->{results_handler};
}
#-----------------------------------------------------------------------------

=head2 error_handler

 Title   : error_handler
 Usage   : $code_reference = $self->handler(\&error_subroutine)
 Function: Get/set the error callback function for the controller node.
 Returns : A code reference.
 Args    : A code reference.

=cut

sub error_handler {
	my ($self, $code_ref) = @_;
	$self->throw("Not a code reference in error_handler: $code_ref")
	  unless ref $code_ref eq 'CODE';;
	$self->{error_handler} = $code_ref if $code_ref;
	return $self->{error_handler};
}
#-----------------------------------------------------------------------------

=head2 communicator

 Title   : communicator
 Usage   : $self->communicator
 Function: Returns the MPI_COMM_WORLD communicator.
 Returns : Im not sure what kind of thing MPI_COMM_WORLD is in terms of perl
	   data structures.
 Args    : N/A

=cut

sub communicator {
	my $self = shift;

	$self->{communicator} ||= MPI_COMM_WORLD;

	return $self->{self_communicator};
}
#-----------------------------------------------------------------------------
=head2 data_available

 Title   : data_available
 Usage   : $self->data_available();
 Function: Get/set the data_available MPI flag to an integer value.
 Returns : An integer.
 Args    : An integer.

=cut

sub data_available {
	my ($self, $integer) = @_;
	$self->throw("MPI data_available flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{data_available} = $integer if defined $integer;
	$self->{data_available} ||= 3333;
	return $self->{data_available};
}
#-----------------------------------------------------------------------------
=head2 debug

 Title   : debug
 Usage   : $self->debug('Debugging Message');
 Function: Prints a debugging message if debugging is turned on.
 Returns : undef
 Args    : A string.

=cut

sub debug {
	my ($self, $message, $level) = @_;

	$level ||= 1;

	print STDERR $message if $level >= $self->debug_level;

	return undef;
}
#-----------------------------------------------------------------------------
=head2 debug_level

 Title   : debug_level
 Usage   : $level = $self->debug_level(1);
 Function: Get/Set the value of debug_level.
 Returns : An integer in the range of 0-3.
 Args    : An integer in the range of 0-3.

=cut

sub debug_level {
	my ($self, $level) = @_;
	$self->throw("Debug Level must be in the range 0-3: $level")
	  unless ($level >= 0 && $level <= 3);
	$self->{debug_level} = $level if defined $level;
	return $self->{debug_level};
}
#-----------------------------------------------------------------------------
=head2 get_error

 Title   : get_error
 Usage   : $error = $self->get_error($worker_node);
 Function: Get an error message from a given worker node.
 Returns : A string.
 Args    : An integer representing the worker node.

=cut

sub get_error {
	my ($self, $worker_node) = @_;

	my $error = $self->mpi_recieve($worker_node, $self->mpi_error);

	return $error;
}
#-----------------------------------------------------------------------------
=head2 get_results

 Title   : get_results
 Usage   : $results = $self->get_results();
 Function: Get the results string from a worker node.
 Returns : A string.
 Args    : An integer representing the worker node.

=cut

sub get_results {
	my ($self, $worker_node) = @_;

	my $results = $self->mpi_recieve($worker_node, $self->mpi_results);

	return $results;
}
#-----------------------------------------------------------------------------
=head2 get_worker_node

 Title   : get_worker_node
 Usage   : $worker_node = $self->get_worker_node;
 Function: Get get the worker node ID for this node.
 Returns : An integer.
 Args    : None

=cut

sub get_worker_node {
	my $self = shift;

	my $worker_node;
	MPI_Recv(\$worker_node, 1, MPI_INT, -2, $self->requesting_data, $self->communicator);
	return $worker_node;
}

#-----------------------------------------------------------------------------

=head2 message_length

 Title   : message_length
 Usage   : $message_length = $self->message_length($message_length);
 Function: Get/Set the integer value of the message_length flag.
 Returns : The integer value of the message_length flag.
 Args    : An integer value to set the message_length flag to.

=cut

sub message_length {
	my ($self, $integer) = @_;
	$self->throw("MPI message_length flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{message_length} = $integer if defined $integer;
	$self->{message_length} ||= 4444;
	return $self->{message_length};
}
#-----------------------------------------------------------------------------

=head2 mpi_data

 Title   : mpi_data
 Usage   : $mpi_data = $self->mpi_data($mpi_data);
 Function: Get/Set the integer value of the mpi_data flag.
 Returns : The integer value of the mpi_data flag.
 Args    : A interger value to set the mpi_data flag to.

=cut

sub mpi_data {
	my ($self, $integer) = @_;
	$self->throw("MPI mpi_data flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{mpi_data} = $integer if defined $integer;
	$self->{mpi_data} ||= 2222;
	return $self->{mpi_data};
}
#-----------------------------------------------------------------------------

=head2 mpi_error

 Title   : mpi_error
 Usage   : $mpi_error = $self->mpi_error($mpi_error_flag);
 Function: Get/Set the integer value of the mpi_error flag.
 Returns : The integer value of the mpi_error flag.
 Args    : An integer value to set the mpi_error flag to.

=cut

sub mpi_error {
	my ($self, $integer) = @_;
	$self->throw("MPI mpi_error flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{mpi_error} = $integer if defined $integer;
	$self->{mpi_error} ||= 6666;
	return $self->{mpi_error};
}
#-----------------------------------------------------------------------------

=head2 mpi_result

 Title   : mpi_result
 Usage   : $mpi_result = $self->mpi_result($mpi_result);
 Function: Get/Set the interger value of the mpi_result flag.
 Returns : The integer value of the mpi_result flag.
 Args    : An integer value to set the mpi_result flat to.

=cut

sub mpi_result {
	my ($self, $integer) = @_;
	$self->throw("MPI mpi_result flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{mpi_result} = $integer if defined $integer;
	$self->{mpi_result} ||= 55555;
	return $self->{mpi_result};
}
#-----------------------------------------------------------------------------

=head2 node_is_active

 Title   : node_is_active
 Usage   : while ($self->node_is_active) { print $self->rank . " is active\n";
 Function: Determine if the current node (as determined by $self->rank) has
	   been previously give a job.
 Returns : The value of foo.
 Args    : A value to set foo to.

=cut

sub node_is_active {
	my ($self, $value) = @_;
	$self->{foo} = $value if defined $value;
	return $self->{foo};
}
#-----------------------------------------------------------------------------

=head2 rank

 Title   : rank
 Usage   : $rank = $self->rank();
 Function: Gets the rank (node ID) for the current process.
 Returns : An integer.
 Args    : N/A

=cut

sub rank {
	my ($self) = @_;
	$self->{rank} ||= MPI_Comm_rank($self->communicator);
	return $self->{rank};
}
#-----------------------------------------------------------------------------

=head2 recieve_data

 Title   : recieve_data
 Usage   : while (my $data = $self->recieve_data) {print $data}
 Function: Get data from the controller.
 Returns : A variable containing some arbitrary perl data structure (no coderefs please).
 Args    : N/A

=cut

sub recieve_data {
	my $self = shift;

	my $data;

	# Request a line;
	$self->request_line($self->controller_node);

	# Is there work?
	if ($self->there_is_data) {

		$data = $self->mpi_recieve($self->controller_node, $self->mpi_data);
	}

	return $data;
}
#-----------------------------------------------------------------------------

=head2 request_line

 Title   : request_line
 Usage   : $self->request_line();
 Function: Request a communication line from MPI
 Returns : undef
 Args    : N/A

=cut

sub request_line {
	my ($self, $dest_node) = shift;

	MPI_Send($self->rank, 1, MPI_INT, $dest_node, $self->requesting_data, $self->communicator);

	return undef;
}
#-----------------------------------------------------------------------------

=head2 requesting_data

 Title   : requesting_data
 Usage   : $requesting_data_tag = $self->requesting_data($integer);
 Function: Gets/sets the integer value of the requesting data tag.
 Returns : An integer representing the requesting data tag.
 Args    : An integer representing the requesting data tag.

=cut

sub requesting_data {
	my ($self, $integer) = @_;
	$self->throw("MPI requesting_data flag must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{requesting_data} = $integer if defined $integer;
	$self->{requesting_data} ||= 1111;
	return $self->{requesting_data};
}
#-----------------------------------------------------------------------------

=head2 there_is_data

 Title   : there_is_data
 Usage   : $self->there_is_data();
 Function: Check with MPI to see if data is available.
 Returns :
 Args    : A value to set foo to.

=cut

sub there_is_data {
	my $self = shift;

	my $there_is_data;

	MPI_Recv(\$there_is_data, 1, MPI_INT, 0, $self->data_available, $self->communicator);

	return $there_is_data;
}
#-----------------------------------------------------------------------------

=head2 mpi_recieve

 Title   : mpi_recieve
 Usage   : $data = $self->mpi_recieve($sending_node, $mpi_tag);
 Function: Get serialized data from a node via MPI.
 Returns : A scalar which can represent any data structure that can be serialized by Storable.
 Args    : N/A

=cut

sub mpi_recieve {
	my ($self, $sending_node, $mpi_tag) = @_;

	my ($length, $serial_data);

	MPI_Recv(\$length,      1,       MPI_INT,  $sending_node, 'message_length', $self->communicator);
	MPI_Recv(\$serial_data, $length, MPI_CHAR, $sending_node, $mpi_tag,         $self->communicator);

	my $data = ${thaw($serial_data)};

	return $data;
}
#-----------------------------------------------------------------------------

=head2 mpi_send

 Title   : mpi_send
 Usage   : $self->mpi_send($dest_node, $datum);
 Function: Send data to a node.
 Returns : undef
 Args    : An integer value representing the node to send data to and a scalar
	   that can be a reference to any perl data structure that can be
	   handled by Storable.

=cut

sub mpi_send {
	my ($self, $dest_node, $mpi_tag, $data) = @_;

	my $serial_data = freeze($data);

	my $length = length($serial_data);

	MPI_Send(\$length,        1,       MPI_INT,  $dest_node, 'message_length', $self->communicator);
	MPI_Send(\$serial_data,   $length, MPI_CHAR, $dest_node, $mpi_tag,             $self->communicator);

	return undef;
}
#-----------------------------------------------------------------------------

=head2 return_results

 Title   : return_results
 Usage   : $self->return_results($results, $error);
 Function: Send the results and error from a worker node to the controller
	   via MPI.
 Returns : undef;
 Args    : Two scalars that can be a references to any perl data structure that
	   can be handled by Storable.

=cut

sub return_results {
	my ($self, $results, $error) = @_;

	$self->request_line($self->controller_node);

	$self->mpi_send($self->controller_node, $self->mpi_data,  $results);
	$self->mpi_send($self->controller_node, $self->mpi_error, $error);

	return undef;
}
#-----------------------------------------------------------------------------

=head2 controller_node

 Title   : controller_node
 Usage   : $controller_node = $self->controller_node($integer);
 Function: Get/Set the value of controller_node.
 Returns : The integer value of controller_node.
 Args    : An integer value to set controller_node to.

=cut

sub controller_node {
	my ($self, $integer) = @_;
	$self->throw("Controller node ID must be an integer: $integer")
	  unless $integer =~ /^\d+$/;
	$self->{controller_node} = defined $integer ? $integer : 0;
	return $self->{controller_node};
}
#-----------------------------------------------------------------------------

=head2 shutdown_node

 Title   : shut_down_node
 Usage   : $self->shut_down_node($node);
 Function: Send an undef value to a node for the data_available flag,
	   signalling that node to terminate.
 Returns : undef
 Args    : An integer value representing the worker node.

=cut

sub shutdown_node {
	my ($self, $node) = @_;

	MPI_Send(undef, 1, MPI_INT, $node, $self->data_available, $self->communicator);

	return undef;
}
#-----------------------------------------------------------------------------

=head2 size

 Title   : size
 Usage   : $size = $self->size();
 Function: Get the integer value of the size of the node pool.
 Returns : The an integer value for value of size.
 Args    : N/A

=cut

sub size {
	my $self = shift;

	$self->{size} ||= MPI_Comm_size($self->communicator);

	return $self->{size};
}

#-----------------------------------------------------------------------------

=head2 throw

 Title   : throw
 Usage   : $self->throw($message);
 Function: Throw and error message (carp) and die.
 Returns : N/A
 Args    : A message returned at death.

=cut

sub throw {
	my ($self, $message) = @_;

	croak($message);

}

#-----------------------------------------------------------------------------

=head2 foo

 Title   : foo
 Usage   : $a = $self->foo();
 Function: Get/Set the value of foo.
 Returns : The value of foo.
 Args    : A value to set foo to.

=cut

sub foo {
	my ($self, $value) = @_;
	$self->{foo} = $value if defined $value;
	return $self->{foo};
}

#-----------------------------------------------------------------------------

=head1 DIAGNOSTICS

=for author to fill in:
     List every single error and warning message that the module can
     generate (even the ones that will "never happen"), with a full
     explanation of each problem, one or more likely causes, and any
     suggested remedies.

=over

=item C<< Error message here, perhaps with %s placeholders >>

[Description of error here]

=item C<< Another error message here >>

[Description of error here]

[Et cetera, et cetera]

=back

=head1 CONFIGURATION AND ENVIRONMENT

<MPIcar::Simple> requires no configuration files or environment variables.

=head1 DEPENDENCIES

None.

=head1 INCOMPATIBILITIES

None reported.

=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to:
barry.moore@genetics.utah.edu

=head1 AUTHOR

Barry Moore <barry.moore@genetics.utah.edu>

However all of the hard work was done by Carson Holt.  He took the
very broken Parallel::MPI and fixed it up enough to be useful.  Here
I've simply added some syntactic sugar over the top of his hard work.
Thanks Carson.

=head1 LICENCE AND COPYRIGHT

Copyright (c) 2008, Barry Moore <barry.moore@genetics.utah.edu>.  All rights reserved.

    This module is free software; you can redistribute it and/or
    modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut

1;
