package FarmHand;

use strict;
use warnings;

use lib "$ENV{HOME}/FarmHand/lib/";
use Parallel::MPIcar qw(:all);
use Storable qw(freeze thaw);
use Error qw(:try);
use Log::Log4perl qw(get_logger :levels :easy);
use Carp;

use vars qw($VERSION);
$VERSION = '0.01';

=head1 NAME

FarmHand

=head1 VERSION

This document describes FarmHand version 0.01

=head1 SYNOPSIS

  use FarmHand;

  my $mpi = FarmHand->new(log_conf        => $log_conf,
                          controller_node => 0);

  #If this is the controller node manage the data
  if ($mpi->is_controller) {
	  #Loop over data and send each datum to controller
	  for my $data (@data) {
		  my $results = $mpi->control_workers($data);
		  print "$results\n"
	  }
	  #Loop over each node and collect final data and
	  #shutdown node.
	  while (my $node = $mpi->next_node) {
		  my $results = $mpi->cleanup_node($node);
		  print "$results\n";
	  }
  }
  #If this is a worker node, do some work using a callback.
  else {
	  $mpi->do_work(\&handler);
  }

  #Write a callback to do the actual work.
  sub handler {
	  my $data = shift;
	  #Do something with data
	  my $results = $data * 2;
	  $mpi->log("Something bad happened", 'error');
	  throw Error::Simple("An error occured");
	  return $results
  }

=head1 DESCRIPTION

FarmHand provides a perl interface to the MPI library allowing you to
write perl scripts that can execute a function on multiple data
points in parallel in an MPI environment with minimal interaction with
(or understanding of) MPI specific code.

As shown in the SYNOPSIS above your code will have a data loop which
calls control_worker for each element of your data set.  It will have
a while loop which will iterate over each node to clean up that node
and shut it down.  It will direct all nodes that aren't the controller
to do_work.  And finally, it will define a callback subroutine which
will be passed to the do_work method as a code reference and which
will recieve as input the data that you sent to control_workers.  If
can execute any perl code on that data and return results.

FarmHand also implements logging and error handling to help you keep
your big jobs verbose, stable and running.  Logging is managed by
Log::Log4perl, and error handeling is managed by Error.pm, so see docs
for those packages the appropriate methods in this package for more
details.

FarmHand uses Parallel::MPIcar under the hood for all MPI
communictions.  A big thank you to Carson Holt for doing the hard work
of getting that module in working shape.

=head1 METHODS

=cut

#-----------------------------------------------------------------------------

=head2 new

     Title   : new
     Usage   : FarmHand->new();
     Function: Creates a FarmHand object;
     Returns : A FarmHand object.
     Args    : See the associated methods for more details.
	       controller_node log_conf

=cut

sub new {
	my ($class, %args) = @_;
	my $self = {};
	bless $self, $class;
	$self->_initialize_args(\%args);

	#Make a closure based counter to use in the cleanup process.
	$self->{counter} = $self->_make_counter;

	# A stupid hack to get around the problem that MPI_Init
	# requires something on the command line.
	push @ARGV, '' if scalar @ARGV == 0;

	my $logger = $self->logger;

	$logger->debug("About to initiate MPI\n");

	MPI_Init(); #initiate the MPI

	$logger->debug("Initiated MPI\n");

	return $self;
}

#-----------------------------------------------------------------------------

sub _initialize_args {

	my ($self, $args) = @_;

	my @data_types = keys %{$args};

	for my $type (@data_types) {
		$args->{$type} && $self->$type($args->{$type});
	}

	# Set up defaults
	$self->{requesting_data} = 1111;
	$self->{mpi_data}        = 2222;
	$self->{data_available}  = 3333;
	$self->{message_length}  = 4444;
	$self->{mpi_results}     = 5555;

}
#-----------------------------------------------------------------------------

=head2 control_workers

 Title   : control_workers
 Usage   : $self->control_workers($data);
 Function: Run the controller which will collect results and send off new
	   data to a worker node.
 Returns : undef
 Args    : A scalar or a reference to a more complex data structure
	   containing data.

=cut

sub control_workers {
	my ($self, $data) = @_;

	my $logger = $self->logger;

	# Signal that there is work to be done.
	my $there_is_data = 1;

	$self->log("Begin control_workers\n");

	# Recieve message from any node that needs more data
	my $worker_node = $self->get_worker_node;

	$self->log("Got worker");

	my $results;

	# If this node has had work in the past then collect that work and process it.
	if ($self->node_is_active($worker_node)) {

		$self->log("node_is_active");

		# Receive results
		$results = $self->get_results($worker_node);

		$self->log("got results");
	}

	# Keep track of which nodes have been sent work.
	$self->activate_node($worker_node);

	$self->log("sending data_available tag");

	MPI_Send(\$there_is_data, 1, MPI_INT, $worker_node, $self->data_available, MPI_COMM_WORLD);

	$self->log("sending data");

	# Send data to worker node
	$self->mpi_send_data($worker_node, $self->mpi_data, \$data);

	$self->log("returning results");

	return ($results);

}
#-----------------------------------------------------------------------------

=head2 cleanup_node

 Title   : cleanup_node
 Usage   : $self->cleanup_node;
 Function: Retrieve any final results from worker nodes.
 Returns : A scalar string from the worker node representing the results
 Args    : An integer representing the node ID to clean up and optionally
	   a code reference to do any custom cleanup on the node before
	   shutdown.

=cut

sub cleanup_node {
	my ($self, $node) = @_;

	my $logger = $self->logger;

	$self->log("start");

	# $worker_node is going to be the same as $node, just need a place holder.
	my $worker_node;

	$self->log("get a worker");

	MPI_Recv(\$worker_node, 1, MPI_INT, $node, $self->requesting_data, MPI_COMM_WORLD);

	$self->log("got a worker");

	my $results;
	if ($self->node_is_active($worker_node)) {

		# Receive results
		$results = $self->get_results($worker_node);

		$self->log("got results");

	}

	# Send 0 for data_available flag to worker signaling it to terminate.
	$self->shutdown_node($node);

	$self->log("finished");

	return $results;
}
#-----------------------------------------------------------------------------

=head2 _make_counter

 Title   : _make_counter
 Usage   : my $counter = $self->_make_counter
 Function: This creates a closure that produces a counter that begins at 0
	   and counts up by integers.
 Returns : an interger representing a node or undef
 Args    : N/A

=cut

sub _make_counter {

	my $self = shift;

	my $start = 0;

	return sub { $start++ }
}
#-----------------------------------------------------------------------------

=head2 next_node

 Title   : next_node
 Usage   : $self->next_node;
 Function: This function returns a different node on each call until all
	   nodes have been returned at which point it resets itself and
	   returns undef.
 Returns : an interger representing a node or undef
 Args    : N/A

=cut

sub next_node {

	my $self = shift;

	my $logger = $self->logger;

	$self->log("start");

	my $node = &{$self->{counter}};

	# Don't ever return the controller node (we don't want to shut it down);
	$node = &{$self->{counter}} if $node == $self->controller_node;

	if ($node >= $self->size) {

		# Reset the counter;
		$self->{counter} = $self->_make_counter;
		return undef;

	}

	$self->log("end");

	return $node;
}
#-----------------------------------------------------------------------------

=head2 do_work

 Title   : do_work
 Usage   : $self->do_work(init_handler    => \&init_handler,
			  job_handler     => \&job_handler,
			  cleanup_handler => \&cleanup_handler);
 Function: Run a worker node which will execute a callback to initialize the
	   node, then run a callback on each $data passed to it and finally
	   run a callback to cleanup the node.
 Returns : undef
 Args    : A hash or array handlers.  The job_handler should be considered
	   mandatory since the nothing will be run otherwise.

=cut

sub do_work {
	my ($self, %handlers) = @_;

	my $init_handler    = $handlers{init_handler};
	my $job_handler     = $handlers{job_handler};
	my $cleanup_handler = $handlers{cleanup_handler};

	my $logger = $self->logger;

	my $there_is_data;

	$self->log("start");

	# Run any custom initialization code passed by user.
	if (ref $init_handler eq 'CODE') {
		try {
			&{$init_handler};
		}
		catch Error with {
			my $exception = shift;
			$self->log("init_handler error", 'error');
			$self->log($exception->text, 'error');
		};
	}

	$self->log("completed init_handler");

	# Run the job handler while data is available.
	while (my $data = $self->recieve_data) {

		$self->log("top of data while");

		my $results;

		if (ref $init_handler eq 'CODE') {
			try {
				$results = &{$job_handler}($data);
			}
			catch Error with {
				my $exception = shift;
				$self->log("job_handler error", 'error');
				$self->log($exception->text, 'error');
			};
		}

		$self->return_results($results);

		$self->log("returned results in data while\n");

	}

	$self->log("completed job_handler");

	# Run any custom cleanup code passed by users.
	if (ref $cleanup_handler eq 'CODE') {
		try {
			&{$cleanup_handler};
		}
		catch Error with {
			my $exception = shift;
			$self->log("cleanup_handler error", 'error');
			$self->log($exception->text, 'error');
		};
	}

	$self->log("finished\n");
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

=head2 data_available

 Title   : data_available
 Usage   : $self->data_available();
 Function: Get the data_available MPI flag to an integer value.
 Returns : An integer.
 Args    : N/A.

=cut

sub data_available {
	return shift->{data_available};
}
#-----------------------------------------------------------------------------

=head2 log

 Title   : log
 Usage   : $self->log('Log message', $level);
 Function: Prints a log message using log4perl
 Returns : undef
 Args    : A message and a level (level defaults to debug).  Valid levels are
	   debug, info, warn, error, fatal.  The level of messages which will
	   be printed to the log are currently controlled by the
	   farmhand.log.conf file.  This is expected to change soon to become
	   more general.

=cut

sub log {
	my ($self, $message, $level) = @_;

	chomp $message;

	$level ||= 'debug';

	my $logger = $self->logger;

	my $rank = $self->rank;
	my ($package, $filename, $line) = caller;

	$level = lc $level;

	my @valid_levels = qw(debug info warn error fatal);

	$level = 'debug' unless grep {$level eq $_} @valid_levels;

	$logger->$level("$package:$line  Node:$rank  $message");

	return undef;
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

	my $logger = $self->logger;

	$self->log("start");

	my $results = $self->mpi_recieve_data($worker_node, $self->mpi_results);

	$results ||= '';

	$self->log("end");

	return $results;
}
#-----------------------------------------------------------------------------

=head2 get_worker_node

 Title   : get_worker_node
 Usage   : $worker_node = $self->get_worker_node;
 Function: Get the node ID of an available worker.
 Returns : An integer.
 Args    : None

=cut

sub get_worker_node {
	my $self = shift;

	my $logger = $self->logger;

	$self->log("start");

	my $worker_node;
	MPI_Recv(\$worker_node, 1, MPI_INT, -2, $self->requesting_data, MPI_COMM_WORLD);

	$self->log("end");
	$self->log("node: $worker_node");

	return $worker_node;
}

#-----------------------------------------------------------------------------

=head2 message_length

 Title   : message_length
 Usage   : $message_length = $self->message_length($message_length);
 Function: Get the integer value of the message_length flag.
 Returns : The integer value of the message_length flag.
 Args    : N/A

=cut

sub message_length {
	return shift->{message_length};
}
#-----------------------------------------------------------------------------

=head2 mpi_data

 Title   : mpi_data
 Usage   : $mpi_data = $self->mpi_data($mpi_data);
 Function: Get the integer value of the mpi_data flag.
 Returns : The integer value of the mpi_data flag.
 Args    : N/A

=cut

sub mpi_data {
	return shift->{mpi_data};
}
#-----------------------------------------------------------------------------

=head2 mpi_results

 Title   : mpi_results
 Usage   : $mpi_results = $self->mpi_results($mpi_results);
 Function: Get the interger value of the mpi_results flag.
 Returns : The integer value of the mpi_results flag.
 Args    : N/A

=cut

sub mpi_results {
	return shift->{mpi_results};
}
#-----------------------------------------------------------------------------

=head2 node_is_active

 Title   : node_is_active
 Usage   : while ($self->node_is_active) { print $self->rank . " is active\n";
 Function: Determine if the current node (as determined by $self->rank) has
	   been previously given a job.
 Returns : 1 if the node is active undef otherwise
 Args    : The integer value of a node ID.

=cut

sub node_is_active {
	my ($self, $node) = @_;
	if (defined $self->{active_nodes}[$node] &&
	    $self->{active_nodes}[$node] > 0) {
		return 1;
	}
	else {
		return undef;
	}
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
	$self->{rank} ||= MPI_Comm_rank(MPI_COMM_WORLD);
	return $self->{rank};
}
#-----------------------------------------------------------------------------

=head2 recieve_data

 Title   : recieve_data
 Usage   : while (my $data = $self->recieve_data) {print $data}
 Function: Get data from the controller.
 Returns : A variable containing some arbitrary perl data structure (no
	   coderefs please) or undef if no data is available.
 Args    : N/A

=cut

sub recieve_data {
	my $self = shift;

	my $data;

	my $logger = $self->logger;

	$self->log("start");

	my $rank = $self->rank;

	my $there_is_data;

	$self->log("requesting data");

	MPI_Send(\$rank,  1, MPI_INT, $self->controller_node, $self->requesting_data, MPI_COMM_WORLD);

	$self->log("requested data");

	MPI_Recv(\$there_is_data, 1, MPI_INT, $self->controller_node, $self->data_available,  MPI_COMM_WORLD);

	$self->log("there_is_data: $there_is_data");

	if ($there_is_data) {

		$self->log("if there_is_data start");

		$data = $self->mpi_recieve_data($self->controller_node, $self->mpi_data);

		$self->log("recieved data");

		return $data;
	}

	return undef;

}
#-----------------------------------------------------------------------------

=head2 requesting_data

 Title   : requesting_data
 Usage   : $requesting_data_tag = $self->requesting_data($integer);
 Function: Gets the integer value of the requesting data tag.
 Returns : An integer representing the requesting data tag.
 Args    : N/A

=cut

sub requesting_data {
	return shift->{requesting_data};
}
#-----------------------------------------------------------------------------

=head2 mpi_recieve_data

 Title   : mpi_recieve_data
 Usage   : $data = $self->mpi_recieve_data($source_node, $mpi_tag);
 Function: Get serialized data from a node via MPI.
 Returns : A scalar which can represent any data structure that can be serialized by Storable.
 Args    : N/A

=cut

sub mpi_recieve_data {
	my ($self, $source_node, $mpi_tag) = @_;

	my ($length, $serial_data);

	my $logger = $self->logger;

	$self->log("start");

	$self->validate_mpi_snd_rcv($source_node, $mpi_tag);

	MPI_Recv(\$length,      1,       MPI_INT,  $source_node, $self->message_length, MPI_COMM_WORLD);

	$self->log("got length");

	$self->log("length: $length, source_node: $source_node, mpi_tag: $mpi_tag");

	MPI_Recv(\$serial_data, $length, MPI_CHAR, $source_node, $mpi_tag,         MPI_COMM_WORLD);

	$self->log("got data");

	my $thawed = thaw($serial_data);

	my $data = defined $thawed ? ${$thawed} : '';

	return $data;
}
#-----------------------------------------------------------------------------

=head2 mpi_send_data

 Title   : mpi_send_data
 Usage   : $self->mpi_send_data($dest_node, $datum);
 Function: Send data to a node.
 Returns : undef
 Args    : An integer value representing the node to send data to and a scalar
	   that can be a reference to any perl data structure that can be
	   handled by Storable.

=cut

sub mpi_send_data {
	my ($self, $dest_node, $mpi_tag, $data) = @_;

	$data = '' if ! defined $data;

	my $serial_data = freeze($data);

	my $length = length($serial_data);

	my $logger = $self->logger;

	$self->log("start");

	$self->validate_mpi_snd_rcv($dest_node, $mpi_tag);

	MPI_Send(\$length, 1, MPI_INT,  $dest_node, $self->message_length, MPI_COMM_WORLD);

	$self->log("length: $length, dest_node: $dest_node, mpi_tag: $mpi_tag");

	$self->log("sent data");

	MPI_Send(\$serial_data,   $length, MPI_CHAR, $dest_node, $mpi_tag,              MPI_COMM_WORLD);

	$self->log("end");

	return undef;
}
#-----------------------------------------------------------------------------

=head2 validate_mpi_snd_rcv

 Title   : validate_mpi_snd_rcv
 Usage   : $self->validate_mpi_snd_rcv($node, $mpi_tag);
 Function: Checks for the existance of and validity of the parameters passed to
	   MPI_Send and MPI_Rcv to avoid difficult to debug errors.
 Returns : 1 on success, undef on failure;
 Args    : $node (source or destination), $mpi_tag

=cut

sub validate_mpi_snd_rcv {

	my ($self, $node, $mpi_tag) = @_;

	if ($node   !~ /^\d+$/) {
		throw Error::Simple("dest_node sent to mpi_send(recieve)_data must be an integer, not $node");
	}
	elsif ($mpi_tag !~ /^\d+$/) {
		throw Error::Simple("mpi_tag sent to mpi_send(recieve)_data must be an integer, not $mpi_tag");
	}
	return 1;
}
#-----------------------------------------------------------------------------

=head2 return_results

 Title   : return_results
 Usage   : $self->return_results($results);
 Function: Send the results from a worker node to the controller
	   via MPI.
 Returns : undef;
 Args    : A scalar that can be a references to any perl data structure that
	   can be handled by Storable.

=cut

sub return_results {
	my ($self, $results) = @_;

	my $logger = $self->logger;

	$self->log("start");

	$self->mpi_send_data($self->controller_node, $self->mpi_results, \$results);

	$self->log("end");

	return undef;
}
#-----------------------------------------------------------------------------

=head2 controller_node

 Title   : controller_node
 Usage   : $controller_node = $self->controller_node($integer);
 Function: Get/Set the value of controller node - the node ID for the node
	   that will control the workers.
 Returns : The integer value of controller_node.
 Args    : An integer value to set controller_node to.

=cut

sub controller_node {
	my ($self, $integer) = @_;
	if (defined $integer && $integer !~ /^\d+$/) {
		throw Error::Simple("Controller node ID must be an integer: $integer")
	}
	$self->{controller_node} = $integer if defined $integer;
	$self->{controller_node} ||= 0;
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

	my $logger = $self->logger;

	$self->log("start node: $node");

	my $signal = 0;

	MPI_Send(\$signal, 1, MPI_INT, $node, $self->data_available, MPI_COMM_WORLD);

	$self->log("end");

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

	my $logger = $self->logger;

	$self->log("start");

	$self->{size} ||= MPI_Comm_size(MPI_COMM_WORLD);

	$self->log("end");

	return $self->{size};
}

#-----------------------------------------------------------------------------

=head2 is_controller

 Title   : is_controller;
 Usage   : if ($self->is_controller) { ... }
 Function: Determine if this instance is the controller node
 Returns : True of $self->rank == $self->controller_node;
 Args    : N/A

=cut

sub is_controller {
	my $self = shift;

	return 1 if $self->rank == $self->controller_node;

	return undef;

}
#-----------------------------------------------------------------------------

=head2 log_conf

 Title   : logger
 Usage   : my $log_conf = $self->log_conf($log_conf);
 Function: Return the name (path) of the log configuration file.
 Returns : A scalar string.
 Args    : A scalar string.

=cut

sub log_conf {
	my ($self, $log_conf) = @_;

	$self->{log_conf} = $log_conf if defined $log_conf;

	if ( ! defined $self->{log_conf}) {
		if (-e "$ENV{HOME}/.farmhand.log.conf") {
			$self->{log_conf} = "$ENV{HOME}/.farmhand.log.conf";
		}
		elsif (-e '/etc/.farmhand.log.conf') {
			$self->{log_conf} = '/etc/.farmhand.log.conf';
		}
	}

	return $self->{log_conf};
}
#-----------------------------------------------------------------------------

=head2 logger

 Title   : logger
 Usage   : my $logger = $self->logger
 Function: Return a Log4perl object
 Returns : Log4perl object
 Args    : N/A

=cut

sub logger {
	my $self = shift;

	if ( ! defined $self->{logger} ) {
		if (-e $self->log_conf) {
			Log::Log4perl->init($self->log_conf);
		}
		else {
			Log::Log4perl->easy_init($DEBUG);
		}

		$self->{logger} = get_logger('FarmHand');
	}

	return $self->{logger};
}
#-----------------------------------------------------------------------------

=head2 DESTROY

 Title   : DESTROY
 Usage   : Dont use this method directly.
 Function: Do an MPI_Finalize() on destruction of each node.
 Returns : N/A
 Args    : N/A

=cut

sub DESTROY {
	my $self = shift;

	MPI_Finalize();
}
#-----------------------------------------------------------------------------

=head1 DIAGNOSTICS

=for

     Error handeling is managed by Error.pm.  All exceptions thrown by
     the worker callback are trapped and reported.  This means that
     most errors that are thrown are defined by the user in their
     callback.

=over

=item C<< dest_node sent to mpi_send(recieve)_data must be an integer, not $node >>

You have sent an destination node value to mpi_send_data or
mpi_recieve_data which is not an integer. This is almost certainly not
your fault, and should be reported as a bug to the author.

=item C<< mpi_tag sent to mpi_send(recieve)_data must be an integer, not $mpi_tag >>

You have sent an mpi_tag value to mpi_send_data or mpi_recieve_data
which is not an integer.  This is almost certainly not your fault, and
should be reported as a bug to the author.

=back

=head1 CONFIGURATION AND ENVIRONMENT

<FarmHand> requires a file currently named farmhand.log.conf which
defines a configuration for logging.  This file is used by
Log::Log4perl and is described in the docs to that module.

=head1 DEPENDENCIES

Parallel::MPIcar;
Storable;
Error;
Log::Log4perl;

=head1 INCOMPATIBILITIES

None reported.

=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to:
barry.moore@genetics.utah.edu

=head1 AUTHOR

Barry Moore <barry.moore@genetics.utah.edu>

However the hard work of getting Parallel::MPI working was done by
Carson Holt.  He took the very broken Parallel::MPI and fixed it up
enough to be useful.  Here I've simply added some syntactic sugar over
the top of Parallel::MPIcar.  Thanks Carson.

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
