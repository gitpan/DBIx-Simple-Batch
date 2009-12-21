package DBIx::Simple::Batch;

use warnings;
use strict;
use DBIx::Simple;

=head1 NAME

DBIx::Simple::Batch - An Alternative To SQL Stored Procedures using DBIx::Simple

=head1 VERSION

Version 1.64

=cut

our $VERSION = '1.64';
our @properties = caller();

=head1 SYNOPSIS

DBIx::Simple::Batch is formerly DBIx::Simple::Procedure.

This module allows your program to process text files containing one or many
commands that execute SQL statements sequentially. Please keep in mind that
DBIx::Simple::Batch is an alternative to database stored procedures and not
a replacement or emulation of them. Essentially it is an interface to execute
and return data from multiple queries.

Here is an example of how to setup and process a (sql) text file.

    # DBIx::Simple::Batch uses DBIx::Simple and provides an accessor through the
    DBIx::Simple::Batch->{dbix} hash reference.
    
    use DBIx::Simple::Batch;
    my $db = DBIx::Simple::Batch->new($path_to_sqlfiles, 'dbi:SQLite:dbname=file.dat');
    # Will error out using DBIx::Simple if a connection error occurs.
    
    # The queue function takes one parameter (a text file) that contains DBIx::Simple::Batch
    # sql commands.
    
    # The process_queue function processes all queued sql statements using the parameters passed
    # to it, similar to the execute function of DBI.
    
    $db->queue($sql_file)->process_queue(@sql_parameters, {other_param_a => 'excepts_hashrefs_also'});
    
    # The cache function returns an array of resultsets, or the resultset of the index passed,
    # return by the select statements encountered in the sql file.
    # Note! files included using the "include" command will not have there resultsets cached,
    # even if a "capture" command is encountered, only the select statement(s) found in the
    # initial sql file are cached in the order they are encountered. 
    
    foreach my $result (@{$db->sets->[0]}){
        # do something with the records of the first resultset
        $result->{...};
    }

=head1 SQL FILE SYNTAX

The (sql) procedural text file to be processed may contain any text you desire, e.g.
comments and other markup. DBIx::Simple::Batch only reacts to command lines (commands).
These instructions (or commands) must be placed on its own line and be prefixed with an
exclamation point, a space, the command, another space, and the statement to be evaluated.

E.g. "! execute select * from foo".

Multiple commands can be used in a single sql file.
Update! multi-line sql statements are now supported. See below under "Included Example"

    SQL File Commands:
        
    ! execute:
        This command simply execute the supplied sql statement.
    
    ! capture:
        This is an execute command who's dataset will be cached (stored) for later use.
        Note! This command can only be used with a select statement.
    
    ! replace:
        This is an execute command that after successfully executed, replaces the scope
        parameters with data from the last row in its dataset. Note! This command can
        only be used with a select statement.
    
    ! include:
        This command processes the supplied sql file in a sub transaction. Note! Included
        sql file processing is isolated from the current processing. Any capture commands
        encountered in the included sql files will not cache the dataset.
    
    ! proceed:
        This command should be read "proceed if" because it evaluates the string passed
        (perl code) for truth, if true, it continues if false it skips to the next proceed
        command or until the end of the sql file.
    
    ! ifvalid: [validif:]
        This command is a synonym for proceed.
    
    ! storage:
        This command does absolutely nothing except store the sql statement in the commands
        list (queue) for processing individually from within the perl code with a method
        like process_command.
    
    ! declare:
        This command is effectively equivalent to the select .. into sql sytax and uses an
        sql select statement to add vairables to the scope for processing
        (e.g. ! declare select `name` from `foo` where `id` = $0) can be used in other
        instructions as $!name, e.g. ! execute update `foo` set `name` = $!name where `id` = $0.
    
    ! forward:
        This command takes an index and jumps to that command line and continues from there.
        Similar to a rewind or fast forward function for the command queue.
    
    ! process:
        This command takes an index and executes that command line.
    
    ! perl -e:
        This command passes the statement to perl's eval function which can evaluate perl code
        and even runtime variables.
    
    ! examine:
        This command is used for debugging, it errors out with the compiled statement passed to it.
    
=head1 INCLUDED EXAMPLE

Inside tables/users/getall (.sql omitted purposefully).

    # mysql example
    
    # this command tell DSP to treat passed in parameters that are left blank
    # as integers with a value of zero, by default it is '' (an empty string),
    # it can also be null
    ! setting blank as zero
    
    # does what it says, no special magic here
    ! execute create table if not exists `group` (`id` int(11) auto_increment, `info` varchar(255) not null, primary key(`id`) )
    ! execute truncate table `group`
    
    # the declare command stores the list of values in the custom parameters hash
    # using the column names as keys
    ! declare select '0' as `count`
    ! execute insert into `group` values (null, concat_ws(' ', 'I typed', $0, ($!count + 1), 'times.'))
    
    # hopefully not often but there are times when the sql command file should
    # loop, evakuate, etc
    
    # validif, and ifvalid are synonyms for proceed reads a perl expression which
    # may contain passed or declared parameters, and if true proceeds to the next
    # command, not false, skip every following command until it reached a proceed,
    # ifvalid, or validif command the evalutes true, or reaches the end of the file
    
    # begin loop
    ! declare select count(*) as `count` from `group`
    ! validif $!count < 5
    ! forward 4
    ! ifvalid 1
    # end loop
    
    # here is an example of a multi-line sql statement
    ! execute {
    INSERT INTO `group`
    VALUES     (NULL,
                concat_ws(' ', 'The last record to be entered was number', $!count))

    }
    
    # capture store the returned data in the sets array which contains all resultsets
    # returned by the encountered capture commands
    ! capture select * from `group`

Inside the test.pl script.

    #!/usr/env/perl -w
    
    BEGIN {
        use FindBin;
        use lib "$FindBin::Bin/lib";
    }
    
    use DBIx::Simple::Batch;
    
    # connecting to a mysql database
    my $fs = "$FindBin::Bin/sql/";
    my $db = DBIx::Simple::Batch->new(
        $fs,
        'dbi:mysql:database=test', # dbi source specification
        'root', '',                     # username and password
    );
    
    $db->queue('tables/users/getall')->process_queue('this is a test');
    
    foreach my $result (@{$db->sets->[0]}){
        print "$result->{info}\n"; # database column
    }
     
    print "\nDone. Found " . ( @{$db->sets->[0]} || 0 ) . " records";

=head1 PASSED-IN PARAMETERS Vs. CUSTOM PARAMETERS

The difference between (what we refer to as) passed-in parameters and custom parameters 
is determined by how those values are passed to the process_queue and process_command methods.
They are also differentiated by the expressions used identify them. Technically, passed-in
parameters and custom parameters are one in the same. Passed-in parameters are passed to
the process_command and process_queue methods as an array of values and are referred to in the
sql file using expressions like this: [$0, $1, $2, $3]. Custom parameters are passed to the
process_command and process_queue methods as a hash reference and are referred to in the sql file
using expressions like this: [$!hashrefkey1, $!hashrefkey2].

=head1 MORE SQL COMMAND FILE EXAMPLES

Replacing passed in parameters.

    ... in script
    $dsp->queue(...)->process_queue('this', 'that');
    ... in sql file
    ! replace select 'baz' as `foo`
    # $0 in the sql command file was 'this' and now has the value 'baz'
    # $1 in the sql command file was 'that' and now has no value or is undefined
    # basically, the replace command overwrites @_ (all passed in values)
    
    Alternatively...
    
    ... in script
    $dsp->queue(...)->process_queue('this', 'that', { foo => 'the', bar => 'other'});
    ... in sql file
    ! declare select 'baz' as `foo`
    # $0, and $1 in the sql command file are left untouched
    # $!foo in the sql command file was 'the' and now has the value 'baz'
    # basically, the declare command updates or appends the internal (custom parameters) hash

Using the storage, forward and process commands.

    ... in script
    $dsp->queue(...)->process_queue({'foo' => 'bar'});
    ... in sql file
    ! forward 2
    ! execute insert into `foo` (NULL, 'bar', 'baz')
    ! execute insert into `foo` (NULL, 'bar', 'baz')
    ! process 1
    ! capture select * from `foo`
    
    # this sql file is intentionally meant to be confusing, the insert query will
    # be executed 2 times, as a test, see if you can figure out why

NOTE! When using the forward and/or process commands, please be aware that they
both take a command line index which means that if your not careful when you update
the sql file at a later date, you could be shifting the index which means your sql
file will execute but not as you intended.

=head1 METHODS

=cut

=head2 new

The new method initializes a new DBIx::Simple and DBIx::Simple::Batch
object and accepts all parameters required/accepted by DBIx::Simple.

=cut

sub new {
    my ($class, $path, @connect_options) = @_;
    my $self = {};
    bless $self, $class;
    $self->{set_names} = {};
    $self->{sets} = [];
    $self->{path} = $path;
    $self->{dbix} = DBIx::Simple->connect(@connect_options) or die DBIx::Simple->error;
    $self->_load_commands;
    return $self;
}

=begin comment

The _load_commands method is an internal method for build the commands dispatch table.

=end comment
=cut


sub _load_commands {
    my $self = shift;
    
    # identify commands that can only contain select statements
    $self->{select_required} = ['capture', 'replace', 'declare'];
    
    # determine how blank parameters are handled by default
    $self->{settings}->{blank} = '0';
    
    #! capture: stores the resultset for later usage
    $self->{commands}->{capture} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        $self->{sets}->[@{$self->{sets}}] = $self->{processing}->{resultset}->hashes;
        
        # store resultset via name
        $self->{set_names}->{$self->{processing}->{set_name}} =
            $self->{sets}->[(@{$self->{sets}})-1]
                if $self->{processing}->{set_name};
        $self->{processing}->{set_name} = ''
            if $self->{processing}->{set_name};
    };
    
    #! execute: execute sql commands only, nothing else, nothing fancy
    $self->{commands}->{execute} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
    };
    
    #! proceed: evaluates the statement passed (perl code) for truth, if true, it continues if false it
    #  skips to the next proceed command or until the end of the sql file.
    $self->{commands}->{proceed} = sub {
        my ($statement, @parameters) = @_;
        if (@parameters) {
            foreach my $parameter (@parameters) {
                $parameter = $self->{settings}->{blank} unless defined $parameter;
                $statement =~ s/\?/$parameter/;
            }
        }
        $self->{processing}->{skip_switch} = eval $statement ? 0 : 1;
    };
    
    #! ifvalid: a synonym for proceed
    $self->{commands}->{ifvalid} = $self->{commands}->{proceed};
    $self->{commands}->{validif} = $self->{commands}->{proceed};
    
    #! replace: replaces parameters with the data from the last row of the resultset
    $self->{commands}->{replace} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        $self->{processing}->{parameters} = @{$self->{processing}->{resultset}->array};
    };
    
    #! include: processes another (sql) text file
    $self->{commands}->{include} = sub {
        my ($statement, @parameters) = @_;
        my ($sub_sqlfile, $placeholders) = split /\s/, $statement;
        DBIx::Simple::Batch->new($self->{path}, $self->{dbix}->{dbh})
        ->queue($sub_sqlfile)->process_queue(@parameters, $self->{processing}
        ->{custom_parameters});
    };
    
    #! storage: stores sql statements for later
    $self->{commands}->{storage} = sub {
        my ($statement, @parameters) = @_;
    };
    
    #! declare: uses an sql select statement to add vairables to the scope for processing
    $self->{commands}->{declare} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        my $results = $self->{processing}->{resultset}->hash;
        if ($results) {
            my %params = %{$results};
            while ( my ($key, $val) = each %params ) {
                $self->{processing}->{custom_parameters}->{$key} = $val;
            }
        }
    };
    
    #! forward: changes the queue position, good for looping
    $self->{commands}->{forward} = sub {
        no warnings;
        my ($statement, @parameters) = @_;
        $self->{cursor} = $statement;
        next; # purposefully next out of the loop to avoid incrementation. warning should be turned off.
    };
    
    #! process: executes a command in the queue by index number
    $self->{commands}->{process} = sub {
        my ($statement, @parameters) = @_;
        $self->process_command($statement, @parameters);
    };
    
    #! examine: dumps the passed sql statement to the screen (should not be left in the sql file)
    $self->{commands}->{examine} = sub {
        my ($statement, @parameters) = @_;
        my $db = $self->{dbix}->{dbh};
        foreach my $parameter (@parameters) {
            my $placeholder = $db->quote($parameter);
            $statement =~ s/\?/$placeholder/;
        }
        die $self->_error( $statement );
    };
    
    #! setting: configures how the module handles blank parameters
    $self->{commands}->{setting} = sub {
        my ($statement, @parameters) = @_;
        $self->{settings}->{blank} = '0' if (lc($statement) eq 'blank as zero');
        $self->{settings}->{blank} = '' if (lc($statement) eq 'blank as blank');
        $self->{settings}->{blank} = 'NULL' if (lc($statement) eq 'blank as null');
    };
    
    #! setname: configures how the module handles blank parameters
    $self->{commands}->{setname} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{set_name} = $statement if $statement;
    };
    
    #! perl -e: provides access to perl's eval function
    $self->{commands}->{perl} = sub {
        my ($statement, @parameters) = @_;
        $statement =~ s/^\-e//;
        eval $statement;
    }
}

=begin comment

The _execute_query method is an internal method for executing queries against the databse in
a standardized fashion.

=end comment
=cut


sub _execute_query {
    my ($self, $statement, @parameters) = @_;
    my $resultset = $self->{dbix}->query( $statement, @parameters ) or die $self->_error(undef, @parameters);
    return $resultset;
}

=begin comment

The _error method is an internal method that dies with a standardized error message.

=end comment
=cut


sub _error {
    my ( $self, $message, @parameters ) = @_;
    my $error_message = ref($self)
    . " - sql file $self->{file} processing failed execution, "
    . ($message || "database error") . ".";
    if (ref($self->{cmds}) eq "ARRAY") {
        $error_message .= "Point of failure, of command number "
        . ( $self->{cursor} || '0' ) . " ["
        . ( $self->{cmds} ? $self->{cmds}->[ $self->{cursor} ]->{command} : '' )
        . "] "
        . (
        $self->{cmds}->[ $self->{cursor} ]->{statement}
        ? ( "and statement ("
              . substr( $self->{cmds}->[ $self->{cursor} ]->{statement}, 0, 20 )
              . "...) " )
        : " "
        )
        . ( @parameters ? ( "using " . join( ', ', @parameters ) . " " ) : "" )
        . "at $properties[1]"
        . " on line $properties[2], "
        . ( $message || $self->{dbix}->error || "Check the sql file for errors" )
        . ".";
    }
    return $error_message;
}


=begin comment

The _processor method is an internal methoed that when passed a command hashref, processes the command.

=end comment
=cut


sub _processor {
    my ($self, $cmdref) = @_;
    my $command = $cmdref->{command};
    my $statement = $cmdref->{statement};
    
    # replace statement placeholders with actual "?" placeholders while building the statement params list
    # my @statement_parameters = map { $self->{processing}->{parameters}[$_] } $statement =~ m/\$(\d+)/g;
    # $self->{processing}->{statement_parameters} = \@statement_parameters;
    # $statement =~ s/\$\d+/\?/g;
    
    # reset statement parameters
    $self->{processing}->{statement_parameters} = ();
    
    # replace statement placeholders with actual "?" placeholders while building the statement params
    # list using passed or custom parameters
    while ($statement =~ m/(\$\!([a-z0-9A-Z\_\-]+))|(\$(\d+(?!\w)))/) {
        my $custom = $2;
        my $passed = $4;
        # if the found param is a custom param
        if (defined $custom) {
            push @{$self->{processing}->{statement_parameters}}, $self->{processing}->{custom_parameters}->{$custom};
            $statement =~ s/\$\!$custom/\?/;
        }
        # if the found param is a passed-in param
        if (defined $passed) {
            push @{$self->{processing}->{statement_parameters}}, $self->{processing}->{parameters}[$passed];
            $statement =~ s/\$$passed/\?/;
        }
    }
    
    if ($self->{processing}->{skip_switch} && ( $command ne "proceed" && $command ne "ifvalid" && $command ne "validif" ) )
    {
        # skip command while skip_switch is turned on
        return;    
    }
    else
    {
        # execute command
        $self->{commands}->{$command}->($statement, @{$self->{processing}->{statement_parameters}});
        return $self->{processing}->{resultset};
    }
}

=begin comment

The _parse_parameters method examines each initially passed in parameter specifically looking for a hashref
to add its values to the custom parameters key.

=end comment
=cut


sub _parse_parameters {
    my ($self, @parameters) = @_;
    for (my $i=0; $i < @parameters; $i++) {
        my $param = $parameters[$i];
        if (ref($param) eq "HASH") {
            while (my($key, $val) = each (%{$param})) {
                $self->{processing}->{custom_parameters}->{$key} = $val;
            }
            delete $parameters[$i];
        }
    }
    $self->{processing}->{parameters} = \@parameters;
    return $self;
}

=begin comment

The _parse_sqlfile method scans the passed (sql) text file and returns a list of sql statement queue objects.

=end comment
=cut


sub _parse_sqlfile {
    my ($self, $sqlfile) = @_;
    my (@lines, @statements);
    # open file and fetch commands
    $self->{file} = $sqlfile;
    open (SQL, "$self->{path}$sqlfile") || die $self->_error( "Could'nt open $self->{path}$sqlfile sql file" );
    push @lines, $_ while(<SQL>);
    close SQL || die $self->_error( "Could'nt close $self->{path}$sqlfile sql file" );
    # attempt to parse commands w/multi-line sql support
    my $use_mlsql = 0;
    my $mlcmd = '';
    my $mlsql = '';
    foreach my $command (@lines) {
        if ($command =~ /^\!/) {
            my @commands = $command =~ /^\!\s(\w+)\s(.*)/;
            if (grep ( $commands[0] eq $_, keys %{$self->{commands}})) {
                if ($commands[1] =~ /^\{/) {
                    $use_mlsql = 1;
                    $mlcmd = $commands[0];
                    next;
                }
                else {
                    push @statements, { "command" => "$commands[0]", "statement" => "$commands[1]" };
                }
            }
        }
        if ( $use_mlsql == 1 ) {
            if ( $command !~ /^\}$/ ) {
                $mlsql .= $command;
                next;
            }
            else {
                push @statements, { "command" => "$mlcmd", "statement" => "$mlsql" };
                $use_mlsql = 0;
                $mlcmd = '';
                $mlsql = '';
            }
        }
    }
    # validate statements
    $self->_validate_sqlfile(@statements);
    return @statements;
}

=begin comment

The _validate_sqlfile method make sure that the supplied (sql) text file conforms to its command(s) rules.

=end comment
=cut


sub _validate_sqlfile {
    my ($self, @statements) = @_;
    # rule1: replace, and capture can only be used with select statements
    foreach my $statement (@statements) {
        if (grep ( $statement->{command} eq $_, @{$self->{select_required}})) {
            if (lc($statement->{statement}) !~ /^(\s+)?select/) {
                die $self->_error( "Validation of the sql file $self->{file} failed. The command ($statement->{command}) can only be used with an SQL (select) statement.", $statement->{statement});
            }
        }
    }
}

=head2 queue

The queue function parses the passed (sql) text file and build the list of sql statements to be
executed and how.

=cut

sub queue {
    my ($self, $sqlfile) = @_;
    my (@statements);
    $self->{cmds} = '';
    
    # set caller data for error reporting
    @properties = caller();
    @statements = $self->_parse_sqlfile($sqlfile);
    $self->{cmds} = \@statements;
    return $self;
}

=head2 process_queue

The process_queue function sequentially processes the recorded commands found the (sql) text file.

=cut

sub process_queue {
    my ($self, @parameters) = @_;
    # set caller data for error reporting
    @properties = caller();
    $self->_parse_parameters(@parameters) if @parameters;
    $self->{processing}->{skip_switch} = 0;
    $self->{cursor} = 0; 
    if (@{$self->{cmds}}) {
        # process sql commands 
        for (my $i = 0; $self->{cursor} < @{$self->{cmds}}; $i++) {
            my $cmd = $self->{cmds}->[$self->{cursor}];
            if ( grep($cmd->{command} eq $_, keys %{$self->{commands}}) )
            {
                # process command
                $self->_processor($cmd);
                $self->{cursor}++;
            }
        }
        return $self->{processing}->{resultset};
    }
    else {
        die $self->_error( "File has no commands to process" );
    }
    return $self;
}

=head2 sets

The sets method provides direct access to the resultsets array or
resultsets.

=cut

sub sets {
    return shift->{sets};
}

=head2 cache

The cache method accesses an arrayref of resultsets that were captured
using the (sql file) capture command and returns the resultset of the
index or name passed to it or an empty arrayref.

=cut

sub cache {
    my ($self, $index) = @_;
    if ($index =~ /^\d+$/) {
        if ($self->{sets}->[$index]) {
            return $self->{sets}->[$index];
        }
    }
    else {
        if ($self->{set_names}->{$index}) {
            return $self->{set_names}->{$index};
        }
    }
    return 0;
}

=head2 rs

The rs method is a synonym for the cache method

=cut

sub rs {
    return shift->cache(@_);
}

=head2 command

The command method is used to queue a command to be processed later by the process_queue method. Takes two
arguments, "command" and "sql statement", e.g. command('execute', 'select * from foo').

=cut

sub command {
    my ($self, $command, $statement) = @_;
    my @statements = @{$self->{cmds}};
    push @statements, { "command" => "$command", "statement" => "$statement" };
    $self->{cmds} = \@statements;
    return $self;
}

=head2 process_command

The process_command method allows you to process the indexed sql satements from your sql file
individually. It take two argument, the index of the command as it is encountered in the sql file and tries
returns a resultset, and any parameters that need to be passed to it.

=cut

sub process_command {
    my ($self, $index, @parameters) = @_;
    my $cmd = $self->{cmds}->[$index];
    if ( grep($cmd->{command} eq $_, keys %{$self->{commands}}) )
    {
        # process command
        $self->_parse_parameters(@parameters) if @parameters;
        return $self->_processor($cmd);
    }
}

=head2 clear

The clear method simply clears the cache (resultset store)

=cut

sub clear {
    my $self = shift;
    $self->{cmds} = '';
    $self->{set_names} = {};
    $self->{sets} = [];
    $self->{processing}->{resultset} = '';
    $self->{processing}->{skip_switch} = 0;
    $self->{processing}->{parameters} = [];
    $self->{processing}->{custom_parameters} = {};
    $self->{cursor} = 0;
    
    return $self;
}

=head1 AUTHOR

Al Newkirk, C<< <al.newkirk at awnstudio.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-DBIx-Simple-Batch at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Simple-Batch>.
I will be notified, and then you'll automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Simple::Batch


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-Simple-Batch>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-Simple-Batch>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Simple-Batch>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-Simple-Batch/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2009 Al Newkirk.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of DBIx::Simple::Batch
