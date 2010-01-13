package DBIx::Simple::Batch;

use warnings;
use strict;
use DBIx::Simple;
use File::Find;

=head1 NAME

DBIx::Simple::Batch - An Alternative To ORM and SQL Stored Procedures.

=head1 VERSION

Version 1.65

=head1 DOCUMENTATION

=over 4

=item * L<DBIx::Simple::Batch::Documentation>

=item * L<http://app.alnewkirk.com/pod/projects/dbix/simple/batch/>

=back

=cut

our $VERSION = '1.66';
our @properties = caller();

=head1 METHODS

=head2 new

I<The new method initializes a new DBIx::Simple::Batch object.>

new B<arguments>

=over 3

=item L<$path|/"$path">

=item L<@connection_string|/"@connection_string">

=back

new B<usage and syntax>

    $db = DBIx::Simple::Batch->new($path);
    
    takes 2 arguments
        1st argument           - required
            $path              - path to folder where sql files are stored
        2nd argument           - required
            @connection_string - display help for a specific command
            
    example:
    my $path = '/var/www/app/queries';
    my @connection_string = ('dbi:SQLite:/var/www/app/foo.db');
    my $db = DBIx::Simple::Batch->new($path, @connection_string);
    
    # $path can also take a file pattern which turns on object mapping
    my $path = '/var/www/app/queries/*.sql';
    my @connection_string = ('dbi:SQLite:/var/www/app/foo.db');
    my $db = DBIx::Simple::Batch->new($path, @connection_string);
    # now you can
    $db->call->folder->file(...);

=cut

sub new {
    my ($class, $path, @connect_options) = @_;
    my $self = {};
    my $file_pattern = '';
    bless $self, $class;
    $self->{set_names} = {};
    $self->{sets} = [];
    
    $self->{dbix} = DBIx::Simple->connect(@connect_options)
        or die DBIx::Simple->error;
    
    ($path, $file_pattern) = $path =~ m/([^\*]+)(\*\.[\w\*]+)?/;
    
    unless (-d -r $path) {
        die "The path specified '$path', " .
        "does not exist and/or is not accessible.";
    }
    
    $self->{path}          = $path =~ m/[\\\/]$/ ? $path : "$path/";
    $self->{file_pattern}  = $file_pattern;
    
    # turn-on object mapping
    if ($self->{file_pattern}) {
        our @package = ("package DBIx::Simple::Batch::Map;\n");
        our $package_switch = 0;
        our $new_routine = 'sub new {my $class = shift;my $base  = shift;my $self = {};$self->{base} = $base;bless $self, $class;return $self;}';
        our $has_folder = 0;
        find sub {
            my $file      = $_;
            my $file_path = $File::Find::name;
            my $directory = $File::Find::dir;
            my $namespace = 'DBIx::Simple::Batch::Map::';
            
            # specify package
            if (-d $file_path) {
                my $package_name = $file_path;
                my $prune = $path; $prune =~ s/[\\\/]+$//;
                $package_name =~ s/^$prune([\\\/])?//;
                if ($package_name) {
                    $package_name =~ s/[\\\/]/::/g;
                    $package_name =~ s/[^:a-zA-Z0-9]/\_/g;
                    push @package, "$new_routine\n";
                    my $fqns = "$namespace$package_name";
                    my $instantiator = "$fqns"."->new(". 'shift->{base}' .")";
                    my $sub = $package_name; $sub =~ s/.*::([^:]+)$/$1/;
                    push @package, "sub $sub { return $instantiator }\n" . "package $fqns;\n";
                    $package_switch = 1;
                    $has_folder = 1;
                }
            }
            elsif (-f $file_path) {
                my $pat = $self->{file_pattern};
                if ($pat) {
                    $pat =~ s/^\*\.([\*\w]+)/\.\*\.$1/;
                }
                else {
                    $pat = '.*';
                }
                if ($file =~ /$pat/) {
                    my $name = $file;
                    $name =~ s/\.\w+$//g;
                    $name =~ s/\W/\_/g;
                    push(@package, "$new_routine\n") if $package_switch == 1;
                    #
                    $package_switch = 0;
                    push @package, "sub $name ". '{ my $db = shift->{base}; return $db->queue(\''. $file_path .'\')->process(@_); }' ."\n";
                }
            }
            else {
                push @package, "$file -> ???\n";
            }
            
        }, $self->{path};
        # ugly no folders hack, this whole instantiation should be rewritten
        unless ($has_folder){
            my $package_name = shift(@package);
            unshift @package, $package_name, $new_routine;
        }
        eval "@package";
            die "Error mapping sql file objects: $@" if $@;
    }
    
    # load directives
    $self->_load_commands;
    return $self;
}

=head2 call

I<The call method is used to access and process sql files in an object-oriented fashion.>

call B<arguments>

No arguments.

call B<usage and syntax>

    $db->call;
    
    takes 0 arguments
            
    example:
    $db->call->file(...);
    $db->call->folder->file;
    $db->call->folder->folder->file;

=cut

sub call { return DBIx::Simple::Batch::Map->new(shift); }

# The _load_commands method is an internal method for building the commands
# dispatch table.

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
        @parameters = split /[\,\s]/, $placeholders if $placeholders;
        my $sub = DBIx::Simple::Batch->new($self->{path}, $self->{dbix}->{dbh});
        $sub->queue($self->{path}.$sub_sqlfile)->process_queue(@parameters,
        $self->{processing}->{custom_parameters});
        # copying sub resultsets
        if (keys %{$sub->{set_names}}) {
            map {
                $self->{set_names}->{$_} = $sub->{set_names}->{$_}
            } keys %{$sub->{set_names}};
        }
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

# The _execute_query method is an internal method for executing queries
# against the databse in a standardized fashion.

sub _execute_query {
    my ($self, $statement, @parameters) = @_;
    my $resultset = $self->{dbix}->query( $statement, @parameters ) or
        die $self->_error(undef, @parameters);
    return $resultset;
}

# The _error method is an internal method that dies with a standardized
# error message.

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

# The _processor method is an internal methoed that when passed a command
# hashref, processes the command.

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

# The _parse_parameters method examines each initially passed in parameter
# specifically looking for a hashref to add its values to the custom
# parameters key.

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

# The _parse_sqlfile method scans the passed (sql) text file and returns
# a list of sql statement queue objects.

sub _parse_sqlfile {
    my ($self, $sqlfile) = @_;
    my (@lines, @statements);
    # open file and fetch commands
    $self->{file} = $sqlfile;
    open (SQL, "$sqlfile") || die $self->_error( "Could'nt open $sqlfile sql file" );
    push @lines, $_ while(<SQL>);
    close SQL || die $self->_error( "Could'nt close $sqlfile sql file" );
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

# The _validate_sqlfile method make sure that the supplied (sql) text
# file conforms to its command(s) rules.

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

I<The queue function parses the passed (sql) text file and build the list
of sql statements to be executed and how.>

queue B<arguments>

=over 3

=item L<$sql_file|/"$sql_file">

=back

queue B<usage and syntax>

    $db->queue($sql_file);
    
    takes 1 argument
        1st argument  - required
            $sql_file - path to the sql file to process
            
    example:
    $db->queue($sql_file);

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

I<The process_queue function sequentially processes the recorded commands
found the (sql) text file.>

process_queue B<arguments>

=over 3

=item L<@parameters|/"@parameters">

=back

process_queue B<usage and syntax>

    $self->process_queue(@parameters);
    
    takes 1 argument
        1st argument    - required
            @parameters - parameters to be used in parsing the sql file
            
    example:
    $db->process_queue(@parameters);
    $db->process_queue($hashref, @parameters);
    
process_queue B<synonyms>

=over 3

=item * process

=back

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

# process_queue synonym

sub process {
    shift->process_queue(@_);
}

# The sets method provides direct access to the resultsets array or
# resultsets.

sub sets {
    return shift->{sets};
}

=head2 cache

I<The cache method accesses an arrayref of resultsets that were captured
using the (sql file) capture command and returns the resultset of the
index or name passed to it or returns 0.>

cache B<arguments>

=over 3

=item L<$index|/"$index">

=back

cache B<usage and syntax>

    my $results = $db->cache($index);
    
    takes 1 argument
        1st argument  - required
            $index    - name or array index of the desired resultset
            
    example:
    my $resultset = $db->cache('new_group');
    my $resultset = $db->cache(2);

cache B<synonyms>

=over 3

=item * rs

=back

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

# The rs method is a synonym for the cache method

sub rs {
    return shift->cache(@_);
}

# The command method is used to queue a command to be processed later by the # # # process_queue method. Takes two arguments, "command" and "sql statement",
# e.g. command('execute', 'select * from foo').

sub command {
    my ($self, $command, $statement) = @_;
    my @statements = @{$self->{cmds}};
    push @statements, { "command" => "$command", "statement" => "$statement" };
    $self->{cmds} = \@statements;
    return $self;
}

# The process_command method allows you to process the indexed sql
# satements from your sql file individually. It take two argument, the
# index of the command as it is encountered in the sql file and tries
# returns a resultset, and any parameters that need to be passed to it.

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

I<The clear method simply clears the cache (the resultset space).>

clear B<arguments>

No arguments.

clear B<usage and syntax>

    $db->clear;
    
    takes 0 arguments
            
    example:
    $db->clear

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
