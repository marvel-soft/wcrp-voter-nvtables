#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# wcrp-voter-tables
#
#
#
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#use strict;
use warnings;
$| = 1;
use File::Basename;
use DBI;
use Data::Dumper;
use Getopt::Long qw(GetOptions);
use Time::Piece;
use Math::Round;

no warnings "uninitialized";


=head1 Function
=over
=head2 Overview
	This program will analyze a washoe-county-voter file
		a) file is sorted by precinct ascending
		b)
	Input: county voter registration file.
	       
	Output: a csv file containing the extracted fields 
=cut

my $records;
#my $inputFile 				= "../test-in/2019.nv.VoterList.ElgbVtr-250-low.csv";    
#my $inputFile 				= "../test-in/nv-washoe-voter-20190219.csv";    
#my $inputFile 				= "../wcrp-voter-splitter/VoterList.ElgbVtr.20190327-1.csv";

my $inputFile 				= "../prod-load-washoe/VoterList.washoe.ElgbVtr-20190218.csv";
my $voterStatsFile    = "../prod-load-washoe/sorted-voters-2019 1st Free List 1.7.19.csv";

#my $inputFile 				= "base-nvtables-merge-test.csv";    #
#my $voterStatsFile    = "sorted-extracts-test.csv";
my $voterEmailFile    = "../prod-load-washoe/email-sort.csv";

my $voterEmailFileh;
my $voterStatsFileh;

my $adPoliticalFile 	= "../prod-in1/adall-precincts-jul.csv";

my $fileName         = "";
my $baseFile         = "base.csv";
my $baseFileh;
my %baseLine         = ();
my $emailLogFile      = "email-adds-log.csv";
my $emailLogFileh;
my %emailLine         = ();
my $printFile        = "print-.txt";
my $printFileh;
my $votingFile       = "voting.csv";
my $votingFileh;
my %votingLine       = ();
my %politicalLine       = ();


my @adPoliticalHash = ();
my %adPoliticalHash;
my $adPoliticalHeadings = "";
my @voterEmailArray;
my $voterEmailArray;
my @voterStatsArray;
my $voterStatsArray;
my $voterStatsHeadings = "";
my @voterStatsHeadings;
my $stats;
my $emails;

my $helpReq            = 0;
my $maxLines           = "25000";
my $voteCycle          = "";
my $fileCount          = 1;
my $csvHeadings        = "";
my @csvHeadings;
my $line1Read       = '';
my $linesRead       = 0;
my $linesIncRead    = 0;
my $printData;
my $linesWritten    = 0;
my $emailAdded      = 0;
my $statsAdded      = 0;

my $selParty;
my $skipRecords     = 20;
my $skippedRecords  = 0;

my $generalCount;
my $party;
my $primaryCount;
my $pollCount;
my $absenteeCount   = 0;
my $leansRepCount   = 0;
my $leansDemCount   = 0;
my $leanRep         = 0;
my $leanDem         = 0;
my $leans           = "";
my $activeVOTERS    = 0;
my $activeREP       = 0;
my $activeDEM       = 0;
my $activeOTHR      = 0;
my $totalVOTERS     = 0;
my $totalAMER       = 0;
my $totalAMEL       = 0;
my $totalDEM        = 0;
my $totalDUO        = 0;
my $totalFED        = 0;
my $totalGRN        = 0;
my $totalIA         = 0;
my $totalIAP        = 0;
my $totalIND        = 0;
my $totalINAC       = 0;
my $totalLIB        = 0;
my $totalLPN        = 0;
my $totalNL         = 0;
my $totalNP         = 0;
my $totalORGL       = 0;
my $totalOTH        = 0;
my $totalPF         = 0;
my $totalPOP        = 0;
my $totalREP        = 0;
my $totalRFM        = 0;
my $totalSOC        = 0;
my $totalTEANV      = 0;
my $totalUWS        = 0;
my $totalGENERALS   = 0;
my $totalPRIMARIES  = 0;
my $totalPOLLS      = 0;
my $totalABSENTEE   = 0;
my $totalSTRDEM     = 0;
my $totalMODDEM     = 0;
my $totalWEAKDEM    = 0;
my $percentSTRGRDEM = 0;
my $totalSTRREP     = 0;
my $totalMODREP     = 0;
my $totalWEAKREP    = 0;
my $percentSTRGREP  = 0;
my $totalSTROTHR    = 0;
my $totalMODOTHR    = 0;
my $totalWEAKOTHR   = 0;
my $percentSTRGOTHR = 0;
my $totalOTHR       = 0;
my $totalLEANREP    = 0;
my $totalLEANDEM    = 0;

my @csvRowHash;
my %csvRowHash = ();
my @partyHash;
my %partyHash  = ();
my %schRowHash = ();
my @schRowHash;
my @values1;
my @values2;
my @date;
my $voterRank;

my $calastName;
my $cafirstName;
my $camiddleName;
my $caemail;
my $capoints;

my @baseLine;
my $baseLine;
my @baseProfile;
my $baseHeading = "";
my @baseHeading = (
	"Voter ID",       "State ID",     
	"Status",        	"Precinct", 
	"Asm dist",       "Sen dist",
  "First",          "Last",
	"Middle",         "Suffix",  
	"Phone",         	"email",
  "Birth Date",     "Reg Date",  
	"Party", 					
	"Street No",      "Street Name",
	"Address 1",      "Address 2",
	"City",           "State",
	"Zip", 			
	"Reg Date Orig",  "Days Totl Reg",
	"Age",
	"Gender",         "Military",    
	"Generals", 			"Primaries",			
	"Leans",          "Strength",
);
my @emailProfile;
my $emailHeading = "";
my @emailHeading = (
	"Voter ID",       "Precinct",     
  "First",          "Last", 	"Middle",
	"email",
);

my @votingLine;
my $votingLine;
my @votingProfile;
my $votingHeading = "";
my @votingHeading = (
	"state_id",     "voter_id",
	"publish_date", 
	"party",      
	"election01",   "vote01",  	
	"election02",	"vote02",
	"election03",   "vote03",    
	"election04",	"vote04",
	"election05",   "vote05",    
	"election06",	"vote06",
  "election07", 	"vote07", 	
	"election08",	"vote08",
	"election09",   "vote09",   
	"election10",	"vote10",
	"election11",   "vote11",   
	"election12", 	"vote12",
	"election13",   "vote13",   
	"election14",	"vote14",
	"election15",   "vote15",    
	"election16",	"vote16", 
	"election17",   "vote17",    
	"election18",	"vote18",
	"election19",   "vote19",    
	"election20",	"vote20"
	);

my $precinct = "000000";

#
# main program controller
#
sub main {
	#Open file for messages and errors
	open( $printFileh, ">$printFile" )
	  or die "Unable to open PRINT: $printFile Reason: $!";

	# Parse any parameters
	GetOptions(
		'infile=s'  => \$inputFile,
		'outile=s'  => \$baseFile,
		'statfile=s'  => \$voterStatsFile,
		'emailfile=s'  => \$voterEmailFile,
		'skip=i'    => \$skipRecords,
		'lines=s'   => \$maxLines,
		'votecycle' => \$voteCycle,
		'help!'     => \$helpReq,
	) or die "Incorrect usage!\n";
	if ($helpReq) {
		print "Come on, it's really not that hard.\n";
	}
	else {
		printLine ("My inputfile is: $inputFile.\n");
	}
	unless ( open( INPUT, $inputFile ) ) {
		printLine ("Unable to open INPUT: $inputFile Reason: $!\n");
		die;
	}

	# pick out the heading line and hold it and remove end character
	$csvHeadings = <INPUT>;
	chomp $csvHeadings;

	# headings in an array to modify
	# @csvHeadings will be used to create the files
	@csvHeadings = split( /\s*,\s*/, $csvHeadings );

	# Build heading for new voter record
	$baseHeading = join( ",", @baseHeading );
	$baseHeading = $baseHeading . "\n";

	# Build heading for new voting record
	$votingHeading = join( ",", @votingHeading );
	$votingHeading = $votingHeading . "\n";	

	# Build heading for new voting record
	$emailHeading = join( ",", @emailHeading );
	$emailHeading = $emailHeading . "\n";	
	#
	# Initialize process loop and open files
	printLine ("Voter Base-table file: $baseFile\n");
	open( $baseFileh, ">$baseFile" )
	  or die "Unable to open baseFile: $baseFile Reason: $!";
	print $baseFileh $baseHeading;

	printLine ("Voter Voting-table file: $votingFile\n");
	printLine ("My votingFile is: $votingFile.\n");
	open( $votingFileh, ">$votingFile" )
	  or die "Unable to open votingFileh: $votingFile Reason: $!";
	print $votingFileh $votingHeading;


	# initialize the voter email log and the email array
	if ($voterEmailFile ne "") {
		printLine ("My emailFile is: $voterEmailFile.\n");
		printLine ("Email updates file: $voterEmailFile\n");
		open( $emailLogFileh, ">$emailLogFile" )
			or die "Unable to open emailLogFileh: $emailLogFile Reason: $!";
		print $emailLogFileh $emailHeading;
		voterEmailLoad(@voterEmailArray);
	}

	if ($voterStatsFile ne "") {
		printLine ("Voter Stats file: $voterStatsFile\n");
		voterStatsLoad(@voterStatsArray);
	}
	# Process loop
	# Read the entire input and
	# 1) edit the input lines
	# 2) transform the data
	# 3) write out transformed line
  NEW:
	while ( $line1Read = <INPUT> ) {
		$linesRead++;
		$linesIncRead++;
		if ($linesIncRead == 5000) {
			printLine ("$linesRead lines processed\n");
			$linesIncRead = 0;
		}
		#
		# Get the data into an array that matches the headers array
		chomp $line1Read;

		# replace commas from in between double quotes with a space
		$line1Read =~ s/(?:\G(?!\A)|[^"]*")[^",]*\K(?:,|"(*SKIP)(*FAIL))/ /g;
		$line1Read =~ s/\"/ /g;
		chop $line1Read;


		# then create the values array
		@values1 = split( /\s*,\s*/, $line1Read, -1 );

		# Create hash of line for transformation
		@csvRowHash{@csvHeadings} = @values1;

		#- - - - - - - - - - - - - - - - - - - - - - - - - - 
		# Assemble database load  for base segment
		#- - - - - - - - - - - - - - - - - - - - - - - - - - 
		%baseLine = ();
		$baseLine{"State ID"}     = $csvRowHash{"nv_id"};
		$baseLine{"Voter ID"}     = $csvRowHash{"cnty_id"};
		my $voterid               = $csvRowHash{"cnty_id"};
		$baseLine{"Status"}       = $csvRowHash{"status"};
		$baseLine{'Asm dist'}     = $csvRowHash{"am_dist"};
		$baseLine{'Sen dist'}     = $csvRowHash{"sen_dist"};
	  $baseLine{"Precinct"}     = substr $csvRowHash{"precinct"}, 0, 6;
    my $UCword                = $csvRowHash{"first"};
		$UCword  =~ s/(\w+)/\u\L$1/g;
	  $baseLine{"First"}        = $UCword; 
		my $ccfirstName = $UCword;

    $UCword                   = $csvRowHash{"middle"};
		$UCword  =~ s/(\w+)/\u\L$1/g;
		$baseLine{"Middle"}       = $UCword;;
    $UCword                   = $csvRowHash{"last"};
		$UCword  =~ s/(\w+)/\u\L$1/g;
		$baseLine{"Last"}         = $UCword;
		my $cclastName = $UCword;
		$UCword  =~ s/(\w+)/\u\L$1/g;
		$baseLine{"Suffix"}       = $csvRowHash{"name_suffix"};
		$baseLine{"Party"}        = $csvRowHash{"party"};
		$baseLine{"Phone"}        = $csvRowHash{"phone"};
		$UCword                   = $csvRowHash{"address"};
		$UCword  =~ s/(\w+)/\u\L$1/g;
		$baseLine{"Address 1"}    = $UCword;
		my @streetno = split(/ /, $UCword, 2);
		$baseLine{"Street No"}    = $streetno[0];
		$baseLine{"Street Name"}  = $streetno[1];
		$UCword                   = $csvRowHash{"city"};
		$UCword  =~ s/(\w+)/\u\L$1/g;
		$baseLine{"City"}         = $UCword;
		$baseLine{"State"}        = $csvRowHash{"state"};
		$baseLine{"Zip"}          = $csvRowHash{"zip"};
		$baseLine{"email"}        = "";
		my $date = localtime->mdy('-');
		@date = split( /\s*\/\s*/, $csvRowHash{"birth_date"}, -1 );
		$mm = sprintf( "%02d", $date[0] );
		$dd = sprintf( "%02d", $date[1] );
		$yy = sprintf( "%02d", $date[2] );
		$baseLine{"Birth Date"}   = "$mm/$dd/$yy";
		@date = split( /\s*\/\s*/, $csvRowHash{"reg_date"}, -1 );
		$mm = sprintf( "%02d", $date[0] );
		$dd = sprintf( "%02d", $date[1] );
		$yy = sprintf( "%02d", $date[2] );
		$baseLine{"Reg Date"}    = "$mm/$dd/$yy";

		my $adjustedDate = "$mm/$dd/$yy";
		my $before;
		if (length($yy) == 4) {;
		   $before = Time::Piece->strptime( $adjustedDate, "%m/%d/%Y" );		
		}
		if (length($yy) == 2)   {
			 $before = Time::Piece->strptime( $adjustedDate, "%m/%d/%y" );		
		}
		my $now            = localtime;
		my $daysRegistered = $now - $before;
		$daysRegistered = ( $daysRegistered / (86400) );
		$daysRegistered = round($daysRegistered);
	#	printLine ("line= $linesRead voterid= $voterid adjusted= $adjustedDate registered= $daysRegistered\n");
		$baseLine{"Days Reg"}     = int($daysRegistered);
		#
		#  locate county data
		#	
		$stats = binary_search(\@voterStatsArray, $voterid);
		if ($stats != -1) {
	  	$baseLine{"Gender"}       = $voterStatsArray[$stats][15]; 
			my $mil = $voterStatsArray[$stats][16];
			chop $mil;
			$baseLine{"Military"}     = $mil;
			$baseLine{"Generals"}     = $voterStatsArray[$stats][7];
			$baseLine{"Primaries"}    = $voterStatsArray[$stats][8];
			$baseLine{"Leans"}        = $voterStatsArray[$stats][13];
			$baseLine{"Strength"}     = $voterStatsArray[$stats][14];
			$baseLine{"Reg Date Orig"}  = $voterStatsArray[$stats][4];
			$baseLine{"Days Totl Reg"}  = $voterStatsArray[$stats][5];
			$baseLine{"Age"}          = $voterStatsArray[$stats][6];
			$statsAdded = $statsAdded + 1;
		}
		#
		#  locate email address
		#  "Last", "First", "Middle","Phone","email","Address", "City","Contact Points",
		#     0       1         2                4      5          6          7

		$emails = binary_ch_search(\@voterEmailArray, $cclastName);
		if ($emails != -1) {
	  	if ( $voterEmailArray[$emails][0] eq $cclastName && 
			     $voterEmailArray[$emails][1] eq $ccfirstName) {
    			 	$calastName 				= $voterEmailArray[$emails][0];	
    				$cafirstName 				= $voterEmailArray[$emails][1];
    				$caemail 						= $voterEmailArray[$emails][4];
 		 				$baseLine{"email"}  = $voterEmailArray[$emails][4];
 		 				$capoints 					= $voterEmailArray[$emails][7];
						$capoints 					=~ s/;/,/g;
        		#printLine (" email added: $calastName $camiddleName $caemail \n");
						$emailAdded = $emailAdded + 1;
						# build a trace line to show email was updated
						%emailLine = ();
						$emailLine{"Voter ID"} = $voterid;
					  $emailLine{"Precinct"} = substr $csvRowHash{"precinct"}, 0, 6;
						$emailLine{"Last"} = $calastName;
						$emailLine{"First"} = $cafirstName;
						$emailLine{"email"} = $caemail;
						@emailProfile = ();
						foreach (@emailHeading) {
							push( @emailProfile, $emailLine{$_} );
						}
						print $emailLogFileh join( ',', @emailProfile ), "\n";
				} 
		}
	
		
		@baseProfile = ();
		foreach (@baseHeading) {
			push( @baseProfile, $baseLine{$_} );
		}
		print $baseFileh join( ',', @baseProfile ), "\n";
	
		$linesWritten++;
		#
		# For now this is the in-elegant way I detect completion
		if ( eof(INPUT) ) {
			goto EXIT;
		}
		next;
	}
	#
	goto NEW;
}
#
# call main program controller
main();
#
# Common Exit
EXIT:

printLine ("<===> Completed transformation of: $inputFile \n");
printLine ("<===> BASE      SEGMENTS available in file: $baseFile \n");
printLine ("<===> VOTING    SEGMENTS available in file: $votingFile \n");
printLine ("<===> Total Records Read: $linesRead \n");
printLine ("<===> Total Emails added: $emailAdded \n");
printLine ("<===> Total Stats  added: $statsAdded \n");
printLine ("<===> Total Records written: $linesWritten \n");

close(INPUT);
close($baseFileh);
close($votingFileh);
close($printFileh);
close($emailLogFileh);
exit;


#
# Print report line
#
sub printLine  {
	my $datestring = localtime();
	($printData) = @_;
	print $printFileh $datestring . ' ' . $printData;
	print $datestring . ' ' . $printData;
}

# $index = binary_search( \@array, $word )
#   @array is a list of lowercase strings in alphabetical order.
#   $word is the target word that might be in the list.
#   binary_search() returns the array index such that $array[$index]
#   is $word.	
sub binary_search {
	  my ($try, $var);
    my ($array, $word) = @_;
    my ($low, $high) = ( 0, @$array - 1 );
    while ( $low <= $high ) {              # While the window is open
        $try = int( ($low+$high)/2 );      # Try the middle element
				$var = $array->[$try][0];
        $low  = $try+1, next if $array->[$try][0] < $word; # Raise bottom
        $high = $try-1, next if $array->[$try][0] > $word; # Lower top
        return $try;     # We've found the word!
    }
		$try = -1;
    return;              # The word isn't there.
}
#
# binay search for character strings
#
sub binary_ch_search {
	  my ($try, $var);
    my ($array, $word) = @_;
    my ($low, $high) = ( 0, @$array - 1 );
    while ( $low <= $high ) {              # While the window is open
        $try = int( ($low+$high)/2 );      # Try the middle element
				$var = $array->[$try][0];
        $low  = $try+1, next if $array->[$try][0] lt $word; # Raise bottom
        $high = $try-1, next if $array->[$try][0] gt $word; # Lower top
        return $try;     # We've found the word!
    }
		$try = -1;
    return;              # The word isn't there.
}


#
# open and prime next file
#
sub preparefile {
	print "New output file: $baseFile\n";
	open( baseFileh, ">$baseFile" )
	  or die "Unable to open output: $baseFile Reason: $!";
	print baseFileh $baseHeading;
}

#
# count party memebers
#
sub countParty {
	$party = $csvRowHash{"party"};
	$totalVOTERS++;

	if ( $csvRowHash{"status"} eq "A" ) {
		$activeVOTERS++;
		if    ( $party eq 'REP' ) { $activeREP++; }
		elsif ( $party eq 'DEM' ) { $activeDEM++; }
		else                      { $activeOTHR++; }
	}
	if    ( $party eq 'AMEL' )  { $totalAMEL++; }
	elsif ( $party eq 'AMER' )  { $totalAMER++; }
	elsif ( $party eq 'DEM' )   { $totalDEM++; }
	elsif ( $party eq 'DUO' )   { $totalDUO++; }
	elsif ( $party eq 'FED' )   { $totalFED++; }
	elsif ( $party eq 'GRN' )   { $totalGRN++; }
	elsif ( $party eq 'IA' )    { $totalIA++; }
	elsif ( $party eq 'IAP' )   { $totalIAP++; }
	elsif ( $party eq 'IND' )   { $totalIND++; }
	elsif ( $party eq 'IN AC' ) { $totalINAC++; }
	elsif ( $party eq 'LIB' )   { $totalLIB++; }
	elsif ( $party eq 'LPN' )   { $totalLPN++; }
	elsif ( $party eq 'NL' )    { $totalNL++; }
	elsif ( $party eq 'NP' )    { $totalNP++; }
	elsif ( $party eq 'ORG L' ) { $totalORGL++; }
	elsif ( $party eq 'OTH' )   { $totalOTH++; }
	elsif ( $party eq 'PF' )    { $totalPF++; }
	elsif ( $party eq 'POP' )   { $totalPOP++; }
	elsif ( $party eq 'REP' )   { $totalREP++; }
	elsif ( $party eq 'RFM' )   { $totalRFM++; }
	elsif ( $party eq 'SOC' )   { $totalSOC++; }
	elsif ( $party eq 'TEANV' ) { $totalTEANV++; }
	elsif ( $party eq 'UWS' )   { $totalUWS++; }
}
#
# calculate percentage
#sub percentage {
#	my $val = $_;
#	return ( sprintf( "%.2f", ( $- * 100 ) ) . "%" . $/ );
#}
#
# create the voter stats binary search array
#
sub voterStatsLoad() {
	$voterStatsHeadings = "";
	open( $voterStatsFileh, $voterStatsFile )
	  or die "Unable to open INPUT: $voterStatsFile Reason: $!";
	$voterStatsHeadings = <$voterStatsFileh>;
	chomp $voterStatsHeadings;

	# headings in an array to modify
	@voterStatsHeadings = split( /\s*,\s*/, $voterStatsHeadings );

	# Build the UID->survey hash
	while ( $line1Read = <$voterStatsFileh> ) {
		chomp $line1Read;
		my @values1 = split( /\s*,\s*/, $line1Read, -1 );
		push @voterStatsArray , \@values1;
	}
	close $voterStatsFileh;
	return @voterStatsArray;
}

#
# create the voter email binary search array
#
sub voterEmailLoad() {
	$voterEmailHeadings = "";
	open( $voterEmailFileh, $voterEmailFile )
	  or die "Unable to open INPUT: $voterEmailFile Reason: $!";
	$voterEmailHeadings = <$voterEmailFileh>;
	chomp $voterEmailHeadings;

	# headings in an array to modify
	@voterEmailHeadings = split( /\s*,\s*/, $voterEmailHeadings );
  my $emailCount = 0;
	# Build the UID->survey hash
	while ( $line1Read = <$voterEmailFileh> ) {
		chomp $line1Read;
		my @values1 = split( /\s*,\s*/, $line1Read, -1 );
		push @voterEmailArray , \@values1;
		$emailCount = $emailCount + 1;
	}
	close $voterEmailFileh;
	printLine("email array: $emailCount");
	return @voterEmailArray;
}