$limit = 5;

if($#ARGV < 1){
    die "Usage: perl samplegraph.pl <ingraph> <outgraph>\n";
}
$filein = $ARGV[0];
$fileout = $ARGV[1];
print "File in: $filein\n";
print "File out: $fileout\n";

open(FILEIN,"<$filein");
open(FILEOUT,">$fileout");

$header=1;
while ($line = <FILEIN>){
    chomp $line;
    if($header){
	$header=0;
	print FILEOUT "$line\n";
	next;
    }
    $r = rand 400;
    if($r < $limit){
	print FILEOUT "$line\n";
    }
}
close(FILEIN);
close(FILEIN);

