
if($#ARGV < 2){
    die "Usage: findextremes.pl <filein> <mapin> <outlierout>\n";
}
open(FILEIN,"<$ARGV[0]");
open(MAPIN,"<$ARGV[1]");
open(OUTLIERSOUT,">$ARGV[2]");

print "outlier file = $ARGV[2]\n";

my %namemap;
while($line = <MAPIN>){
    chomp $line;
    $line =~ /(\d+)\t(.*)$/;
    $id = $1;
    $name = $2;
    #print "id=$id,name=$name\n";
    $namemap{$id} = $name;
}
close(MAPIN);


while($line = <FILEIN>){
    chomp $line;
    if($line =~ /@/ || $line eq ""){
	next;
    }
    @line = split(",",$line);
    $id = $line[0];
    $name = $namemap{$id};

# nedits errors
#    $predicted = $line[1];
#    $actual = $line[2];
#    $narticles = $line[3];

#pctadminerrors
#    $pctarticle = $line[1];
#    $predicted = $line[2];
#    $actual = $line[3];

#articlesnserrors
    $nedits = $line[1];
    $predicted = $line[2];
    $actual = $line[3];

    #print "$id,$nedits,$predicted,$narticles\n";
    $diff = $predicted - $actual;
    $absdiff = abs($diff);
    if($predicted == 0){
	$pct = 0;
    }else{
	$pct = abs($diff)/$predicted;
    }
    print OUTLIERSOUT "$pct,$diff,$absdiff,$line,$name\n";

}
close(MAPIN);
close(FILEIN);
close(OUTLIERSOUT);
