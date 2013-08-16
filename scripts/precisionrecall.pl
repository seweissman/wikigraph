if($#ARGV < 0){
    die "Usage: precisionrecall.pl <namefile>\n";
}

$vectorfile = $ARGV[0];
open(VECTORIN,"<$vectorfile");

$total = 832+1798;
$goodct = 0;
$allct = 0;
while($line = <VECTORIN>){
    #$line =~ /[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,(.*)$/;
    chomp $line;
    $name = $line;
    if($name =~ /(B|b)(O|o)(T|t)/){
	#print "BOT $name\n";
	$goodct += 1;
    }else{
	#print "NONBOT $name\n";
    }
    $allct += 1;
    $recall = $goodct/$total;
    $precision = $goodct/$allct;
    print "$recall $precision\n";

}

#print "$allct\t$goodct\n";
close(VECTORIN);

