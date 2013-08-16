
# in header:  "name,nedits,narticles,daysactive,timetonext,bytesadded,naddedits,bytesremoved,nremoveedits";
$header = "id,nedits,narticles,daysactive,naddedits,nremoveedits,bytesadded,bytesremoved,avgeditsday,avgarticlesday,avgtimetonext,avgbytesadded,avgbytesremoved,netbytes,avgnetbytes,articlensedits,adminnsedits,pctarticlensedits,pctadminnsedits";

$filein = $ARGV[0];
$vectorfile = "$filein.vector";
$namefile = "$filein.name";
print "$filein\n";

open(FILEIN,"<$filein");
open(VECTOROUT,">$vectorfile");
open(NAMEOUT,">$namefile");

print VECTOROUT "$header\n";
$ct = 0;
while ($line = <FILEIN>){
    chomp $line;
    unless($line =~ /(.*)\t\[(\d+),(\d+),\[(.*)\],(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\]$/){
	die "Bad line $line\n";
    }

    #print $line,"\n";
    $name = $1;
    $nedits = $2;
    $narticles = $3;
    $nsmap = $4;
    $daysactive = $5;
    $timetonext = $6;
    $bytesadded = $7;
    $naddedits = $8;
    $bytesremoved = $9;
    $nremoveedits = $10;
    
# ns 0 article
# ns 1 talk
# ns 2 user
# ns 3 user talk
# ns 4 wikipedia
# ns 5 wikipedia talk

    #print "TESTLINE: $name,$nedits;$nsmap;$narticles,$daysactive,$timetonext, $bytesadded,$naddedits,$bytesremoved,$nremoveedits\n";

    if($nsmap =~ /\{0,(\d+)\}/){
	$articlensedits = $1;
    }else{
	$articlensedits = 0;
    }
    $adminnsedits = 0;
    if($nsmap =~ /\{1,(\d+)\}/){
	$adminnsedits += $1;
    }
    if($nsmap =~ /\{2,(\d+)\}/){
	$adminnsedits += $1;
    }
    if($nsmap =~ /\{3,(\d+)\}/){
	$adminnsedits += $1;
    }
    if($nsmap =~ /\{4,(\d+)\}/){
	$adminnsedits += $1;
    }
    if($nsmap =~ /\{5,(\d+)\}/){
	$adminnsedits += $1;
    }
    $pctarticlensedits = $articlensedits/$nedits;
    $pctadminnsedits = $adminnsedits/$nedits;
    #print "PCTS $pctarticlensedits\t$pctadminnsedits\n";

    print NAMEOUT "$ct\t$name\n";

    $avgeditsday = sprintf("%.4f", $nedits/$daysactive);
    $avgarticlesday = sprintf("%.4f", $narticles/$daysactive);
    $avgtimetonext = sprintf("%.4f", $timetonext/$nedits);
    if($naddedits > 0){
    	$avgbytesadded = sprintf("%.4f", $bytesadded/$naddedits);
    }else{
    	$avgbytesadded = 0;
    }
    if($nremoveedits > 0){
    	$avgbytesremoved = sprintf("%.4f", $bytesremoved/$nremoveedits);	
    }else{
    	$avgbytesremoved = 0;
    }

    $netbytes = $bytesadded - $bytesremoved;
    $avgnetbytes = sprintf("%.4f", $netbytes/$nedits);
    

    #print "TESTOUT $ct,$nedits,$narticles,$daysactive,$naddedits,$nremoveedits,$bytesadded,$bytesremoved,";
    #print "$avgeditsday,$avgarticlesday,$avgtimetonext,$avgbytesadded,$avgbytesremoved,$netbytes,$avgnetbytes,";
    #print "$articlensedits,$adminnsedits,$pctarticlensedits,$pctadminnsedits\n";

    print VECTOROUT "$ct,$nedits,$narticles,$daysactive,$naddedits,$nremoveedits,$bytesadded,$bytesremoved,";
    print VECTOROUT "$avgeditsday,$avgarticlesday,$avgtimetonext,$avgbytesadded,$avgbytesremoved,$netbytes,$avgnetbytes,";
    print VECTOROUT "$articlensedits,$adminnsedits,$pctarticlensedits,$pctadminnsedits\n";
    $ct++;

}
close(NAMEOUT);
close(VECTOROUT);
close(FILEIN);
