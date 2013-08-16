
#$header = "id,nedits,narticles,daysactive,naddedits,nremoveedits,bytesadded,bytesremoved,avgeditsday,avgarticlesday,avgtimetonext,avgbytesadded,avgbytesremoved,netbytes,avgnetbytes";
if($#ARGV < 2){
    die "Usage: findoutliers.pl <vectorfile> <namefile> <out-prefix>\n";
}

$vectorfile = $ARGV[0];
$namefile = $ARGV[1];
$fileout = $ARGV[2];
$outlierfile = "$fileout.outlier";
$restfile = "$fileout.rest";
print "vector file = $vectorfile\n";
print "outlier file = $outlierfile\n";

open(VECTORIN,"<$vectorfile");
open(OUTLIEROUT,">$outlierfile");
open(RESTOUT,">$restfile");
open(MAPIN,"<$namefile");


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

#99 limits
my @limits;
#$llimit[0] = -1;
#$llimits[1] = 2350; #nedits
#$llimits[2] = 1750; #narticles
$llimits[3] = 3; #daysactive
#$llimits[4] = 2400; #naddedits
#$llimits[5] = 30; #nremoveedits
#$llimits[6] = 700000; #bytesadded
#$llimits[7] = 40000; #bytesremoved
#$llimits[8] = 1.6; #avgeditsday
#$llimits[9] = 1.5; #avgarticlesday
#$llimits[10] = 466; #avgtimestonext
#$llimits[11] = 5900; #avgbytesadded
#$llimits[12] = 3800; #avgbytesremoved
$llimits[13] = -500; #netbytes
$llimits[14] = -100; #avgnetbytes
#$llimits[17] = .1; #pctarticlensedits

$ulimit[0] = -1;
$ulimits[1] = 2350; #nedits
$ulimits[2] = 1750; #narticles
#$ulimits[3] = 3; #daysactive
$ulimits[4] = 2400; #naddedits
$ulimits[5] = 30; #nremoveedits
$ulimits[6] = 700000; #bytesadded
$ulimits[7] = 40000; #bytesremoved
$ulimits[8] = 1.6; #avgeditsday
$ulimits[9] = 1.5; #avgarticlesday
$ulimits[10] = 466; #avgtimestonext
$ulimits[11] = 5900; #avgbytesadded
$ulimits[12] = 3800; #avgbytesremoved
$ulimits[13] = 600000; #netbytes
$ulimits[14] = 5600; #avgnetbytes
$ulimits[15] = 2000; #articlensedits
$ulimits[16] = 350; #adminnsedits
#$ulimits[17] = .99; #pctarticlensedits
#$ulimits[18] = .75; #pctadminnsedits

# #95 limits
# my @limits;
# $limit[0] = -1;
# $limits[1] = 225; #nedits
# $limits[2] = 200; #narticles
# $limits[3] = 6; #daysactive
# $limits[4] = 200; #naddedits
# $limits[5] = 3; #nremoveedits
# $limits[6] = 40000; #bytesadded
# $limits[7] = 1250; #bytesremoved
# $limits[8] = .67; #avgeditsday
# $limits[9] = .52; #avgarticlesday
# $limits[10] = 188; #avgtimestonext
# $limits[11] = 2100; #avgbytesadded
# $limits[12] = 400; #avgbytesremoved
# $limits[13] = 40000; #netbytes
# $limits[14] = 2000; #avgnetbytes

$header = 1;

while ($line = <VECTORIN>){
    chomp $line;
    if($header){
	print OUTLIERSOUT "$line,outlier,name\n";
	print RESTOUT "$line,outlier,name\n";
	$header = 0;
    }
    $outlier = 0;
    @line = split(",",$line);
    $id = $line[0];
    $name = $namemap{$id};
    for($i=1;$i<=$#line;$i++){
	if($llimits[$i] && $line[$i] < $llimits[$i]){
	    $outlier += 1;
	}
    }

    for($i=1;$i<=$#line;$i++){
	if($ulimits[$i] && $line[$i] > $ulimits[$i]){
	    $outlier += 1;
	}
    }

    if($outlier){
	print OUTLIEROUT "$line,$outlier,$name\n";
    }else{
	print RESTOUT "$line,$outlier,$name\n";
    }

}

close(OUTLIEROUT);
close(RESTOUT);
close(VECTORIN);
