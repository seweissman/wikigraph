
if($#ARGV < 2){
    die "Usage: findoutliers.pl <vectorfile> <namefile> <outfile>\n";
}

$vectorfile = $ARGV[0];
$namefile = $ARGV[1];
$fileout = $ARGV[2];

open(VECTORIN,"<$vectorfile");
open(FILEOUT,">$fileout");
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

$header = 1;
while ($line = <VECTORIN>){
    chomp $line;
    if($header){
	print FILEOUT "$line,isbot\n";
	$header = 0;
	next;
    }
    @line = split(",",$line);
    $id = $line[0];
    $name = $namemap{$id};
    if($name =~ /(B|b)(O|o)(T|t)/){
	print FILEOUT "$line,bot\n";	
    }else{
	print FILEOUT "$line,nobot\n";	
    }
}

close(FILEOUT);
close(MAPIN);
close(VECTORIN);
