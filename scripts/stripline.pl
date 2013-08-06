#!/opt/local/bin/perl

my $allin = "";
my $input;
my $ct = 0;
while($input = <STDIN>){
    $ct++;
    chomp $input;
    $allin = "$allin $ct $input";
}

print "$allin\n";
