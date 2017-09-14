# Выбирает из таблицы Bank урлы и если они числовые или состоят не из букв,цифр и подчеркиваний
# генерирует новый урл из title. Так же проверяется чтоб новый урл был уникальным.
BEGIN {
	$ENV{'SITE_ROOT'}='./..';
	unshift @INC,join('', $ENV{'SITE_ROOT'}, '/lib');
	unshift @INC,join('', $ENV{'SITE_ROOT'}, '/sys');
}

use strict;
use DBI::Conn1;
use Basic;
use encoding 'utf8';
use open qw/:std :encoding(cp866)/;

my $dbh=DBI::Conn1::dbh;

# извлекаем из таблицы Unite урлы и присваиваем их как ключи хэшу.
my $sth1=$dbh->prepare(<<'_QUERY_') or die $dbh->errstr;
SELECT
    url
FROM
  `Unite`
ORDER BY
    `url`    
_QUERY_

$sth1->execute() or die $dbh->errstr;
my $unite=$sth1->fetchall_arrayref();
$sth1->finish;
my %uh = map {$_->[0],1} @$unite;
 

my $sth2=$dbh->prepare(<<'_QUERY_') or die $dbh->errstr;
SELECT
    uid,url,title
FROM
  `Bank`
ORDER BY
    `url`    
_QUERY_

$sth2->execute() or die $dbh->errstr;
my $result=$sth2->fetchall_arrayref({});
$sth2->finish;

my $count=0;
foreach my $hr (@$result) 
{
  next if ($hr->{'url'} =~ /^[a-z0-9_-]+$/i && $hr->{'url'} !~ /^\d+$/i && $hr->{'url'} !~ /^['"]+$/i);
  my $new_url = lc Basic::transliteration2($hr->{'title'});
        
  START: 
  {      
    last unless (exists $uh{$new_url}); 
    $new_url .='_';    
    redo;                    
  }    
      $uh{$new_url} = 1;      
      $dbh->do('UPDATE `Bank` SET url=? WHERE uid=?',undef,$new_url,$hr->{'uid'}) or die $dbh->errstr;
      $dbh->do('UPDATE `Unite` SET url=? WHERE uid=?',undef,$new_url,$hr->{'uid'}) or die $dbh->errstr;
      $count++
}
printf('Заменено url: %s',$count) if $count;





   




