#!/usr/bin/perl

BEGIN
{
	#unshift @INC,'/d1/www//site/lib/Perl5';
	#unshift @INC,'/usr/www/gazeta2/lib/Perl5';
	unshift @INC,'/home/www/gazeta.spb.ru-3/lib/Perl5';
}

#$ENV{'VAR_ROOT'} = '/d1/www/prohotel/var';
#$ENV{'VAR_ROOT'} = '/usr/www/gazeta2/var';
$ENV{'VAR_ROOT'} = '/home/www/gazeta.spb.ru-3/var';

use strict;
use CGI::Log;

require Feedback;
require DBI_conn1;
require comvars;
require 'libsqlfile.pl';
require 'lib1.ru.pl';


#------------------------------------------------------


my $dbh=DBI_conn1::db_handle();
my $comvars = comvars::load('001');


#------------------------------------------------------


local *LOG_1;
open(LOG_1,join('','>>',$ENV{'VAR_ROOT'},'/ru/log/','cron_delivery_sys','.log')) or die("Can't open file: $^E\n");
flock LOG_1,2;
select((select(LOG_1), $|=1)[0]);

local *LOG_2;
open(LOG_2,join('','>',$ENV{'VAR_ROOT'},'/ru/log/','cron_delivery_user','.log')) or die("Can't open file: $^E\n");
flock LOG_2,2;
select((select(LOG_2), $|=1)[0]);

my $log=new CGI::Log;
$log->log_format('compile_user',
	sub
	{
		my($bit,$r_string,$r_caller)=@_;

		sprintf("[%s] [%s] %s\n",
			time2sqlstr(time(),2),
			('emergency','alert','critical','error','warning','notice','info','debug')[$bit],
			$$r_string
		);
	}
);

$log->register_fh(*LOG_1,255,'common');
$log->register_fh(*LOG_2,255,'compile_user');


#------------------------------------------------------


$log->info('Получение новостей за прошедшие сутки ...');

my $sth=$dbh->prepare(<<'_QUERY_') or die $dbh->errstr;
SELECT
	A.i_id,
	A.timestamp,
	B.title,
	B.file_a
FROM
	Ru_T1_A AS A
	LEFT JOIN Ru_T1_B AS B ON (A.r_id=B.r_id)
WHERE
	B.profile_id IN ('event','news')
	AND A.set_b1 <= 1
	AND A.timestamp > UNIX_TIMESTAMP()-86400
ORDER BY
	A.timestamp DESC
LIMIT
	50
_QUERY_

$sth->execute() or die $dbh->errstr;

unless ($sth->rows)
{
	$log->info('Новостей нет. Рассылка не требуется.');
	exit;
}

my $news = $sth->fetchall_arrayref({});

$sth->finish;


#------------------------------------------------------


$log->info('Получение списка подписчиков ...');


my $user=[];

my $datafile=sprintf('%s/ru/subscribe/list1.txt', $ENV{'VAR_ROOT'});
local *FH;
open(FH, '<'.$datafile) or die "$! : $datafile";
flock FH,1;
while (<FH>)
{
	chop if /\x0A\z/;
	chop if /\x0D\z/;

	next unless m/"([^\x22]*)" <([^)]+)>/;

	push @$user, {
		'user_fullname' => $1,
		'user_email'    => $2
	}
}
close FH;

=pod
my $user=[
	{
		'user_fullname' => 'Тестовая рассылка',
		'user_email'    => 'support@webmaster.spb.ru'
	}
	,{
		'user_fullname' => 'Тестовая рассылка',
		'user_email'    => 'rav@webmaster.spb.ru'
	}
];
=cut

unless (@$user)
{
	$log->info('Подписчиков нет. Рассылка не требуется.');
	exit;
}


#------------------------------------------------------


$log->info('Производим рассылку...');


my $body = <<'_EOL_';
<html lang="ru">
<head>

<meta http-equiv="content-type" content="text/html; charset=Windows-1251">

<style type="text/css">
body, td, th {font-family:Tahoma, Verdana, Arial, sans-serif; font-size:12px; color:#000000;}
p {margin:12px 0;}
dl {margin:0;}
dt {margin:12px 0;}
dd {margin:12px 0 12px 24px;}
a {color:#0000FF; text-decoration:underline;}
h1,h2,h3,h4,h5,h6 {font-size:20px; font-weight:bold; margin:20px 0;}
h2 {font-size:18px;}
h3 {font-size:16px;}
h4 {font-size:14px;}
h5 {font-size:13px;}
h6 {font-size:12px;}
.date {font-size:10px; color:#999999;}
</style>

</head>
<body>
_EOL_

$body .=
	htmlspecialtags($comvars->{'email_header'})
	.'<dl>';

my $tmpl=<<'_EOL_';
<dt><a href="http://www.gazeta.spb.ru/%u-0/">%s</a> <span class="date">%s</span>
<dd>%s
_EOL_

foreach my $hr (@$news)
{
	unpacksqlfile2(\$hr->{'file_a'}, $hr);

	$body .= sprintf($tmpl
		,$hr->{'i_id'}
		,htmlspecialchars($hr->{'title'})
		,time2hrstr_digital($hr->{'timestamp'},1)
		,htmlspecialtags($hr->{'lead'})
	);
}

$body .=
	'</dl>'
	.htmlspecialtags($comvars->{'email_footer'})
	.'</body></html>';


my %h=(
	 'From'         => $comvars->{'email2'}
	,'Subject'      => Feedback::base64($comvars->{'email_subject'})
	,'X-Mailer'     => sprintf('Feedback.pm version %f',$Feedback::VERSION)
	,'Content-Type' => 'text/html; charset=Windows-1251'
	,'Body'         => $body
);


my $n=0;

foreach my $hr (@$user)
{
	$hr->{user_fullname} =~ tr/"<>//d;   ### "
	$hr->{user_email}    =~ tr/"<>//d;   ### "

	$h{'To'}=sprintf(qq|%s <%s>|
			,Feedback::base64($hr->{user_fullname})
			,$hr->{user_email}
	);

	unless (Feedback::sendmail(\%h))
	{
		$log->error(sprintf("[%s] error on send mail to '%s'", scalar(localtime),$hr->{'email'}));
		next;
	}

	$n++;
}

$log->info(sprintf('Рассылка завершена (оправлено писем: %u)',$n));

exit;
