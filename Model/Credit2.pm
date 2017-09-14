package Model::Credit2;

use strict;
use base qw(Model);
use utf8;
use Basic;
use Data::Dumper;
use Cache;

=pod
$Data::Dumper::Useqq = 1;
{ no warnings 'redefine';
    sub Data::Dumper::qquote {
        my $s = shift;
        return "'$s'";
    }
}
=cut

my $dbh=Model::dbh();
my $rate_var_cache=undef; # Значения для переменных процентных ставок

sub list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my @select=();
	my @from=();
	my @where=();
	my $group;
	my $rate_joined;
	my $cache=new Cache();

	my $cache_key_str ='';

	{
		last unless $arg->{'cache'};

		local $Data::Dumper::Indent=0;
		local $Data::Dumper::Purity=1;

		my @sort_arg;
		push @sort_arg, $_, $arg->{$_} for sort { $a cmp $b } keys %$arg;

		$cache_key_str=Dumper(\@sort_arg);

		my $values_list = $cache->get('key'=>$cache_key_str);

		last unless defined $values_list;

		my $out = $values_list->[0];

		$this->{'rows'} = $values_list->[1] if $values_list->[1];

		return $out;
	}

	if (exists $arg->{'parent_uid'})
	{
		push @where,sprintf('A.parent_uid=%u',$arg->{'parent_uid'});
	}
	elsif(exists $arg->{'parent_uid_list'} && ref($arg->{'parent_uid_list'}) eq 'ARRAY' && @{$arg->{'parent_uid_list'}})
	{
		push @where, sprintf('A.`parent_uid` IN(%s)',join(',' => map { sprintf('%u',$_) } @{$arg->{'parent_uid_list'}}));
	}

	if (exists $arg->{'priority'})
	{
		push @where,sprintf('A.priority=%u',$arg->{'priority'});
	}

	if (exists $arg->{'exclude_uid_list'} && ref($arg->{'exclude_uid_list'}) eq 'ARRAY' && @{$arg->{'exclude_uid_list'}})
	{
		push @where, sprintf('A.`uid` NOT IN(%s)',join(',' => map { sprintf('%u',$_) } @{$arg->{'exclude_uid_list'}}));
	}

	if (exists $arg->{'is_hidden'})
	{
		push @where,sprintf('A.is_hidden=%u',$arg->{'is_hidden'});
	}

	if(exists $arg->{'profile'})
	{
		push @where, sprintf('A.`profile`=%s', $dbh->quote($arg->{'profile'}));
	}

	if (exists $arg->{'bank_uid'} && !exists $arg->{'city_uid'})
	{
		push @where,sprintf('A.bank_uid=%u',$arg->{'bank_uid'});
	}

	if(exists $arg->{'uid_list'} && @{$arg->{'uid_list'}})
	{
		push @where,sprintf('A.`uid` IN(%s)', join(',' => map { int($_) } @{$arg->{'uid_list'}}));
	}

	if(exists $arg->{'purpose_id'})
	{
		push @from, sprintf('INNER JOIN Credit2_Purpose_Ref PR3 ON (A.uid=PR3.uid AND PR3.id=%u)',$arg->{'purpose_id'});
	}


	if(exists $arg->{'city_uid'})
	{
		my $bank_uids = ($arg->{'bank_uid'})
			? [int $arg->{'bank_uid'}]
			: $this->base_model()->obj('Bank')->bank_city_list('city_uid' => $arg->{'city_uid'});
		return [] unless @$bank_uids;

		push @where,sprintf('A.bank_uid IN (%s)', join(',' => @$bank_uids));
		# TODO нужно оптимизировать
		push @from,sprintf('INNER JOIN Credit2_Rate AS D ON (A.uid=D.credit_uid AND D.city_uid IN (0,%u))',$arg->{'city_uid'});
		$group++;
		$rate_joined++;
	}

	if(exists $arg->{'rate_sum_less'})
	{
		push @from,		'INNER JOIN `Credit2_Rate` AS D ON(A.`uid`=D.`credit_uid`)' unless $rate_joined;
		push @where,	sprintf('D.`credit_from`<=%u', $arg->{'rate_sum_less'});
		push @where,	sprintf('D.`currency`=%u',$arg->{'rate_currency'}) if exists $arg->{'rate_currency'};

		$rate_joined++;
		$group++;
	}

	if (exists $arg->{'limit'} && not $arg->{'no_calc_pagenav_rows'}) #подсчет кол-ва строк без лимита (для станичной навигации)
	{
		my $from=join(' ',@from);
		my $where=@where ? 'WHERE '.join(' AND ',@where) : '';

		my $query=sprintf(<<'		_QUERY_',($group ? 'DISTINCT' : ''),$this->{'profile'},$from,$where);
		SELECT
			COUNT(%s A.uid)
		FROM
			%s AS A
			%s
		%s
		_QUERY_

		my $sth=$dbh->prepare($query) or die $this->{'dbh'}->errstr;
		$sth->execute or die $this->{'dbh'}->errstr;
		($this->{'rows'})=$sth->fetchrow_array;
		$sth->finish;


		return [] if $this->{'rows'}==0;
	}

	if    ($arg->{'no_select'} eq 'file_ab') { 1; }
	elsif ($arg->{'no_select'} eq 'file_b')  { push @select,'A.file_a'; }
	else                                     { push @select,'A.file_a,A.file_b'; }

	my %addon = map { $_ => 1 } split(',' => $arg->{'addon'});

	if($addon{'length'})
	{
		push @select,'L.total, L.visible, L.new_visible';
		push @from,'LEFT JOIN Length AS L ON (A.uid=L.uid)';
	}

	if($addon{'bank'})
	{
		push @select,'B.title AS bank_title, B.`url` AS bank_url';
		push @from,'LEFT JOIN Bank AS B ON (B.uid=A.bank_uid)';
	}

	if($addon{'parent'})
	{
		push @select, 'P.`title` AS parent_title, P.`url` AS parent_url';
		push @from, 'LEFT JOIN `Unite` AS P ON (A.`parent_uid`=P.`uid`)';
	}

	if($addon{'purpose'})
	{
		push @from, 'LEFT JOIN `Credit2_Purpose_Ref` PR ON(A.`uid`=PR.`uid`) LEFT JOIN `Credit2_Purpose_Lib` PL ON(PR.`id`=PL.`id`)';
		push @select, 'GROUP_CONCAT(DISTINCT PL.`title`) AS purpose_text';

		$group++;
	}

	if($addon{'purpose2'})
	{
		push @from, 'LEFT JOIN `Credit2_Purpose_Ref2` PR2 ON(A.`uid`=PR2.`uid`) LEFT JOIN `Credit2_Purpose_Lib2` PL2 ON(PR2.`id`=PL2.`id`)';
		push @select, 'GROUP_CONCAT(DISTINCT PL2.`title`) AS purpose2_text';

		$group++;
	}

	my %order=(
		'timestamp' =>	['A.timestamp DESC', 'A.timestamp ASC'],
		'title'     =>	['A.title ASC',      'A.title DESC'],
		'seq'       =>	['A.seq DESC',       'A.seq ASC'],
		'rand'		=>  ['RAND()', 'RAND()'],
		'parent_uid'=>	['A.priority DESC,A.parent_uid ASC,A.title ASC', 'A.priority DESC,A.parent_uid DESC,A.title ASC'],
		'priority'=>	['A.priority DESC,A.title ASC', 'A.priority ASC,A.title DESC'],
		'parent-title'=>['P.`title` ASC, A.`parent_uid` ASC, A.`title` ASC', 'P.`title` DESC, A.`parent_uid` DESC, A.`title` DESC']
	);

	my $direction=$arg->{'desc'} ? 1:0;

	my $select=@select ? join(',','',@select) : '';
	my $from=join(' ',@from);
	my $where=@where ? 'WHERE '.join(' AND ',@where) : '';
	my $order=exists $order{$arg->{'order'}} ? $order{$arg->{'order'}}->[$direction] : $order{'timestamp'}->[$direction];
	my $limit='';
	if($arg->{'rand_offset'} && $arg->{'limit'})
	{
		my $offset_max = $this->{'rows'} - $arg->{'limit'};
		$offset_max = 0 if $offset_max < 0;
		$offset_max = $arg->{'rand_offset_max'} if $arg->{'rand_offset_max'} > 0 && $offset_max > $arg->{'rand_offset_max'};
		$limit = sprintf(' LIMIT %u,%u', rand($offset_max), $arg->{'limit'});
	}
	elsif($arg->{'limit'})
	{
		$limit = sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'});
	}
	#exists $arg->{'limit'} ? sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'}) : '';
	my $group_by = $group ? 'GROUP BY A.`uid`' : '';

	my $query=sprintf(<<'	_QUERY_',$select,$this->{'profile'},$from,$where,$group_by,$order,$limit);
	SELECT
		A.uid,
		A.parent_uid,
		A.timestamp,
		A.priority,
		A.flags,
		A.is_hidden,
		A.access_group,
		A.child_default_profile,
		A.url,
		A.title,
		A.bank_uid,
		A.pricing_model,
		A.is_citizenship_required,
		A.no_insurance_1,
		A.no_insurance_2,
		A.no_insurance_3,
		A.period_from,
		A.period_to
		%s
	FROM
		%s AS A
		%s
	%s
	%s
	ORDER BY
		%s
	%s
	_QUERY_

	my @out=();

	my $credit=[];

	my %attr=(Slice=>{});
	$credit=$dbh->selectall_arrayref($query, \%attr);
	die $dbh->errstr if $dbh->err;


	# если не найдено, на этом заканчиваем выполнение метода. Возвращаем пустой массив.

	return [] unless @$credit;


	# Формирует сводные данные для кредитов

	my %a=();
	my @uids=map {$_->{'uid'}} @$credit;
	$a{'uids'}=\@uids;
	$a{'city_uid'}=$arg->{'city_uid'} if exists $arg->{'city_uid'};
	my $summary=$this->_summary(\%a) or die;

	# распаковываем доп. данные

	foreach my $hr (@$credit)
	{
		Bin::unpackhash(\$hr->{'file_a'}, $hr) if exists $hr->{'file_a'};
		Bin::unpackhash(\$hr->{'file_b'}, $hr) if exists $hr->{'file_b'};
		delete $hr->{'file_a'};
		delete $hr->{'file_b'};

		# возможно фильтр по городу отбросил все возможные значения по этому кредиту, поэтому возвращаем не все

		if (exists $summary->{$hr->{'uid'}})
		{
			$hr->{'summary'}=$summary->{$hr->{'uid'}};
			push @out,$hr;
		}
	}

	if ($arg->{'cache'})
	{
		my $val_list = [\@out];
		push @$val_list, $this->{'rows'} if defined $this->{'rows'};

		$cache->put({ 'key'=>$cache_key_str, 'value'=>$val_list, 'mem_expire'=>10, 'file_expire'=>60*60, 'tag'=>['Credit2','Credit'] });
	}

	return \@out;
}

sub credit_count
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %sql = ('select'=>'','group'=>'');
	my @where =();

	if(exists $arg->{'city_uid'})
	{
		push @where,sprintf('city_uid=%u',$arg->{'city_uid'});
	}

	if($arg->{'bank_uid'})
	{
		$sql{'select'} = '`cnt`';
		push @where,sprintf('bank_uid=%u',$arg->{'bank_uid'});
	}
	elsif($arg->{'types'})
	{
		$sql{'select'} = 'SUM(`cnt`) AS cnt';
		$sql{'group'} = 'GROUP BY `type_uid`';
	}

	my $where=@where ? join(' AND ',@where) : '';

	my $q = sprintf(<<'	__Q__',$sql{'select'},$where,$sql{'group'});
		SELECT
			`type_uid`, %s
		FROM `Credit2_Count`
		WHERE
			%s
		%s
	__Q__

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['Bank','Credit']);
	my $result=$this->{'dbcache'}->selectall_arrayref($q, \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->err;

	my %out=();

	$out{$_->{'type_uid'}}=$_->{'cnt'} for(@$result);

	return \%out;
}

sub item
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $select='';
	my $from='';
	my $where='';

	if    ($arg->{'uid'})       { $where=sprintf('A.uid=%u',$arg->{'uid'}); }
	elsif ($arg->{'url'} ne '') { $where=sprintf('A.url=%s',$this->{'dbh'}->quote($arg->{'url'})); }
	else                        { return {}; }

	$where.=sprintf(' AND A.is_hidden=%u',$arg->{'is_hidden'}) if exists $arg->{'is_hidden'};

	my %addon = map { $_ => 1 } split(',' => $arg->{'addon'}) if $arg->{'addon'};
	if($addon{'profile'})
	{
		$from .= 'LEFT JOIN `Unite` U ON(A.`uid`=U.`uid`)';
		$select .= ', U.`profile`';
	}

	my $sth=$dbh->prepare(sprintf(<<'	_QUERY_',$select,$this->{'profile'},$from,$where)) or die $dbh->errstr;
	SELECT
		A.*
		%s
	FROM
		%s AS A
		%s
	WHERE
		%s
	LIMIT
		1
	_QUERY_

	$sth->execute or die $this->{'dbh'}->errstr;
	my $hr=$sth->fetchrow_hashref;
	$sth->finish;

	return {} unless defined $hr;


	# Формирует сводные данные для кредитов

	my %a=();
	$a{'uids'}=[ $hr->{'uid'} ];
	$a{'city_uid'}=$arg->{'city_uid'} if exists $arg->{'city_uid'};
	my $summary=$this->_summary(\%a) or die;

	# комиссии для кредитных карт
	{
		last if $hr->{'parent_uid'} != 7209 || scalar @{$a{'uids'}} != 1;

		my $commission = [];
		my $commission=$this->_commission(\%a) or die;

		$hr->{'commission_credits_card'} = $commission;
	}

	# возможно фильтр по городу отбросил все возможные значения по этому кредиту. Не возвращаем такой кредит.

	# 14.06.2014 — теперь возвращаем максимум данных. При пустом значении ставок выводим сообщение о том,
	# что кредит не предоставляется в данном городе…

	#return {} unless exists $summary->{$hr->{'uid'}};

	# Если нет ставок или не указан конкретный город, — возвращаем список идентификаторов городов,
	# для которых доступен данный кредит
	if(!exists $summary->{$hr->{'uid'}} || !$arg->{'city_uid'})
	{
		# получаем идентификаторы городов, для которых доступны отдельные ставки
		my %a = map { $_->[0] => 1 } @{$dbh->selectall_arrayref(
			sprintf('SELECT DISTINCT `city_uid` FROM `Credit2_Rate` WHERE `credit_uid`=%u', $hr->{'uid'}),
			{'Slice' => []}
		)};

		# Если есть набор ставок для всех городов — возвращаем список городов, в которых есть
		# отделения данного банка
		if(exists $a{'0'})
		{
			%a = map { $_ => (int($a{$_})+1) } (@{$this->base_model()->obj('Bank')->city_bank_list('bank_uid'=>$hr->{'bank_uid'})}, keys %a);
		}

		$hr->{'city_allow'} = \%a;
	}

	# доп. данные

	Bin::unpackhash(\$hr->{'file_a'}, $hr);
	Bin::unpackhash(\$hr->{'file_b'}, $hr);
	delete $hr->{'file_a'};
	delete $hr->{'file_b'};

	$hr->{'summary'}=$summary->{$hr->{'uid'}};


	# цель кредита

	my %attr=(Slice=>{});
	$hr->{'purpose'}=$dbh->selectall_arrayref(<<'	_QUERY_', \%attr, $hr->{'uid'});
	SELECT
		B.title
	FROM
		Credit2_Purpose_Ref AS A
		INNER JOIN Credit2_Purpose_Lib AS B ON (B.id=A.id)
	WHERE
		A.uid=?
	ORDER BY
		B.id
	_QUERY_

	# цель кредита #2

	my %attr=(Slice=>{});
	$hr->{'purpose2'}=$dbh->selectall_arrayref(<<'	_QUERY_', \%attr, $hr->{'uid'});
	SELECT
		B.title
	FROM
		Credit2_Purpose_Ref2 AS A
		INNER JOIN Credit2_Purpose_Lib2 AS B ON (B.id=A.id)
	WHERE
		A.uid=?
	ORDER BY
		B.id
	_QUERY_

	return $hr;
}


# Формирует сводные данные для кредитов. Всяческие диапазоны (от - до).
#
# @param arrayref $arg->{'uids'} - список uid
# @param int $arg->{'city_uid'} - uid города, для дополнительной фильтрации сводных данных по городу (необязательный)
# @return hashref

sub _summary
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	# выборка всех возможных вариантов ставок, сумм, сроков и т.п. по кредиту

	my $query=sprintf('SELECT * FROM `Credit2_Rate` WHERE `credit_uid` IN (%s)%s'
		,join(',',map {sprintf('%u',$_)} @{$arg->{'uids'}})
		,(exists $arg->{'city_uid'} ? sprintf(' AND `city_uid` IN (0,%u)',$arg->{'city_uid'}) : '')
	);
	#print $query,"<br>";

	my %attr=(Slice=>{});
	my $rate=$dbh->selectall_arrayref($query, \%attr);
	die $dbh->errstr if $dbh->err;


	# Получаем значения для переменных процентных ставок

	my $var=$this->rate_var_value();


	# группируем строки вариантов по credit_uid

	my %rate_by_credit_uid=();

	foreach my $hr (@$rate)
	{
		push @{$rate_by_credit_uid{$hr->{'credit_uid'}}},$hr;
	}


	# может быть применен фильтр по городу (частично данные уже отфильтрованы в SQL запросе)

	if (exists $arg->{'city_uid'})
	{
		# Для строки варианта кредита может быть определен город city_uid!=0
		# или выставлен признак "все остальные" city_uid==0
		# выкидываем "все остальные", если хотя бы в одной строке кредита есть city_uid!=0 (город определен точно)

		foreach my $credit_uid (keys %rate_by_credit_uid)
		{
			my @a=grep { $_->{'city_uid'}!=0 } @{$rate_by_credit_uid{$credit_uid}};
			$rate_by_credit_uid{$credit_uid}=\@a if @a;
		}
	}


	# Рассчитываем min/max параметры, с учетом валюты и переменной процентной ставки
	# Для начала собираем все данные по кредитам в массивы, потом выберем min/max из массивов

	my %limit=(); # для min/max параметры, по кредиту, и по валюте

	foreach my $credit_uid (keys %rate_by_credit_uid)
	{
		foreach my $line (@{$rate_by_credit_uid{$credit_uid}})
		{
			unless (exists $limit{$credit_uid} && exists $limit{$credit_uid}->{$line->{'currency'}})
			{
				$limit{$credit_uid}->{$line->{'currency'}}={
					'сумма_от'=>[],
					'сумма_до'=>[],
					'взнос_от'=>[], # первоначальный взнос
					'взнос_до'=>[], # первоначальный взнос
					'срок_кредита_от'=>[],
					'срок_кредита_до'=>[],
					'ставка'=>[],
					'сумма_ставок_без_ндфл2'=>0 # для определения обязательна ли справка о доходах. Если нет ставки без НДФЛ2, значит справка о доходах обязательна.
				};
			}

			my $ref=$limit{$credit_uid}->{$line->{'currency'}}; # чтобы не писать много букв

			push @{$ref->{'сумма_от'}}, $line->{'credit_from'};
			push @{$ref->{'сумма_до'}}, $line->{'credit_to'};
			push @{$ref->{'взнос_от'}}, $line->{'payment_from'};
			push @{$ref->{'взнос_до'}}, $line->{'payment_to'};
			push @{$ref->{'срок_кредита_от'}}, $line->{'time_from'};
			push @{$ref->{'срок_кредита_до'}}, $line->{'time_to'};
			push @{$ref->{'ставка'}}, $line->{'rate1'} + ($line->{'rate1_var'} ? $var->{$line->{'rate1_var'}} : 0) if $line->{'rate1'};
			push @{$ref->{'ставка'}}, $line->{'rate2'} + ($line->{'rate2_var'} ? $var->{$line->{'rate2_var'}} : 0) if $line->{'rate2'};
			push @{$ref->{'ставка'}}, $line->{'rate3'} + ($line->{'rate3_var'} ? $var->{$line->{'rate3_var'}} : 0) if $line->{'rate3'};
			push @{$ref->{'ставка'}}, $line->{'rate4'} + ($line->{'rate4_var'} ? $var->{$line->{'rate4_var'}} : 0) if $line->{'rate4'};

			$ref->{'сумма_ставок_без_ндфл2'}+=$line->{'rate1'};
		}
	}

	# выберем min/max из массивов

	while (my ($credit_uid,$ref0)=each %limit)
	{
		while (my ($currency,$ref)=each %$ref0)
		{
			$ref->{'сумма_от'}=min($ref->{'сумма_от'});
			$ref->{'сумма_до'}=max($ref->{'сумма_до'});
			$ref->{'взнос_от'}=min($ref->{'взнос_от'});
			$ref->{'взнос_до'}=max($ref->{'взнос_до'});
			$ref->{'срок_кредита_от'}=min($ref->{'срок_кредита_от'});
			$ref->{'срок_кредита_до'}=max($ref->{'срок_кредита_до'});
			$ref->{'ставка_от'}=min($ref->{'ставка'});
			$ref->{'ставка_до'}=max($ref->{'ставка'});
			$ref->{'обязательна_ндфл2'}=$ref->{'сумма_ставок_без_ндфл2'}==0 ? 1 : 0; # Если нет ставки без НДФЛ2, значит справка о доходах обязательна.
			delete $ref->{'ставка'};
			delete $ref->{'сумма_ставок_без_ндфл2'};
		}
	}

	return \%limit;
}

# коммисии для кредитных карт
sub _commission
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $query=sprintf('SELECT name,currency,percent FROM `Credit2_Commission` WHERE `credit_uid` = %u %s ORDER BY currency'
		,$arg->{'uids'}->[0]
		,(exists $arg->{'city_uid'} ? sprintf(' AND `city_uid` IN (0,%u)',$arg->{'city_uid'}) : '')
	);

	my %attr=(Slice=>{});
	my $commission=$dbh->selectall_arrayref($query, \%attr);
	die $dbh->errstr if $dbh->err;

	my $out = [];
	foreach (@$commission)
	{
    push @$out, $_ if $_->{'percent'};
  }

  return $out;
}
sub rate_var_value
{
	my $this=shift;

	return $rate_var_cache if defined $rate_var_cache;

	my $var=$this->base_model()->obj('Var')->item('url'=>'credit_var_rate') or die;
	$rate_var_cache=$var->{'numeric'};

	return $rate_var_cache;
}


sub get_bank_stat
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return '' unless $arg->{'label'} && $arg->{'bank_uid'};

	my @where = (
		sprintf('A.`bank_uid`=%u',$arg->{'bank_uid'})
	);
	push @where, sprintf('A.`city_uid`=%u', $arg->{'city_uid'}) if $arg->{'city_uid'};

	return sprintf(<<'	__Q__', $this->{'dbh'}->quote($arg->{'label'}), join('&&' => @where));
		SELECT
			%s as label,
			A.`city_uid`,
			SUM(A.`cnt`) AS value
		FROM
			`Credit2_Count` AS A
		WHERE
			%s
		GROUP BY A.`city_uid`
	__Q__
}

# Список всех возможных целей кредита #1
sub purpose
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['purpose']);
	my $ar=$this->{'dbcache'}->selectall_arrayref('SELECT * FROM Credit2_Purpose_Lib ORDER BY title', \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->err;

	my @out=();

	if (exists $arg->{'profiles'} && ref $arg->{'profiles'} eq 'ARRAY') # если передан несколько профилей, передаем ссылкой на массив
	{
		my %H = ();
		$H{$_}++ foreach @{$arg->{'profiles'}};
		@out = grep { exists $H{$_->{'profile'}} } @$ar;
	}
	elsif (exists $arg->{'profile'}) # если передан один профиль
	{
		@out=grep { $_->{'profile'} eq $arg->{'profile'} } @$ar;
	}
	else
	{
		@out=@$ar;
	}

	return \@out;
}

# Список всех возможных целей кредита #2
sub purpose2
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['purpose2']);
	my $ar=$this->{'dbcache'}->selectall_arrayref('SELECT * FROM Credit2_Purpose_Lib2 ORDER BY title', \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->err;

	my @out=();

	if (exists $arg->{'profiles'} && ref $arg->{'profiles'} eq 'ARRAY') # если передан несколько профилей, передаем ссылкой на массив
	{
		my %H = ();
		$H{$_}++ foreach @{$arg->{'profiles'}};
		@out = grep { exists $H{$_->{'profile'}} } @$ar;
	}
	elsif (exists $arg->{'profile'}) # если передан один профиль
	{
		@out=grep { $_->{'profile'} eq $arg->{'profile'} } @$ar;
	}
	else
	{
		@out=@$ar;
	}

	return \@out;
}

# Параметры переданные из формы подбора кредитов приводятся к допустимым значениям.
sub validate_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %out=();

	# Тип кредита
	my $type = $this->base_model->obj('Node')->list('uid_list' => $arg->{'type'},'parent_uid' => 2464);

	die unless $type->[0]{'uid'};

	$out{'type'} = [];
	map { push @{$out{'type'}},$_->{'uid'} } @$type;

	# Валюта
	$out{'currency'}=$arg->{'currency'};
	$out{'currency'}=0 unless $out{'currency'}=~m/\A[012]\z/;

	# Размер кредита
	$out{'sum'}=$arg->{'sum'};
	$out{'sum'}=~tr/0-9//cd;
	$out{'sum'}=1_000_000 unless $out{'sum'}=~m/\A[0-9]\d+\z/;

	# Срок кредита
	$out{'period'}=$arg->{'period'};
	$out{'period'}=12 unless $out{'period'}=~m/\A[0-9]\d*\z/;

	# если первоначальный взнос передан в виде суммы — переводим значение в проценты
	{
		last unless $arg->{'initial_sum'};
		$arg->{'initial_sum'}=~tr/0-9//cd;
		last unless $arg->{'initial_sum'}=~m/\A[0-9]\d*\z/;

		$arg->{'initial'} = int(
			$arg->{'initial_sum'} / $out{'sum'} * 100
		);

		#use Data::Dumper;
		#warn Dumper $arg->{'initial_sum'}, $arg->{'sum'}, $arg->{'initial'};
	}

	{
		$out{'initial'}=$arg->{'initial'};
		$out{'initial'}=~tr/,/./;
		$out{'initial'}=int($out{'initial'} < 100 ? $out{'initial'}*1000/10 : $out{'initial'}); # т.к. проверка срабатывает до запаковки (когда значение еще дробное) и после (когда значение уже * 100)
		$out{'initial'}=40_00 unless $out{'initial'}=~m/\A\d+\z/ && $out{'initial'} < 100_00;

		#warn "OUTINITIAL:", $out{'initial'};
	}

		# доход подтвержден справкой
		$out{'ndfl'}=$arg->{'ndfl'} ? 1 : 0; # в расширенном варианте появляется возможность выбора

		# фиксированная ставка
		$out{'fix_rate'} = $arg->{'fix_rate'} ? 1 : 0;

		# схема расчета
		$out{'pricing_model'}=$arg->{'pricing_model'};
		$out{'pricing_model'}=0 unless $out{'pricing_model'}=~m/\A[012]\z/;

		# для граждан других стран
		$out{'no_citizenship'}=$arg->{'no_citizenship'} ? 1 : 0;

		# цель кредита
		$out{'purpose'}=$arg->{'purpose'};
		$out{'purpose'}=0 unless $out{'purpose'}=~m/\A\d+\z/;

		# цель кредита №2
		$out{'purpose2'}=$arg->{'purpose2'};
		$out{'purpose2'}=0 unless $out{'purpose2'}=~m/\A\d+\z/;

		# страхование
		$out{'no_insurance_1'}=$arg->{'no_insurance_1'} ? 1 : 0;
		$out{'no_insurance_2'}=$arg->{'no_insurance_2'} ? 1 : 0;
		$out{'no_insurance_3'}=$arg->{'no_insurance_3'} ? 1 : 0;

		# банки
		if (exists $arg->{'bank'} && ref($arg->{'bank'}) eq 'ARRAY' && @{$arg->{'bank'}} && not(in_array(0,$arg->{'bank'})))
		{
			$out{'bank'}=[
				sort {$a<=>$b} map {sprintf('%u',$_)} @{$arg->{'bank'}}
			];
		}
	return \%out;
}

{
	# Версия и формат запаковки.
	# Предполагаю, со временем формат может измениться. Нужно обеспечить распаковку данных, и старого формата, и нового.

	my %pack_format=(
		# для поиска кредита
		1 => [ 'currency','sum','period','initial','ndfl','pricing_model','no_citizenship','no_insurance_1','no_insurance_2','no_insurance_3','purpose','purpose2','type','bank' ],

    	# для сравнения кредитов
		2 => [ 'type','currency','sum','period','initial','ndfl','pricing_model','no_citizenship','no_insurance_1','no_insurance_2','no_insurance_3' ],

		# для поиска кредита с фиксированной ставкой
		3 => [ 'currency','sum','period','initial','ndfl','pricing_model','no_citizenship','no_insurance_1','no_insurance_2','no_insurance_3','purpose','purpose2','fix_rate','type','bank' ]
	);

	# Запаковывает параметры переданные из формы подбора кредитов в строку.
	# Перед запаковкой используется метод validate_request.
	# my $pack=$HEAP{'view'}->obj('Credit_Moneyzzz')->pack_request(\%GET) or die;

	sub pack_request
	{
		my $this=shift;
		my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

		if (ref $arg->{'value'}->{'type'} eq 'ARRAY' && in_array(2465,$arg->{'value'}->{'type'}))
		{
			$arg->{'value'}->{'period'} = $arg->{'value'}->{'period'}*12;
		}

		my $val=$this->validate_request($arg->{'value'}) or die; # параметры переданные из формы подбора кредитов приводятся к допустимым значениям

		my $pack_format_version=exists $pack_format{$arg->{'format'}} ? $arg->{'format'} : 1; # текущая версия формата запаковки

		my $key=$pack_format{$pack_format_version};

		my @a=map {ref($val->{$_}) eq 'ARRAY' ? join('.',map {sprintf('%x',$_)} sort {$a<=>$b} @{$val->{$_}}) : sprintf('%x',$val->{$_})} @$key;

		unshift @a,sprintf('%x',$pack_format_version); #первым элемент строки всегда содержит версию формата запаковки

		return join('-',@a);
	}

	# Распаковывает строку в значения для подбора кредитов (методом search).
	# После распаковки используется метод validate_request.
	# my $hr=$HEAP{'view'}->obj('Credit_Moneyzzz')->unpack_request('pack'=>$pack) or die;

	sub unpack_request
	{
		my $this=shift;
		my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

		my @a=split(/-/,$arg->{'pack'});

		my $pack_format_version=shift @a; # по первому элементу определяется версия формата запаковки

		$pack_format_version=1 unless $pack_format{$pack_format_version};

		my $key=$pack_format{$pack_format_version};

		my %out=(); # тут будут возвращаемые значения

		for (my $i=0, my $j=$#{$key}; $i<=$j; $i++)
		{
			$out{$key->[$i]}=index($a[$i],'.')==-1
				? hex($a[$i])
				: [ map {hex($_)} split(/\./,$a[$i]) ];
		}
		$out{$key->[0]}=[ $out{$key->[0]} ] if ref($out{$key->[0]}) ne 'ARRAY' && $pack_format_version == 2; # последний элемент (банки) всегда массив
		$out{$key->[-2]}=[ $out{$key->[-2]} ] if ref($out{$key->[-2]}) ne 'ARRAY' && $pack_format_version !=2; # предпоследний элемент (тип) всегда массив
		$out{$key->[-1]}=[ $out{$key->[-1]} ] if ref($out{$key->[-1]}) ne 'ARRAY' && $pack_format_version !=2; # последний элемент (банки) всегда массив

		return $this->validate_request(\%out) or die;
	}
}

sub rate_var_value
{
	my $this=shift;

	return $rate_var_cache if defined $rate_var_cache;

	my $var=$this->base_model()->obj('Var')->item('url'=>'credit_var_rate') or die;
	$rate_var_cache=$var->{'numeric'};

	return $rate_var_cache;
}


# Подбирает кредиты по параметрам. Дополнительно рассчитывает переплату по кредиту.
#
# @param - описание временно отсутствует (TODO)
# @return arrayref

sub search
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

  $arg->{'parent_uid'} = $arg->{'type'} if ref $arg->{'type'} eq 'ARRAY' && @{$arg->{'type'}};
  $arg->{'bank'} = [] unless $arg->{'bank'};

  $arg->{'sort'} ||= 0;

  die 'Incorrect $arg->{\'sort\'}' if $arg->{'sort'}%10 >= 3;

	die 'Undefined $arg->{parent_uid}' unless exists $arg->{'parent_uid'};
	die 'Undefined $arg->{city_uid}' unless exists $arg->{'city_uid'};
	die 'Undefined $arg->{ndfl}' unless exists $arg->{'ndfl'};
	die 'Incorrect $arg->{sum}' unless $arg->{'sum'}=~m/\A\d+\z/;
	die 'Incorrect $arg->{currency}' unless $arg->{'currency'}=~m/\A[012]\z/;
	die 'Incorrect $arg->{period}' unless $arg->{'period'}=~m/\A\d+\z/;
	#die 'Incorrect $arg->{max_rate}' if exists $arg->{'max_rate'} && $arg->{'max_rate'} !~ m/\A\d+\z/;
	die 'Incorrect $arg->{initial}' unless $arg->{'initial'}=~m/\A\d+\z/ && $arg->{'initial'}<100_00;
	die 'Incorrect $arg->{bank}' unless exists $arg->{'bank'} && ref($arg->{'bank'}) eq 'ARRAY';


	# Получаем значения для переменных процентных ставок

	my $var=$this->rate_var_value();


	# выбор банков у которых есть филиалы в city_uid

	my $bank_uid_list='';
	{
		my $uids=$this->base_model()->obj('Bank')->bank_city_list({ 'city_uid'=>$arg->{'city_uid'} }) or die;

		return [] unless @$uids;

		my %all_bank=map {( $_, 1 )} @$uids; # все банки из СПб
		my %uniq_bank_uid=();

		unless (@{$arg->{'bank'}}) # выбрано "в любом банке"
		{
			(%uniq_bank_uid)=(%all_bank); # берем все банки из СПб
		}
		else
		{
			#if (in_array(sprintf('%u',-1), $arg->{'bank'})) # top20 банков
			#{
			#	%uniq_bank_uid=map {( $_, 1 )} (@$uids)[0..19]; # top20 банков
			#}

			# добавляем к уже выбранным из top20

			foreach my $uid (@{$arg->{'bank'}})
			{
				$uniq_bank_uid{$uid}++ if exists $all_bank{$uid};
			}
		}

		$bank_uid_list=join(',',map {sprintf('%u',$_)} keys %uniq_bank_uid);
	}

	return [] if $bank_uid_list eq '';


	# первоначальный фильтр кредитов

	my $where='';
	$where.=sprintf(' AND A.parent_uid IN (%s)',join(',',@{$arg->{'parent_uid'}}));
	$where.=sprintf(' AND A.uid IN (%s)',join(',',map {sprintf('%u',$_)} @{$arg->{'compare_uid_list'}})) if exists $arg->{'compare_uid_list'} && ref($arg->{'compare_uid_list'}) eq 'ARRAY' && @{$arg->{'compare_uid_list'}};
	$where.=sprintf(' AND A.bank_uid IN (%s)',$bank_uid_list);
	$where.=$arg->{'initial'} ? sprintf(' AND R.payment_from<=%1$u AND (R.payment_to>%1$u OR R.payment_to=0)',$arg->{'initial'}) : ' AND R.payment_from=0';
	$where.=sprintf(' AND A.is_hidden=%u',$arg->{'is_hidden'}) if exists $arg->{'is_hidden'};
	$where.=sprintf(' AND A.pricing_model IN (0,%u)',$arg->{'pricing_model'}) if $arg->{'pricing_model'}; # 1 или 2 (аннуитетный или дифференцированный)
	$where.=' AND A.is_citizenship_required=0' if $arg->{'no_citizenship'};

	my $query=sprintf(<<'	_QUERY_',$this->{'profile'},$arg->{'currency'},$arg->{'city_uid'},$arg->{'sum'},$arg->{'sum'},$arg->{'period'},$arg->{'period'},$where);
	SELECT
		R.*
	FROM
		%s AS A
		INNER JOIN Credit2_Rate AS R ON (A.uid=R.credit_uid)
	WHERE
		R.currency=%u
		AND R.city_uid IN (0,%u)
		AND R.credit_from<=%u AND (R.credit_to>%u OR R.credit_to=0)
		AND R.time_from<=%u AND (R.time_to>=%u OR R.time_to=0)
		%s
	ORDER BY
		R.seq
	_QUERY_

	my %attr=(Slice=>{});
	my $ar=$dbh->selectall_arrayref($query, \%attr);
	die $dbh->errstr if $dbh->err;

	return [] unless @$ar;


	# группируем строки вариантов по credit_uid (id кредита)

	my %by_credit_uid=();

	foreach my $hr (@$ar)
	{
		push @{$by_credit_uid{$hr->{'credit_uid'}}},$hr;
	}

	# удаление кредитов у которых есть переменная ставка
	if ($arg->{'fix_rate'})
	{
		foreach my $credit_uid (keys %by_credit_uid)
		{
			foreach my $hr (@{$by_credit_uid{$credit_uid}})
			{
				next if $hr->{'rate1_var'} eq '' && $hr->{'rate2_var'} eq '' && $hr->{'rate3_var'} eq '' && $hr->{'rate4_var'} eq '';
				delete $by_credit_uid{$credit_uid};
				last;
			}
		}
	}

	# Для строки варианта кредита может быть определен город city_uid!=0
	# или выставлен признак "все остальные" city_uid==0
	# выкидываем "все остальные", если хотя бы в одной строке кредита есть city_uid!=0 (город определен точно)

	foreach my $credit_uid (keys %by_credit_uid)
	{
		my @a=grep { $_->{'city_uid'}!=0 } @{$by_credit_uid{$credit_uid}};
		$by_credit_uid{$credit_uid}=\@a if @a;
	}

	# Рассчитываем ставку, с учетом переменной процентной ставки, и наличия НДФЛ

	foreach my $credit_uid (keys %by_credit_uid)
	{
		foreach my $hr (@{$by_credit_uid{$credit_uid}})
		{
			# для каждого варианта выбираем подходящее под заданные условиям значение ставки, помещаем в _rate

			$hr->{'_rate'}=$arg->{'ndfl'} ? $hr->{'rate2'} : $hr->{'rate1'};
			my $rate_var=$arg->{'ndfl'} ? $hr->{'rate2_var'} : $hr->{'rate1_var'};

			if ($rate_var) # если ставка переменная, добавляем к _rate сегодняшнее значение переменного коэффициента
			{
				die 'Undefined rate_var_value' unless exists $var->{$rate_var};
				$hr->{'_rate'}+=$var->{$rate_var};
			}
		}
	}

	# Перестраиваем данные. Группируем варианты если есть плавающая ставка.
	# Фильтруем если задана максимальная ставка.
	foreach my $credit_uid (keys %by_credit_uid)
	{
		my @a=();
		my %tmp=();

		foreach my $hr (@{$by_credit_uid{$credit_uid}})
		{
			$hr->{'float_mon'}==0
				? push @a,[ $hr ] # нет плавающей ставки (массив состоит только из одного варианта)
				: push @{$tmp{join(',', $hr->{'city_uid'}, $hr->{'currency'}, $hr->{'credit_from'}, $hr->{'credit_to'}, $hr->{'payment_from'}, $hr->{'payment_to'}, $hr->{'time_from'}, $hr->{'time_to'})}}, $hr;
		}

		POINT1:
		{
			# неправильно заполнены данные, если ставка плавающая поле "Ставка действует с"
			# должено начинаться с 1.
			# если ставка годовая, то, возможно данные в поле
			# "Взнос от (% ≤ ПВ)" и/или "Взнос до (ПВ < %)" указаны неверно
			if(scalar @a > 1) {
				#warn "Moneyzzz credit data error #1 (credit_uid: $credit_uid) - skip credit";
				warn_log(1,$credit_uid,$by_credit_uid{$credit_uid},$arg,\@a);
				next POINT1;
			}

			foreach my $ar (values(%tmp)) # если есть плавающая ставка, собираем все варианты описывающие ее в один массив
			{
				# сортируем по полю "Ставка действует с"

				my @b=sort { $a->{'float_mon'} <=> $b->{'float_mon'} } @$ar;

				# поиск ошибок оператора при заполнении плавающей ставки
				# если поле "Ставка действует с" фигурирует больше одного раза - оператор ошибся

				my $prev_float_mon=-1;

				foreach my $hr (@b)
				{
					if ($prev_float_mon==$hr->{'float_mon'}) # учитывая что данные отсортированы
					{
						#warn "Moneyzzz credit data error #2 (credit_uid:$credit_uid) - skip credit";
						warn_log(2,$credit_uid,$by_credit_uid{$credit_uid},$arg,\@b);
						next POINT1;
					}
					$prev_float_mon=$hr->{'float_mon'};
				}

				# ошибка не найдена - записываем

				push @a,\@b;
			}
		}
		# обнуляем текущее значения для кредита

		$by_credit_uid{$credit_uid}={
			'var'=>[]
		};

		# заполняем значения по новой. В сгруппированные по плавающей ставке,
		# фильтрованные по максимальной ставке (если нужно),
		# и выбрасываем значение если ставка == 0 (например, если банк не выдает кредита без подтверждения дохода)

		foreach my $ar (@a)
		{
			# рассчитываем максимальную и минимальную процентную ставку для варианта

			my $min_rate = my $max_rate = $ar->[0]->{'_rate'};

			for (my $i=1; $i<=$#{$ar}; $i++)
			{
				my $_rate=$ar->[$i]->{'_rate'};
				$min_rate=$_rate if $min_rate > $_rate;
				$max_rate=$_rate if $max_rate < $_rate;
			}

			next if $min_rate==0; # выбрасываем т.к. ставка не может == 0 (например, банк не выдает кредита без подтверждения дохода)

			#решили отказаться т.к. может откинуть хороший вариант
			#next if exists $arg->{'max_rate'} && $max_rate > $arg->{'max_rate'}; # максимальная ставка задана - фильтруем


			# вариант подходит под условия - сохраняем его

			push @{$by_credit_uid{$credit_uid}->{'var'}},{
				'data'=>$ar,
				'min_rate'=>$min_rate,
				'max_rate'=>$max_rate
			};
		}
	}


	# Удаляем кредиты, у которых, после фильтрации, не осталось подходящий вариантов

	foreach my $credit_uid (keys %by_credit_uid)
	{
		delete $by_credit_uid{$credit_uid} unless @{$by_credit_uid{$credit_uid}->{'var'}};
	}


	# список UID отфильтрованных кредитов

	my $uidlist=join(',',keys %by_credit_uid);

	# если ничего не найдено

	return [] unless $uidlist;

	# выбираем дополнительные данные для отобранных кредитов
	# для оптимизации делаем отдельный SQL запрос
	# вероятно строка $uidlist не будет очень длинной

	my $from='';
	$from.=sprintf(' INNER JOIN Credit2_Purpose_Ref AS P ON (A.uid=P.uid AND P.id=%u)',$arg->{'purpose'}) if $arg->{'purpose'};
	$from.=sprintf(' INNER JOIN Credit2_Purpose_Ref2 AS P2 ON (A.uid=P2.uid AND P2.id=%u)',$arg->{'purpose2'}) if $arg->{'purpose2'};

	my $query=sprintf(<<'	_QUERY_',$this->{'profile'},$arg->{'city_uid'},$from,$uidlist);
	SELECT
		A.*,
		B.uid AS bank_uid,
		B.title AS bank_title,
		B.url AS bank_url,
		B.file_a AS bank_file_a,
		R.rating,
		C.rating AS bank_city_rating
	FROM
		%s AS A
		LEFT JOIN Bank AS B ON (A.bank_uid=B.uid)
		LEFT JOIN Bank_Rating AS R ON (R.bank_uid=B.uid)
		LEFT JOIN Bank_City_Rating AS C ON (C.bank_uid=B.uid AND C.city_uid=%s)
		%s
	WHERE
		A.uid IN (%s)
	_QUERY_

	my %attr=(Slice=>{});
	my $credit=$dbh->selectall_arrayref($query, \%attr);
	die $dbh->errstr if $dbh->err;

	foreach my $hr (@$credit)
	{
		Bin::unpackhash(\$hr->{'file_a'}, $hr);
		Bin::unpackhash(\$hr->{'file_b'}, $hr);
		delete($hr->{'file_a'});
		delete($hr->{'file_b'});

		Bin::unpackhash(\$hr->{'bank_file_a'}, $hr);
		delete($hr->{'bank_file_a'});


		my %calc_arg=( # аргументы для функции расчета графика платежей
			'сумма'=>$arg->{'sum'},
			'месяцы'=>$arg->{'period'},
			'проценты'=>undef
		);

		my $min_overpayment=undef; # для значения минимальной переплаты
		my $min_overpayment_var=undef; # для ссылки на вариант с минимальной переплатой

		foreach my $var (@{$by_credit_uid{$hr->{'uid'}}->{'var'}})
		{
			if (scalar(@{$var->{'data'}})==1)
			{
				$calc_arg{'проценты'}=$var->{'data'}->[0]->{'_rate'}/100;
			}
			else
			{
				$calc_arg{'проценты'}={};
				$calc_arg{'проценты'}->{$_->{'float_mon'}}=$_->{'_rate'}/100 foreach @{$var->{'data'}};
			}

			# расчет графика платежей

			my $calc=($hr->{'pricing_model'}==2)
				? _differentiated_payment(\%calc_arg) # дифференцированный платеж
				: _annuity_payment(\%calc_arg);  # Аннуитетный платеж или Любой

			$var->{'overpayment'}=$calc->{'сумма_выплаченных_процентов'};

			if ($min_overpayment > $var->{'overpayment'} || not defined $min_overpayment)
			{
				$min_overpayment=$var->{'overpayment'};
				$min_overpayment_var=$var;
			}
		}

		# выкидываем все кроме варианта, с минимальной переплатой (у каждого кредита остается один вариант)
		$hr->{'overpayment'}=$min_overpayment_var->{'overpayment'}; # в дальнейшем к этому значению прибавим комиссии банка
		$hr->{'amount_of_interest_paid'}=$min_overpayment_var->{'overpayment'}; # сумма выплаченных процентов
		$hr->{'var'}=$min_overpayment_var->{'data'};
		$hr->{'min_rate'}=$min_overpayment_var->{'min_rate'};
		$hr->{'max_rate'}=$min_overpayment_var->{'max_rate'};
		$hr->{'sum'}=$arg->{'sum'};
		$hr->{'currency'}=$arg->{'currency'};
		$hr->{'period'}=$arg->{'period'};
		$hr->{'initial'}=$arg->{'initial'};
		$hr->{'ndfl'}=$arg->{'ndfl'};
	}

	# Комиссии банка
	my %commission_by_credit_uid=();
  {
    # для кредитных карт мы не считаем коммисию
    map {last if $_ == 7209} @{$arg->{'parent_uid'}};

	  my $query=sprintf(<<'		_QUERY_',$uidlist,$arg->{'currency'},$arg->{'city_uid'},$arg->{'sum'},$arg->{'initial'},$arg->{'period'});
  	SELECT
  		*
  	FROM
  		Credit2_Commission AS C
  	WHERE
  		C.credit_uid IN (%s)
  		AND C.currency=%u
  		AND C.city_uid IN (0,%u)
  		AND C.amount_credit<=%u
  		AND C.first_percent_from<=%u
  		AND C.period<=%u
		_QUERY_

	  my %attr=(Slice=>{});
	  my $commission=$dbh->selectall_arrayref($query, \%attr);
	  die $dbh->errstr if $dbh->err;

	  # группируем комиссии по credit_uid

	  foreach my $hr (@$commission)
	  {
		  push @{$commission_by_credit_uid{$hr->{'credit_uid'}}},$hr;
	  }

	  # выкидываем города "все остальные", если хотя бы в одной строке кредита есть city_uid!=0 (город определен точно)
	  foreach my $credit_uid (keys %commission_by_credit_uid)
	  {
		  my @a=grep { $_->{'city_uid'}!=0 } @{$commission_by_credit_uid{$credit_uid}};
		  $commission_by_credit_uid{$credit_uid}=\@a if @a;
	  }

	  foreach my $credit_uid (keys %commission_by_credit_uid)
	  {
		  # группируем названию + периодичность
		  my %group=();

		  foreach my $hr (@{$commission_by_credit_uid{$credit_uid}})
		  {
			  push @{$group{sprintf('%s-%u',$hr->{'name'},$hr->{'periodicity'})}}, $hr;
		  }

		  # из каждой группы оставляем только одну запись (наиболее подходящий = с максимальными значениями: "сумма кредита от", "первоначальный взнос от", "срок кредита от")
		  my @filtered=();

		  foreach my $gr (values %group)
		  {
			  my @sorted=sort { $b->{'amount_credit'} <=> $a->{'amount_credit'} || $b->{'first_percent_from'} <=> $a->{'first_percent_from'} || $b->{'period'} <=> $a->{'period'} } @$gr;
			  push @filtered, $sorted[0];
		  }

		  # считаем сумму всех комиссий банка (разовых и ежемесячных)
		  my $sum=0;

		  foreach my $hr (@filtered)
		  {
			  my $payment=$hr->{'amount'}; # размер платежа

			  if ($payment==0) # размер платежа явно не указан
			  {
				  $payment=$arg->{'sum'}*$hr->{'percent'}/10000; # рассчитываем проценты от суммы кредита
				  $payment=$hr->{'but_min'} if $hr->{'but_min'}!=0 && $payment < $hr->{'but_min'}; # но не менее
				  $payment=$hr->{'but_max'} if $hr->{'but_max'}!=0 && $payment > $hr->{'but_max'}; # но не более
			  }

			  unless ($payment) # неверно указан размер комиссии
			  {
				  local $Data::Dumper::Indent=0;
				  local $Data::Dumper::Purity=1;
				  local $Data::Dumper::Useqq = 1;
				  no warnings 'redefine';

				local *Data::Dumper::qquote = sub {
					my $s = shift;
					return "'$s'";
				};

				  die "Comission payment = 0; credit_uid=$credit_uid; ",Dumper($hr);
			  }

			  if ($hr->{'periodicity'}==0) # разовый платеж
			  {
				  $sum+=$payment;
			  }
			  else # ежемесячный платеж
			  {
				  $sum+=$payment*$arg->{'period'};
			  }
		  }
		  $commission_by_credit_uid{$credit_uid}=$sum;
	  }
  }

	# добавляем комиссию к данным кредита
	foreach my $hr (@$credit)
	{
	  $hr->{'overpayment'}+=$commission_by_credit_uid{$hr->{'uid'}};
	  $hr->{'commission'}=$commission_by_credit_uid{$hr->{'uid'}};
	}

=pod
	# решили отказаться т.к. может откинуть хороший вариант
	# фильтр по комиссии
	if ($arg->{'without_additional_fee'}) # выкидываем кредиты с комиссией банка
	{
		my @tmp=();
		foreach my $hr (@$credit)
		{
			push @tmp,$hr unless exists $hr->{'commission'} && $hr->{'commission'} > 0;
		}
		$credit=\@tmp;
	}
=cut

	# сортировочка:
	# параметр $arg->{'sort'} - десятичное число, где:
	# первый разряд - параметр сортировки, значения 0 - переплата,1 - рейтинг банка,2 - ставка
	# второй разряд - порядок сортировки, значения 0 - возрастанию и 1 - убыванию
	my $credit_order = 1 if ($arg->{'sort'} >= 10);

	my @rv=();
	if (in_array(7209, $arg->{'parent_uid'}))
	{
		if ($arg->{'sort'}%10 == 1)
		{
			@rv = _sort('for_sort' => \@$credit,'type' => 'rating','order' => $credit_order);
		}
		else
		{
			@rv = _sort('for_sort' => \@$credit,'type' => 'min_rate','order' => $credit_order);
		}
	}
	else
	{
		if ($arg->{'sort'}%10 == 1)
		{
			@rv = _sort('for_sort' => \@$credit,'type' => 'rating','order' => $credit_order);
		}
		elsif ($arg->{'sort'}%10 == 2)
		{
			@rv = _sort('for_sort' => \@$credit,'type' => 'min_rate','order' => $credit_order);
		}
		else
		{
			@rv = _sort('for_sort' => \@$credit,'type' => 'overpayment','order' => $credit_order);
		}
	}


	$this->{'rows'}=scalar(@rv); # общее кол-во найденных кредитов (для постраничной навигации)

	# постраничная навигация

	my $page=[];

	if ($arg->{'limit'})
	{
		for (my $i=0; $i < $arg->{'limit'}; $i++)
		{
			last unless exists $rv[$arg->{'offset'}+$i];

			push @$page, $rv[$arg->{'offset'}+$i];
		}
	}
	else
	{
		$page=\@rv;
	}


	# выборка тем

	{
		my @uid=map {sprintf('%u',$_->{'uid'})} @$page;

		last unless @uid;

		my $uids=join(',',@uid);

		my $query1=sprintf(<<'		_EOL_',$uids);
		SELECT
			R.uid,
			L.title
		FROM
			Credit2_Purpose_Ref AS R
			INNER JOIN Credit2_Purpose_Lib AS L ON (L.id=R.id)
		WHERE
			R.uid IN (%s)
		ORDER BY
			L.title
		_EOL_

		my %attr=(Slice=>{});
		my $purpose1=$dbh->selectall_arrayref($query1, \%attr);
		die $dbh->errstr if $dbh->err;

		my %purpose1_by_uid=();
		push @{$purpose1_by_uid{$_->{'uid'}}},$_->{'title'} foreach @$purpose1;

		my $query2=sprintf(<<'		_EOL_',$uids);
		SELECT
			R.uid,
			L.title
		FROM
			Credit2_Purpose_Ref2 AS R
			INNER JOIN Credit2_Purpose_Lib2 AS L ON (L.id=R.id)
		WHERE
			R.uid IN (%s)
		ORDER BY
			L.title
		_EOL_

		my %attr=(Slice=>{});
		my $purpose2=$dbh->selectall_arrayref($query2, \%attr);
		die $dbh->errstr if $dbh->err;

		my %purpose2_by_uid=();
		push @{$purpose2_by_uid{$_->{'uid'}}},$_->{'title'} foreach @$purpose2;

		foreach my $hr (@$page)
		{
			$hr->{'purpose1str'}=exists $purpose1_by_uid{$hr->{'uid'}} ? join(', ',@{$purpose1_by_uid{$hr->{'uid'}}}) : '';
			$hr->{'purpose2str'}=exists $purpose2_by_uid{$hr->{'uid'}} ? join(', ',@{$purpose2_by_uid{$hr->{'uid'}}}) : '';
		}
	}

  {
	  # Формирует сводные данные для кредитов
		foreach my $hr (@$page)
    {
	    my %a=();
	    $a{'uids'}=[ $hr->{'uid'} ];
	    $a{'city_uid'}=$arg->{'city_uid'} if exists $arg->{'city_uid'};
	    my $summary=$this->_summary(\%a) or die;
      $hr->{'summary'} = $summary->{$hr->{'uid'}};
    }
  }

	return $page;
}

sub _differentiated_payment # дифференцированный платеж
{
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $sc=$arg->{'сумма'}; #сумма кредита
	my $m=$arg->{'месяцы'}; #количество месяцев (срок, на который выдан кредит).

	#my $pgod=$arg->{'проценты'}; #процентов годовых
	my $percent=$arg->{'проценты'}; #процентов годовых, или комбинированные ставки = HASH вида { номер_месяца => процентов_годовых, ... }
	my $komb=ref($percent) eq 'HASH' ? 1 : 0; #комбинированные ставки
	my $pgod=$komb ? $percent->{1} : $percent; #процентов годовых

	my $psm=$pgod/(100*12); #процентная ставка в долях за месяц , т.е., если годовая % ставка равна 18%, то ПС = 18/(100*12);

	my $total_percent=0; #сумма выплаченных процентов
	my $total_debt=0; #сумма выплаты основного долга
	my $total=0; #сумма всех выплат

	my $oz=$sc; #остаток задолженности на начало месяца
	my $vod=$sc/$m; #сумма возврата основного долга

	my %return=(
		'график_платежей'=>[]
	);

	for (my $i=1; $i<=$m; $i++) # $i = номер месяца
	{
		if ($komb && $i!=1 && exists $percent->{$i})
		{
			my $m2=$m+1-$i; #количество месяцев (срок, на который выдан кредит).
			$pgod=$percent->{$i}; #процентов годовых
			$psm=$pgod/(100*12); #процентная ставка в долях за месяц , т.е., если годовая % ставка равна 18%, то ПС = 18/(100*12);
		}

		my $epv=$oz*$psm; #ежемесячные процентные выплаты
		my $oz2=$oz-$vod; #остаток задолженности на конец месяца

		push @{$return{'график_платежей'}},{
			'месяц'=>$i,
			'выплата_процентов'=>sprintf('%.2f',$epv),
			'выплата_долга'=>sprintf('%.2f',$vod),
			'долг_на_начало_месяца'=>sprintf('%.2f',$oz),
			'долг_на_конец_месяца'=>sprintf('%.2f',$oz2 > 0 ? $oz2 : 0)
		};

		$total_percent+=$epv;
		$total_debt+=$vod;
		$total+=$epv+$vod;

		$oz=$oz2;
	}

	if (sprintf('%.2f',$total_debt) ne sprintf('%.2f',$sc)) # ведем лог, на всякий случай, для отлова возможных ошибок
	{
		local $Data::Dumper::Indent=0;
		local $Data::Dumper::Purity=1;
		local $Data::Dumper::Useqq = 1;
		no warnings 'redefine';

		local *Data::Dumper::qquote = sub {
			my $s = shift;
			return "'$s'";
		};
		warn "Incorrect calculation: $total_debt!=$sc; ARG: ",Dumper($arg);
	}

	$return{'сумма_выплаченных_процентов'}=sprintf('%.2f',$total_percent);
	$return{'сумма_всех_выплат'}=sprintf('%.2f',$total);

	return \%return;
}

sub _annuity_payment # Аннуитетный платеж
{
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $sc=$arg->{'сумма'}; #сумма кредита
	my $m=$arg->{'месяцы'}; #количество месяцев (срок, на который выдан кредит).

	#my $pgod=$arg->{'проценты'}; #процентов годовых
	my $percent=$arg->{'проценты'}; #процентов годовых, или комбинированные ставки = HASH вида { номер_месяца => процентов_годовых, ... }
	my $komb=ref($percent) eq 'HASH' ? 1 : 0; #комбинированные ставки
	my $pgod=$komb ? $percent->{1} : $percent; #процентов годовых

	my $psm=$pgod/(100*12); #процентная ставка в долях за месяц , т.е., если годовая % ставка равна 18%, то ПС = 18/(100*12);
	my $ap=($sc * $psm) / (1-(1+$psm)**(-1*$m)); #Аннуитетный платеж - размер ежемесячного платежа

	my %return=(
		'график_платежей'=>[]
	);

	$return{'аннуитетный_платеж'}=sprintf('%.2f',$ap) unless $komb;

	my $total_percent=0; #сумма выплаченных процентов
	my $total_debt=0; #сумма выплаты основного долга
	my $total=0; #сумма всех выплат

	my $oz=$sc; #остаток задолженности на начало месяца

	for (my $i=1; $i<=$m; $i++) # $i = номер месяца
	{
		if ($komb && $i!=1 && exists $percent->{$i})
		{
			my $m2=$m+1-$i; #количество месяцев (срок, на который выдан кредит).
			$pgod=$percent->{$i}; #процентов годовых
			$psm=$pgod/(100*12); #процентная ставка в долях за месяц , т.е., если годовая % ставка равна 18%, то ПС = 18/(100*12);
			$ap=($oz * $psm) / (1-(1+$psm)**(-1*$m2)); #Аннуитетный платеж - размер ежемесячного платежа
		}

		my $epv=$oz*$psm; #ежемесячные процентные выплаты
		my $vod=$ap-$epv; #сумма возврата основного долга
		my $oz2=$oz-$vod; #остаток задолженности на конец месяца

		push @{$return{'график_платежей'}},{
			'месяц'=>$i,
			'выплата_процентов'=>sprintf('%.2f',$epv),
			'выплата_долга'=>sprintf('%.2f',$vod),
			'долг_на_начало_месяца'=>sprintf('%.2f',$oz),
			'долг_на_конец_месяца'=>sprintf('%.2f',$oz2 > 0 ? $oz2 : 0)
		};

		$total_percent+=$epv;
		$total_debt+=$vod;
		$total+=$ap;

		$oz=$oz2;
	}

	if (sprintf('%.2f',$total_debt) ne sprintf('%.2f',$sc)) # ведем лог, на всякий случай, для отлова возможных ошибок
	{
		local $Data::Dumper::Indent=0;
		local $Data::Dumper::Purity=1;
		local $Data::Dumper::Useqq = 1;
		no warnings 'redefine';

		local *Data::Dumper::qquote = sub {
			my $s = shift;
			return "'$s'";
		};
		warn "Incorrect calculation: $total_debt!=$sc; ARG: ",Dumper($arg);
	}

	$return{'сумма_выплаченных_процентов'}=sprintf('%.2f',$total_percent);
	$return{'сумма_всех_выплат'}=sprintf('%.2f',$total);

	return \%return;
}

# сортировка кредитов, параметры:
# $arg->{'for_sort'} - ссылка на массив для сортировки,
# $arg->{'type'} - параметр по которому сортировать,
# $arg->{'order'} - порядок (необязателен), если не равен 0,'',undef, то по убыванию
sub _sort
{
  my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

  die 'Undefined or incorrect $arg->{\'for_sort\'}' unless $arg->{'for_sort'} && ref $arg->{'for_sort'} eq 'ARRAY';
  die 'Undefined $arg->{\'type\'}' unless $arg->{'type'};

  $arg->{'order'} ||= 0;

  my @sorted=();

  if ($arg->{'order'})
  {
    return @sorted=sort { $b->{ $arg->{'type'} } <=> $a->{ $arg->{'type'} } } @{$arg->{'for_sort'}};
  }
  else
  {
    return @sorted=sort { $a->{ $arg->{'type'} } <=> $b->{ $arg->{'type'} } } @{$arg->{'for_sort'}};
  }
  die 'Incorrect sort' unless @sorted;
}

# добавляет кредит к сравнению
sub add_to_compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return 0 unless exists $arg->{'compare_uid_list'} && ref($arg->{'compare_uid_list'}) eq 'ARRAY' && @{$arg->{'compare_uid_list'}};


	$dbh->do('LOCK TABLES Compare_Credit WRITE') or die $dbh->errstr;

	my $compare_id=$arg->{'compare_id'};

	unless ($compare_id) # новое сравнение, пользователь ранее ничего не сравнивал
	{
		# генерируем новый compare_id

		my $sth=$dbh->prepare('SELECT MAX(compare_id) FROM Compare_Credit') or die $dbh->errstr;
		$sth->execute or die $dbh->errstr;
		($compare_id)=$sth->fetchrow_array;
		$sth->finish;

		$compare_id++; # новый $compare_id больше максимально созданного ранее
	}

	my @value=();

	foreach my $credit_uid (@{$arg->{'compare_uid_list'}})
	{
		push @value,sprintf('(%u,%u,%u,%s)'
			,$compare_id
			,$credit_uid
			,time()
			,$dbh->quote($arg->{'pack'}) # очень важно квотировать
		);
	}

	$dbh->do('REPLACE INTO Compare_Credit (compare_id,credit_uid,mtime,param) VALUES '.join(',',@value)) or die $dbh->errstr;

	$dbh->do('UNLOCK TABLES') or die $dbh->errstr;

	return $compare_id;
}

# Удалить кредит из сравнения
sub delete_from_compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	$dbh->do(sprintf('DELETE FROM Compare_Credit WHERE compare_id=%u AND credit_uid=%u',
		,$arg->{'compare_id'}
		,$arg->{'credit_uid'}
	)) or die $dbh->errstr;

	return 1;
}
=pod
# Список идентификаторов кредитов, находящихся в сравнении
sub compare_list
{
  my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'compare_id'};

  my $limit = '';
  $limit = sprintf 'LIMIT %u',$arg->{'limit'} if $arg->{'limit'};

	my @result = ();

	my $sth = $dbh->prepare(sprintf('SELECT `credit_uid` From `Compare_Credit` WHERE `compare_id`=? %s',$limit)) or die $dbh->errstr;
	$sth->execute($arg->{'compare_id'}) or die $dbh->errstr;
	while (my $i=$sth->fetch)
	{
		push(@result,$i->[0]);
	}
	$sth->finish;

	return \@result;
}
=cut

# Список идентификаторов кредитов, находящихся в сравнении
sub compare_list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'compare_id'};

	my @result = ();

	my $sth = $dbh->prepare('SELECT `credit_uid` From `Compare_Credit` WHERE `compare_id`=?') or die $dbh->errstr;
	$sth->execute($arg->{'compare_id'}) or die $dbh->errstr;
	while (my $i=$sth->fetch)
	{
		push(@result,$i->[0]);
	}
	$sth->finish;

	return \@result;
}

#подсчет количества кредитов в сравнении
sub compare_count
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return 0 unless $arg->{'compare_id'};

	my $res = 0;

	my $sth = $dbh->prepare('SELECT COUNT(`credit_uid`) FROM `Compare_Credit` WHERE `compare_id`=?') or die $dbh->errstr;
	$sth->execute($arg->{'compare_id'}) or die $dbh->errstr;
	($res) = $sth->fetchrow_array;
	$sth->finish;

	return $res;
}

# Сравнение кредитов
sub compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

  my $city_uid = $arg->{'city_uid'};

	my @out=();

	my %by_param = %{$arg->{'param_by_type'}};

  # кредиты
	foreach my $pack (keys %by_param)
	{
		my $arg=$this->unpack_request('pack'=>$pack) or die;

		$arg->{'compare_uid_list'}=$by_param{$pack}; # массив uid кредитов
		$arg->{'city_uid'}=$city_uid;

		$arg->{'is_hidden'}=0;

		my $credit_search_list=$this->search($arg) or die;

		push @out,@$credit_search_list;
	}

	return \@out;
}
sub compare_by_type
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

  my $city_uid = $arg->{'city_uid'};
	# Получаем список кредитов для сравнения

	my %attr=(Slice=>{});
	my $compare=$dbh->selectall_arrayref(sprintf('SELECT credit_uid,param FROM Compare_Credit WHERE compare_id=%u',$arg->{'compare_id'}), \%attr);

	# группируем по одинаковым параметрам (для оптимизации SQL запросов)

  my %credit_types = %{$arg->{'credit_types'}};
	my %credit_by_param;

  my %offer_types = %{$arg->{'offer_types'}};
  my %offer_by_param;

  my %deposit_by_param;


  my $out = {
   'Credit2' => {}
  ,'Offer'   => {}
  ,'Deposit' => {}
  };

	foreach my $hr (@$compare)
	{
    my @a = split(/-/,$hr->{'param'},2);


    if ($offer_types{hex($a[1])})
    {
		  push @{$offer_by_param{$hr->{'param'}}}, $hr->{'credit_uid'};
    }
    elsif($credit_types{hex($a[1])} || index($a[1],'.') != -1)
    {
		  push @{$credit_by_param{$hr->{'param'}}}, $hr->{'credit_uid'};
    }
    else
    {
		  push @{$deposit_by_param{$hr->{'param'}}}, $hr->{'credit_uid'};
    }
	}

	$out->{'Credit2'} = \%credit_by_param;
	$out->{'Offer'} = \%offer_by_param;
	$out->{'Deposit'} = \%deposit_by_param;

  return $out;
}


sub get_city_stat
{
	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return '' unless $arg->{'label'};

	my @where = ();
	push @where, sprintf('A.`city_uid`=%u',$arg->{'city_uid'}) if $arg->{'city_uid'};

	return sprintf(<<'	__Q__', $dbh->quote($arg->{'label'}), (@where ? sprintf('WHERE %s',join(' && ',@where)) : ''));
		SELECT
			%s AS label,
			A.`city_uid`,
			SUM(A.`cnt`) AS value
		FROM
			`Credit2_Count` A
		%s
		GROUP BY
			A.`city_uid`
	__Q__
}


sub bank_credit_parent_list
{
	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'bank_uid'};

	my @where;
	if(ref $arg->{'allow_uids'} eq 'ARRAY' && @{$arg->{'allow_uids'}})
	{
		push @where,sprintf('A.uid IN (%s)',join(',',@{$arg->{'allow_uids'}}));
	}
	my $where = @where ? sprintf(' && %s',join(' && ',@where)) : '';

	my %attr=(Slice=>{});
	my $ar = $dbh->selectall_arrayref(sprintf(<<'	__Q__', $arg->{'bank_uid'}, $arg->{'city_uid'},$where),\%attr) or die $dbh->errstr;
		SELECT
			A.`uid`, A.`url`, A.`title`, A.`file_a`, A.`child_default_profile`, A.`parent_uid`, C.`cnt`
		FROM
			`Node` A
			INNER JOIN `Credit2_Count` C ON(A.`uid`=C.`type_uid`)
		WHERE
			A.`is_hidden`=0
			&& C.`bank_uid`=%u
			&& C.`city_uid`=%u
			&& C.`cnt`>0
			%s
		ORDER BY A.`title` ASC
	__Q__
	die $dbh->errstr if $dbh->err;

	return [] unless @$ar;

	foreach my $hr (@$ar)
	{
		Bin::unpackhash(\$hr->{'file_a'}, $hr) if exists $hr->{'file_a'};
		delete $hr->{'file_a'};
	}

	return $ar;
}


sub warn_log
{
	my $warn_code=shift;
	my $credit_uid=shift;
	my $credit_data=shift;
	my $search_arg=shift;
	my $extra=shift;

	local $Data::Dumper::Indent=0;
	local $Data::Dumper::Purity=1;

	$dbh->do('INSERT IGNORE INTO Credit_Warn_Log (ctime,warn_code,uid,selected_data,search_arg,extra) VALUES (NOW(),?,?,?,?,?)',undef
		,$warn_code
		,$credit_uid
		,Dumper($credit_data)
		,Dumper($search_arg)
		,Dumper($extra)
	) or die $dbh->errstr;
}


sub item_payments
{
	use Data::Dumper;
	no warnings 'redefine';
	local $Data::Dumper::Useqq = 1;

	local *Data::Dumper::qquote = sub {
		my $s = shift;
		return "'$s'";
	};

	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return {} unless $arg->{'uid'};

	my $where='';
	$where.= $arg->{'initial'}
		? sprintf(' AND R.payment_from<=%1$u AND (R.payment_to>%1$u OR R.payment_to=0)',$arg->{'initial'})
		: ' AND R.payment_from=0';

	# запрашиваем ставки для заданного кредита
	my $q = sprintf(<<'	__Q__', $this->{'profile'}, $arg->{'uid'}, $arg->{'currency'},$arg->{'city_uid'},$arg->{'sum'}, $arg->{'period'}, $where);
		SELECT
			R.*,
			A.`pricing_model`
		FROM
			`%s_Rate` AS R
			INNER JOIN `%1$s` AS A ON(A.`uid`=R.`credit_uid`)
		WHERE
			R.`credit_uid`=%u
			&& R.`currency`=%u
			&& R.`city_uid` IN(0,%u)
			&& R.credit_from<=%u AND (R.credit_to>%5$u OR R.credit_to=0)
			&& R.time_from<=%u   AND (R.time_to>=%6$u  OR R.time_to=0)
			%s
		ORDER BY
			R.`city_uid` DESC, R.`seq` ASC
	__Q__

	my %attr=(Slice=>{});
	my $ar=$dbh->selectall_arrayref($q, \%attr);
	die $dbh->errstr if $dbh->err;

	return {} unless @$ar;

	# проверяем наличие ставок для текущего города
	my @rate = grep { $_->{'city_uid'} != 0 } @$ar;
	@rate = @$ar unless @rate;

	# Получаем значения для переменных процентных ставок
	my $var=$this->rate_var_value();

	my @a;
	my %tmp;
	foreach my $hr (@rate)
	{
		# коррекция ставки с учетом переменной ставки
		# для каждого варианта выбираем подходящее под заданные условиям значение ставки, помещаем в _rate

		$hr->{'_rate'}=$arg->{'ndfl'} ? $hr->{'rate2'} : $hr->{'rate1'};
		my $rate_var=$arg->{'ndfl'} ? $hr->{'rate2_var'} : $hr->{'rate1_var'};

		if ($rate_var) # если ставка переменная, добавляем к _rate сегодняшнее значение переменного коэффициента
		{
			die 'Undefined rate_var_value' unless exists $var->{$rate_var};
			$hr->{'_rate'}+=$var->{$rate_var};
		}

		# Перестраиваем данные. Группируем варианты если есть плавающая ставка.
		# Фильтруем если задана максимальная ставка.
		$hr->{'float_mon'}==0
			? push @a,[ $hr ] # нет плавающей ставки (массив состоит только из одного варианта)
			: push @{$tmp{join(',', $hr->{'city_uid'}, $hr->{'currency'}, $hr->{'credit_from'}, $hr->{'credit_to'}, $hr->{'payment_from'}, $hr->{'payment_to'}, $hr->{'time_from'}, $hr->{'time_to'})}}, $hr;

	}

	# неправильно заполнены данные, если ставка плавающая поле "Ставка действует с"
	# должено начинаться с 1.
	# если ставка годовая, то, возможно данные в поле
	# "Взнос от (% ≤ ПВ)" и/или "Взнос до (ПВ < %)" указаны неверно
	if(scalar @a > 1)
	{
		warn_log(1,$arg->{'uid'},\@rate,$arg,\@a);
		@a = @a[0];
	}

	foreach my $ar (values(%tmp)) # если есть плавающая ставка, собираем все варианты описывающие ее в один массив
	{
		# сортируем по полю "Ставка действует с"
		my @b=sort { $a->{'float_mon'} <=> $b->{'float_mon'} } @$ar;

		# поиск ошибок оператора при заполнении плавающей ставки
		# если поле "Ставка действует с" фигурирует больше одного раза - оператор ошибся

		my $prev_float_mon=-1;
		foreach my $hr (@b)
		{
			if ($prev_float_mon==$hr->{'float_mon'}) # учитывая что данные отсортированы
			{
				#warn "Moneyzzz credit data error #2 (credit_uid:$credit_uid) - skip credit";
				warn_log(2,$arg->{'uid'},\@rate,$arg,\@b);
				return {};
			}
			$prev_float_mon=$hr->{'float_mon'};
		}

		# ошибка не найдена - записываем
		push @a,\@b;
	}

	my @var = (); #конечный массив ставок
	# заполняем значения по новой. В сгруппированные по плавающей ставке,
	# фильтрованные по максимальной ставке (если нужно),
	# и выбрасываем значение если ставка == 0 (например, если банк не выдает кредита без подтверждения дохода)

	foreach my $ar (@a)
	{
		# рассчитываем максимальную и минимальную процентную ставку для варианта

		my $min_rate = my $max_rate = $ar->[0]->{'_rate'};

		for (my $i=1; $i<=$#{$ar}; $i++)
		{
			my $_rate=$ar->[$i]->{'_rate'};
			$min_rate=$_rate if $min_rate > $_rate;
			$max_rate=$_rate if $max_rate < $_rate;
		}

		next if $min_rate==0; # выбрасываем т.к. ставка не может == 0 (например, банк не выдает кредита без подтверждения дохода)

		#решили отказаться т.к. может откинуть хороший вариант
		#next if exists $arg->{'max_rate'} && $max_rate > $arg->{'max_rate'}; # максимальная ставка задана - фильтруем


		# вариант подходит под условия - сохраняем его

		push @var,{
			'data'=>$ar,
			'min_rate'=>$min_rate,
			'max_rate'=>$max_rate
		};
	}

	return {} unless @var;

	my %calc_arg=( # аргументы для функции расчета графика платежей
		'сумма'   => $arg->{'sum'},
		'месяцы'  => $arg->{'period'},
		'проценты'=> undef
	);

	if(scalar(@var) > 1)
	{
		$calc_arg{'проценты'}={};
		foreach my $var (@var)
		{
			$calc_arg{'проценты'}->{$_->{'float_mon'}}=$_->{'_rate'}/100 foreach @{$var->{'data'}};
		}
	}
	else
	{
		my $var = $var[0];
		if (scalar(@{$var->{'data'}})==1)
		{
			$calc_arg{'проценты'}=$var->{'data'}->[0]->{'_rate'}/100;
		}
		else
		{
			$calc_arg{'проценты'}={};
			$calc_arg{'проценты'}->{$_->{'float_mon'}}=$_->{'_rate'}/100 foreach @{$var->{'data'}};
		}
	}

	# учет первоначального взноса
	$calc_arg{'сумма'} = $calc_arg{'сумма'} - $calc_arg{'сумма'}*$arg->{'initial'}/10000 if $arg->{'initial'};

	# расчет графика платежей

	my $calc=($arg->{'pricing_model'}==2)
		? _differentiated_payment(\%calc_arg) # дифференцированный платеж
		: _annuity_payment(\%calc_arg);  # Аннуитетный платеж или Любой

	$calc->{'сумма_кредита'} = $calc_arg{'сумма'};

	#выборка комиссий
	{
		use Basic qw(trim);

		my $sth = $dbh->prepare(<<'		__Q__') or die $dbh->errstr;
			SELECT
				A.*
			FROM
				`Credit2_Commission` A
			WHERE
				A.`credit_uid`=?
				&& A.`city_uid` IN(0,?)
				&& A.`currency`=?
			ORDER BY A.`seq`
		__Q__
		$sth->execute($arg->{'uid'}, $arg->{'city_uid'}, $arg->{'currency'}) or die $dbh->errstr;
		my $ar = $sth->fetchall_arrayref({});
		$sth->finish;

		last unless @$ar;

		# выборка комиссий по городу
		my %tmp;
		for(@$ar)
		{
			$tmp{$_->{'city_uid'}} = [] unless exists $tmp{$_->{'city_uid'}};
			push @{$tmp{$_->{'city_uid'}}}, $_;
		}

		my @comm = (!exists $tmp{$arg->{'city_uid'}})
			? @{$tmp{'0'}}
			: @{$tmp{$arg->{'city_uid'}}};

		# группировка ставок комиссий по комиссиям
		%tmp = ();
		foreach my $hr (@comm)
		{
			my $key = sprintf('%s-%u',lc(trim($hr->{'name'})),$hr->{'periodicity'});
			$tmp{$key} = [] unless exists $tmp{$key};
			push @{$tmp{$key}}, $hr;
		}

		# из каждой группы оставляем только одну запись (наиболее подходящий = с максимальными значениями: "сумма кредита от", "первоначальный взнос от", "срок кредита от")
		my @filtered = ();
		foreach my $gr (values %tmp)
		{
			my @sorted=sort { $b->{'amount_credit'} <=> $a->{'amount_credit'} || $b->{'first_percent_from'} <=> $a->{'first_percent_from'} || $b->{'period'} <=> $a->{'period'} } @$gr;
			push @filtered, $sorted[0];
		}

		#добавление ставок к платежам
		my $sum = 0;
		foreach my $hr (@filtered)
		{
			my $payment=$hr->{'amount'}; # размер платежа

			if ($payment==0) # размер платежа явно не указан
			{
				$payment=$arg->{'sum'}*$hr->{'percent'}/10000; # рассчитываем проценты от суммы кредита
				$payment=$hr->{'but_min'} if $hr->{'but_min'}!=0 && $payment < $hr->{'but_min'}; # но не менее
				$payment=$hr->{'but_max'} if $hr->{'but_max'}!=0 && $payment > $hr->{'but_max'}; # но не более
			}

			unless ($payment) # неверно указан размер комиссии
			{
				local $Data::Dumper::Indent=0;
				local $Data::Dumper::Purity=1;
				local $Data::Dumper::Useqq = 1;
				no warnings 'redefine';

				local *Data::Dumper::qquote = sub {
					my $s = shift;
					return "'$s'";
				};

				warn "Comission payment = 0; ",Dumper($hr);
				next;
			}

			if ($hr->{'periodicity'}==0) # разовый платеж
			{
				unless($calc->{'график_платежей'}->[0]->{'месяц'} == 0)
				{
					unshift @{$calc->{'график_платежей'}}, {
						'месяц' => 0,
						'долг_на_начало_месяца' => $arg->{'sum'},
						'долг_на_конец_месяца'  => $arg->{'sum'},
						'выплата_процентов'     => 0,
						'выплата_долга'         => 0,
						'доп_расходы'           => 0
					};
					#$calc->{'график_платежей'}->[0]->{'доп_расходы'} = 0;
				}

				$calc->{'график_платежей'}->[0]->{'доп_расходы'} += $payment;
				$sum += $payment;
			}
			else # ежемесячный платеж
			{
				foreach my $phr (@{$calc->{'график_платежей'}})
				{
					next if $phr->{'месяц'} == 0;
					$phr->{'доп_расходы'} = 0 unless exists $phr->{'доп_расходы'};
					$phr->{'доп_расходы'} += $payment;
					$sum += $payment;
				}
			}
		}

		if($sum)
		{
			$calc->{'сумма_комиссий'} = $sum;
			$calc->{'сумма_всех_выплат'} += $sum;
		}
	}

	return $calc;
}


sub credit_city_uid_list
{
	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'uid'};

	my $sth = $dbh->prepare(<<'	__Q__') or die $dbh->errstr;
		SELECT
			DISTINCT R.`city_uid`
		FROM
			`Credit2_Rate` R
		WHERE
			`credit_uid`=?
		ORDER BY 1 ASC
	__Q__
	$sth->execute($arg->{'uid'}) or die $dbh->errstr;
	my $ar = $sth->fetchall_arrayref([]);
	$sth->finish;

	my @out = map { $_->[0] } @$ar;

	return \@out;
}


1;
