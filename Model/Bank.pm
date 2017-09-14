package Model::Bank;

use strict;
use utf8;
use base qw(Model);
use Basic;

my $dbh=Model::dbh();

sub item
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $select='';
	my $from='';
	my $where='';

	if    ($arg->{'uid'})       { $where=sprintf('A.uid=%u',$arg->{'uid'}); }
	elsif ($arg->{'url'} ne '') { $where=sprintf('A.url=%s',$dbh->quote($arg->{'url'})); }
	else                        { return {}; }

	$where.=sprintf(' AND A.is_hidden=%u',$arg->{'is_hidden'}) if exists $arg->{'is_hidden'};

	if (exists $arg->{'role'})
	{
		$select.=',R.access_level';
		$from.=sprintf(' LEFT JOIN Role AS R ON (R.role=%s AND R.access_group=A.access_group)',$dbh->quote($arg->{'role'}));
	}

	my @addon=split(',',$arg->{'addon'});

	if (in_array('length',\@addon))
	{
		$select.=',L.total, L.visible, L.new_visible';
		$from.=' LEFT JOIN Length AS L ON (A.uid=L.uid)';
	}
	
	if(in_array('rating',\@addon))
	{
		$select .= ',RB.`rating`, RB.`rating_change`, RB.`rating_value`, RB.`rating_comment`, RB.`comment_count` as rating_comment_count';
		$from   .= ' LEFT JOIN `Bank_Rating` AS RB ON(A.`uid`=RB.`bank_uid`)';
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

	$sth->execute or die $dbh->errstr;
	my $hr=$sth->fetchrow_hashref;
	$sth->finish;

	return {} unless defined $hr;

	Bin::unpackhash(\$hr->{'file_a'}, $hr);
	Bin::unpackhash(\$hr->{'file_b'}, $hr);
	delete $hr->{'file_a'};
	delete $hr->{'file_b'};

	if ($hr->{'uid'})
	{
		my %attr=(Slice=>{});
		$hr->{'Bank_City_Ref'} = $dbh->selectall_arrayref(sprintf(
			'SELECT R.*, C.`url` AS city_url FROM Bank_City_Ref R LEFT JOIN `City` C ON(R.`city_uid`=C.`uid`) WHERE R.bank_uid=%u ORDER BY R.seq',
			$hr->{'uid'}
		), \%attr) unless $arg->{'no_city_ref'};

		$hr->{'Bank_ATM_Ref'}  = $dbh->selectall_arrayref(sprintf(
			'SELECT R.*, C.`url` AS city_url FROM Bank_ATM_Ref R LEFT JOIN `City` C ON(R.`city_uid`=C.`uid`) WHERE R.bank_uid=%u ORDER BY R.seq',
			$hr->{'uid'}
		), \%attr) unless $arg->{'no_atm_ref'};

		$hr->{'Bank_Descs'}    = $dbh->selectall_arrayref(
			'SELECT * FROM Bank_City_Desc WHERE bank_uid = ?', 
			\%attr,
			$hr->{'uid'}
		) unless $arg->{'no_desc'};
			
		$hr->{'Bank_Seo'}    = $dbh->selectall_arrayref(
			'SELECT * FROM Bank_City_Seo WHERE bank_uid = ?', 
			\%attr , 
			$hr->{'uid'}
		) unless $arg->{'no_seo'};
	}
	
	return $hr;
}

sub list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my @select=();
	my @from=();
	my @where=();
	my @group=();
	my $distinct='';

	if (exists $arg->{'city_uid'})
	{
		my $ar = $this->bank_city_list('city_uid' => $arg->{'city_uid'});
		return [] unless @$ar; # банков в городе не найдено
		push @where, sprintf('A.`uid` IN(%s)', join(',' => @$ar));
	}

	if (exists $arg->{'parent_uid'})
	{
		push @where,sprintf('A.parent_uid=%u',$arg->{'parent_uid'});
	}

	if (exists $arg->{'is_hidden'})
	{
		push @where,sprintf('A.is_hidden=%u',$arg->{'is_hidden'});
	}

	if(exists $arg->{'uid_list'} && @{$arg->{'uid_list'}})
	{
		push @where,sprintf('A.`uid` IN(%s)', join(',' => map { int($_) } @{$arg->{'uid_list'}}));
	}

	push @where, 'A.`register_year` IS NULL' if $arg->{'register_year_empty'};

	if    ($arg->{'no_select'} eq 'file_ab') { 1; }
	elsif ($arg->{'no_select'} eq 'file_b')  { push @select,'A.file_a'; }
	else                                     { push @select,'A.file_a,A.file_b'; }

	if (exists $arg->{'role'})
	{
		push @select,'R.access_level';
		push @from,sprintf('LEFT JOIN Role AS R ON (R.role=%s AND R.access_group=A.access_group)',$dbh->quote($arg->{'role'}));
	}

	my @addon=split(',',$arg->{'addon'});

	# подсчет количества строк специально вынесен над логикой, где могут добавиться LEFT JOIN, чтобы излишне не усложнять сам запрос.
	# все условия, которые могут повлиять на count, должны быть объявлены выше нижеследующего кода.

	if (exists $arg->{'limit'} && not $arg->{'no_calc_pagenav_rows'}) #подсчет кол-ва строк без лимита (для станичной навигации)
	{
		my $from=join(' ',@from);
		my $where=@where ? 'WHERE '.join(' AND ',@where) : '';

		my $sth=$this->{'dbh'}->prepare(sprintf(<<'		_QUERY_',$distinct,$this->{'profile'},$from,$where)) or die $this->{'dbh'}->errstr;
		SELECT
			COUNT(%s A.uid)
		FROM
			%s AS A
			%s
		%s
		_QUERY_

		$sth->execute or die $this->{'dbh'}->errstr;
		($this->{'rows'})=$sth->fetchrow_array;
		$sth->finish;

		return [] if $this->{'rows'}==0;
	}



	if (in_array('length',\@addon))
	{
		push @select,'L.total, L.visible, L.new_visible';
		push @from,'LEFT JOIN Length AS L ON (A.uid=L.uid)';
	}

	my %order=(
		'timestamp'	=> ['A.timestamp DESC',	'A.timestamp ASC'],
		'title'		=> ['A.title ASC',		'A.title DESC'],
		'rating'	=> ['A.`rating1` DESC',	'A.`rating1` ASC']
	);

	if(in_array('rating',\@addon))
	{
		push @select,	'R.`rating`, R.`rating_change`, R.`rating_value`, R.`rating_comment`, R.`comment_count` as rating_comment_count';
		push @from,		'LEFT JOIN `Bank_Rating` AS R ON(A.`uid`=R.`bank_uid`)';

		$order{'rating_cb'} =		['R.`rating` ASC',			'R.`rating` DESC'];
		$order{'rating_comment'} =	['R.`rating_comment` ASC',	'R.`rating_comment` DESC'];
	}

	if(in_array('rating_city',\@addon))
	{
		push @from,   sprintf('LEFT JOIN `Bank_City_Rating` AS CR ON(A.`uid`=CR.`bank_uid` && CR.`city_uid`=%u)',$arg->{'city_uid'});
		push @select, 'CR.`rating` AS city_rating, CR.`comments` AS city_comment_count';

		$order{'city_rating_comment'} = ['CR.`comments` DESC',	'CR.`comments` ASC'];
	}
	
	if(in_array('params',\@addon))
	{

		push @select,	'P.`aktiv_netto`, P.`kapitalizacia`, P.`krediti`, P.`vkladi`, P.`nadejnosti`';
		push @from,		'LEFT JOIN `bank_params` AS P ON(A.`uid`=P.`bank_uid`)';

		$order{'aktiv'} =		['P.`timestamp` DESC, P.`aktiv_netto` DESC','P.`timestamp` DESC, P.`aktiv_netto` ASC'];
		$order{'credit'} =		['P.`timestamp` DESC, P.`krediti` DESC','P.`timestamp` DESC, P.`krediti` ASC'];
		$order{'capital'} =		['P.`timestamp` DESC, P.`kapitalizacia` DESC','P.`timestamp` DESC, P.`kapitalizacia` ASC'];
		$order{'vkladi'} =		['P.`timestamp` DESC, P.`vkladi` DESC','P.`timestamp` DESC, P.`vkladi` ASC'];

	}	
		

	if(in_array('trust',\@addon))
	{
		push @from, <<'		__Q__';
			LEFT JOIN (
				SELECT
					TA.`bank_uid`, MAX(TA.`uid`) as trust_uid
				FROM
					`Trust` TA
				WHERE
					TA.`is_hidden`=0
				GROUP BY TA.`bank_uid`
			) AS TTA ON(A.`uid`=TTA.`bank_uid`)
			LEFT JOIN `Trust` TB ON(TB.`uid`=TTA.`trust_uid`)
		__Q__
		push @select, 'TB.`uid` AS trust_uid, TB.`timestamp` AS trust_timestamp, TB.`rating` AS trust_rating';

		$order{'trust'} = ['TB.`rating` DESC', 'TB.`rating` ASC'];
	}

	if ($arg->{'credits_types'} && @{$arg->{'credits_types'}} && ref $arg->{'credits_types'} eq 'ARRAY')
	{
		push @select, 'SUM(R.`cnt`) AS credits_count';
		push @from, 'LEFT JOIN Credit2_Count AS R ON (A.uid = R.bank_uid)';
		push @where,sprintf('R.`city_uid`=%u AND R.type_uid IN (%s)',$arg->{'city_uid'}, join(',' => map { int($_) } @{$arg->{'credits_types'}}));
		push @group, 'A.`uid`';		
	}

	my $direction=$arg->{'desc'} ? 1:0;



	my $select=@select ? join(',','',@select) : '';
	my $from=join(' ',@from);
	my $where=@where ? 'WHERE '.join(' AND ',@where) : '';
	my $group=@group ? 'GROUP BY '.join(',',@group) : '';  	
	my $order=exists $order{$arg->{'order'}} ? $order{$arg->{'order'}}->[$direction] : $order{'timestamp'}->[$direction];
	my $limit=exists $arg->{'limit'} ? sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'}) : '';

	my %attr=(Slice=>{});
	my $ar=$dbh->selectall_arrayref(sprintf(<<'	_QUERY_',$select,$this->{'profile'},$from,$where,$group,$order,$limit), \%attr);
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
		A.seo_allc_ru_i_id,
		A.seo_allc_spb_ru_i_id,
		A.`rating1`
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

	die $dbh->errstr if $dbh->err;

	foreach my $hr (@$ar)
	{
		Bin::unpackhash(\$hr->{'file_a'}, $hr) if exists $hr->{'file_a'};
		Bin::unpackhash(\$hr->{'file_b'}, $hr) if exists $hr->{'file_b'};
		delete($hr->{'file_a'});
		delete($hr->{'file_b'});
	}

	return $ar;
}

sub validate
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	my $post=$arg->{'heap'}->{'POST'};

	my %out=('errstr'=>[]);

	unless (length($post->{'title'})>0 && length($post->{'title'})<=255)
	{
		$out{'err'}++;
		push @{$out{'errstr'}}, 'Поле "Название" содержит недопустимое значение. Поле должно быть заполнено. Длинна значения не должна превышать 255-и символов.';
	}

	if(length($post->{'register_year'})>0 && $post->{'register_year'} !~ /^\d{4}$/)
	{
		$out{'err'}++;
		push @{$out{'errstr'}}, "Поле год регистрации должно быть пустым или содержать 4 цифры";
	}

	return \%out;
}

sub pack_file_ab
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'heap'}->{'POST'};
	my $upload=$arg->{'heap'}->{'upload'};

	my %out=();
	my $file_a='';
	my $file_b='';

	Bin::packarray([
		 'img2_uri'     , ($upload->{'img2_src'} ne '' ? $upload->{'img2_src'} : $post->{'img2_uri'})
		,'img2_title'   , $post->{'img2_title'}
		,'calculator'   , $post->{'calculator'}
		,'request_link' , $post->{'request_link'}
		,'title_seo_h1' , $post->{'title_seo_h1'}
		,'title_legal'  , $post->{'title_legal'}
		,'title2'       , $post->{'title2'} # В предложном падеже. О ком? О чём?
		,'title3'       , $post->{'title3'} # В родительном падеже (например: Чего? кредиты Альфа-Банка)
		,'licence'		, $post->{'licence'} # № лицензии
		,'prichina'		, $post->{'prichina'} # причина отзыва лицензии
		,'id_banki_ru'	, $post->{'id_banki_ru'} # Id banki.ru
		,'url_banki_ru'	, $post->{'url_banki_ru'} # url banki.ru
		,'asv'			, $post->{'asv'} #участник страхования АСВ
	], \$file_a);

	Bin::packarray([
		 'text'      , $post->{'text'}
		,'title_seo' , $post->{'title_seo'}
		,'address'   , $post->{'address'}
		,'phone'     , $post->{'phone'}
		,'email'     , $post->{'email'}
		,'web'       , $post->{'web'}
		,'http_meta' , $post->{'http_meta'}
	], \$file_b);

	$out{'file_a_ref'}=\$file_a;
	$out{'file_b_ref'}=\$file_b;

	return \%out;
}

sub _lock_tables
{
	my $this=shift;
	#my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	$dbh->do('LOCK TABLES Role READ, Unite WRITE, Child WRITE, Length WRITE, Bank WRITE, Bank_City_Ref WRITE, Bank_ATM_Ref WRITE') or die $dbh->errstr;
}

sub _remove
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	$this->SUPER::_remove($arg) or die;

	$dbh->do(sprintf('DELETE FROM Bank_City_Ref WHERE bank_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
	$dbh->do(sprintf('DELETE FROM Bank_ATM_Ref WHERE bank_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
}

sub _insert
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'post'};
	my $packed=$arg->{'packed'};

	my $rating1=''.$post->{'rating1'}; # Активы нетто (тыс. рублей). sprintf('%u') возвращает только INT4, а тут INT8. Поэтому используем другой способ экранирования.
	#$rating1=~tr/0-9//cd;
	
	$rating1=~tr/,/./;
	$rating1=int($rating1*1000/10); #Возможнен ввод дробных

	$dbh->do(
		sprintf(
			(
				'INSERT INTO %s '
				. '(uid,parent_uid,timestamp,priority,flags,is_hidden,access_group,child_default_profile,'
				. 'url,title,file_a,file_b, seo_allc_ru_i_id,seo_allc_spb_ru_i_id,rating1,register_year) '
				. 'VALUES (?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?)'
			),
			$arg->{'profile'}
		),
		undef
		,$arg->{'uid'}
		,sprintf('%u',$post->{'parent_uid'})
		,sprintf('%u',$arg->{'timestamp'})
		,sprintf('%u',$post->{'priority'})
		,sprintf('%u',$arg->{'flags'})
		,sprintf('%u',$post->{'is_hidden'})
		,$post->{'access_group'}
		,($post->{'child_default_profile'} ne '' ? $post->{'child_default_profile'} : $arg->{'profile'})
		,($post->{'url'} eq '' ? $arg->{'uid'} : $post->{'url'})
		,$post->{'title'}
		,${$packed->{'file_a_ref'}}
		,${$packed->{'file_b_ref'}}

		,sprintf('%u',$post->{'seo_allc_ru_i_id'})
		,sprintf('%u',$post->{'seo_allc_spb_ru_i_id'})
		,$rating1
		,(
			length($post->{'register_year'}) 
			? sprintf('%u', $post->{'register_year'})
			: undef
		)
	) or die $dbh->errstr;

}

sub _update
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'post'};
	my $packed=$arg->{'packed'};

	my $rating1=''.$post->{'rating1'}; # Активы нетто (тыс. рублей). sprintf('%u') возвращает только INT4, а тут INT8. Поэтому используем другой способ экранирования.
	#$rating1=~tr/0-9//cd;
	
	$rating1=~tr/,/./;
	$rating1=int($rating1*1000/10); #Возможнен ввод дробных
	
	$dbh->do(
		sprintf(
			(
				  'UPDATE %s SET parent_uid=?,timestamp=?,priority=?,flags=?,'
				. 'access_group=?,child_default_profile=?,url=?,title=?,file_a=?,'
				. 'file_b=?,seo_allc_ru_i_id=?,seo_allc_spb_ru_i_id=?,rating1=?,'
				. 'register_year=? WHERE uid=?'
			),
			$arg->{'profile'}
		),
		undef
		,sprintf('%u',$post->{'parent_uid'})
		,sprintf('%u',$arg->{'timestamp'})
		,sprintf('%u',$post->{'priority'})
		,sprintf('%u',$arg->{'flags'})
		,$post->{'access_group'}
		,($post->{'child_default_profile'} ne '' ? $post->{'child_default_profile'} : $arg->{'profile'})
		,($post->{'url'} eq '' ? $arg->{'uid'} : $post->{'url'})
		,$post->{'title'}
		,${$packed->{'file_a_ref'}}
		,${$packed->{'file_b_ref'}}

		,sprintf('%u',$post->{'seo_allc_ru_i_id'})
		,sprintf('%u',$post->{'seo_allc_spb_ru_i_id'})
		,$rating1
		,(
			length($post->{'register_year'}) 
			? sprintf('%u', $post->{'register_year'})
			: undef
		)

		,$arg->{'uid'}
	) or die $dbh->errstr;

}
sub replace_descs
{
    my $this   = shift;
    my $arg    = ref($_[0]) eq 'HASH' ? $_[0] : {@_};
    my $matrix = $arg->{'desc'};
    
    return 1 unless @$matrix;
    my $string = join ',',('(?, ?, ?, ?, ?)') x scalar @$matrix / 4;
    for (my $i = scalar @$matrix - 4; $i >= 0; $i -= 4)
    {
        splice @$matrix, $i, 0, $arg->{'uid'};
    }
    $dbh->do('DELETE FROM Bank_City_Desc WHERE bank_uid = ?', undef, $arg->{'uid'}) or die $dbh->errstr;
    $dbh->do('INSERT INTO Bank_City_Desc (bank_uid, city_uid, bank_title, body, web_page) VALUES ' . $string, undef, @$matrix) or die $dbh->errstr;
    
    return 1;
}


sub replace_seo # обновление seo текстов
{
    my $this   = shift;
    my $arg    = ref($_[0]) eq 'HASH' ? $_[0] : {@_};
    my $matrix = $arg->{'seo'};
    
    return 1 unless @$matrix;
    my $string = join ',',('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)') x scalar @$matrix / 10;
    for (my $i = scalar @$matrix - 10; $i >= 0; $i -= 10)
    {
        splice @$matrix, $i, 0, $arg->{'uid'};
    }

    $dbh->do('DELETE FROM Bank_City_Seo WHERE bank_uid = ?', undef, $arg->{'uid'}) or die $dbh->errstr;
    $dbh->do('INSERT INTO Bank_City_Seo (bank_uid, city_uid, bank_seo, autocredit_seo, ipoteka_seo,creditcard_seo,deposit_seo,potreb_seo,office_seo,atm_seo,credit_seo) VALUES ' . $string, undef, @$matrix) or die $dbh->errstr;
    
    return 1;
}

sub insert_atm_from_parser
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $heap=$arg->{'heap'};
	my $post=$heap->{'POST'};	
	my @data;
#	use Data::Dumper;
#	print '<pre>',Dumper($post),'</pre>';
	
	my $url = $this->_get_transliteration('value' => $post->{'address'}, 'exists' => $post->{'exists'});
	
	push @data, sprintf("('%u','%u','%s','%s', '%s','%s','%s','%s',   '%d','%d', '%u','%u','%s')",
		$post->{'bank_uid'} || ''#bank_uid
		,$post->{'city_uid'} || ''
		,$post->{'address'} || ''
		,$post->{'phone'} || ''

		,$post->{'working_time'} || ''
		,$post->{'lat'} || ''
		,$post->{'lng'} || ''
		,$post->{'text'} || ''
		
		,$post->{'i_lat'} || ''
		,$post->{'i_lng'} || ''

		,$post->{'district_id'} || ''
		,$post->{'id'} || ''
		,$url || ''
	);			

#	use Data::Dumper;
#	print '<pre>',Dumper(\@data),'</pre>';
	
	return 1 unless @data;

	$dbh->do('INSERT INTO Bank_ATM_Ref (bank_uid,city_uid,address,phone,working_time,lat,lng,text,i_lat,i_lng,district_id,id,url) VALUES ' . join(',',@data)) or die $dbh->errstr;

	$dbh->do('UPDATE Bank_ATM_Ref SET id=seq WHERE id=0') or die $dbh->errstr;
	
	return 1;
}




sub replace_atm # обновление банкоматов
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $matrix=$arg->{'atm'};

	my @data=();
	my %m   =();

	for (my $i=0; $i <= $#{$matrix}; $i+=9)
	{
		$m{$matrix->[$i]} = {} unless $m{$matrix->[$i]};
		my $url = $this->_get_transliteration('value' => $matrix->[$i+1], 'exists' => $m{$matrix->[$i]});

		push @data, sprintf('(%u,%u,%s,%s,%s,%s,%s,%d,%d,%s,%u,%u,%s)'
			,$arg->{'uid'}					#bank_uid
			,$matrix->[$i]					#city_uid
			,$dbh->quote($matrix->[$i+1])	#address
			,$dbh->quote($matrix->[$i+2])	#working_time
			,$dbh->quote($matrix->[$i+3])	#phone
			,$dbh->quote($matrix->[$i+4])	#lat
			,$dbh->quote($matrix->[$i+5])	#lng
            ,$matrix->[$i+4]*1000000		#i_lat
			,$matrix->[$i+5]*1000000		#i_lng
			,$dbh->quote($matrix->[$i+6])	#text
            ,$matrix->[$i+7]				#district_id
			,$matrix->[$i+8]				#id
			,$dbh->quote($url)				#url
		);
	}

	return 1 unless @data;

	$dbh->do(sprintf('DELETE FROM Bank_ATM_Ref WHERE bank_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
	$dbh->do('INSERT INTO Bank_ATM_Ref (bank_uid,city_uid,address,working_time,phone,lat,lng,i_lat,i_lng,text,district_id,id,url) VALUES ' . join(',',@data)) or die $dbh->errstr;

	# исправление ошибки оператора, если он для нескольких записей задал одинаковый id - обнуляем
	{
		my %attr=(Slice=>{});
		my $ar=$dbh->selectall_arrayref('SELECT id,count(*) as cnt from Bank_ATM_Ref where id!=0 group by id having cnt!=1', \%attr);
		die $dbh->errstr if $dbh->err;

		last unless @$ar;

		$dbh->do(sprintf('UPDATE Bank_ATM_Ref SET id=0 WHERE id IN (%s)',join(',',map {$_->{'id'}} @$ar))) or die $dbh->errstr;
	}

	$dbh->do('UPDATE Bank_ATM_Ref SET id=seq WHERE id=0') or die $dbh->errstr;

	return 1;
}


sub insert_office_from_parser
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $heap=$arg->{'heap'};
	my $post=$heap->{'POST'};	
	my @data;
#	use Data::Dumper;
#	print '<pre>',Dumper($post),'</pre>';
	
	my $url = $this->_get_transliteration('value' => $post->{'address'}, 'exists' => $post->{'exists'});
	
	push @data, sprintf("('%u','%u','%s','%s','%s','%s','%s','%s','%d','%d','%s','%u','%s','%u','%u','%u','%u','%s')",
		$post->{'bank_uid'} || ''#bank_uid
		,$post->{'city_uid'} || ''
		,$post->{'address'} || ''
		,$post->{'phone'} || ''
		,$post->{'email'} || ''
		,$post->{'web'} || ''
		,$post->{'lat'} || ''
		,$post->{'lng'} || ''
		,$post->{'i_lat'} || ''
		,$post->{'i_lng'} || ''
		,$post->{'text'} || ''
		,$post->{'main'} || ''
		,$post->{'working_time'} || ''
		,$post->{'s_natural'} || ''
		,$post->{'s_legal'} || ''
		,$post->{'district_id'} || ''
		,$post->{'id'} || ''
		,$url || ''
	);			

#	use Data::Dumper;
#	print '<pre>',Dumper(\@data),'</pre>';
	
	return 1 unless @data;

	$dbh->do('INSERT INTO Bank_City_Ref (bank_uid,city_uid,address,phone,email,web,lat,lng,i_lat,i_lng,text,main,working_time,s_natural,s_legal,district_id,id,url) VALUES ' . join(',',@data)) or die $dbh->errstr;

	$dbh->do('UPDATE Bank_City_Ref SET id=seq WHERE id=0') or die $dbh->errstr;
	
	return 1;
}


sub replace_branch
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $matrix=$arg->{'branch'};

	my @data=();
	my %m   =();

	for (my $i=0; $i <= $#{$matrix}; $i+=14)
	{
		$m{$matrix->[$i]} = {} unless $m{$matrix->[$i]};
		my $url = $this->_get_transliteration('value' => $matrix->[$i+1], 'exists' => $m{$matrix->[$i]});

		push @data, sprintf('(%u,%u,%s,%s,%s,%s,%s,%s,%d,%d,%s,%u,%s,%u,%u,%u,%u,%s)'
			,$arg->{'uid'}					#bank_uid
			,$matrix->[$i]					#city_uid
			,$dbh->quote($matrix->[$i+1])	#address
			,$dbh->quote($matrix->[$i+2])	#phone
			,$dbh->quote($matrix->[$i+3])	#email
			,$dbh->quote($matrix->[$i+4])	#web
			,$dbh->quote($matrix->[$i+5])	#lat
			,$dbh->quote($matrix->[$i+6])	#lng
			,$matrix->[$i+5]*1000000		#i_lat
			,$matrix->[$i+6]*1000000		#i_lng
			,$dbh->quote($matrix->[$i+7])	#text
			,$matrix->[$i+10]				#main
			,$dbh->quote($matrix->[$i+11])	#working_time
			,$matrix->[$i+8]				#s_natural
			,$matrix->[$i+9]				#s_legal
			,$matrix->[$i+12]				#district_id
			,$matrix->[$i+13]				#id
			,$dbh->quote($url)				#url
		);
	}

	return 1 unless @data;

	$dbh->do(sprintf('DELETE FROM Bank_City_Ref WHERE bank_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
	$dbh->do('INSERT INTO Bank_City_Ref (bank_uid,city_uid,address,phone,email,web,lat,lng,i_lat,i_lng,text,main,working_time,s_natural,s_legal,district_id,id,url) VALUES ' . join(',',@data)) or die $dbh->errstr;

	# исправление ошибки оператора, если он для нескольких записей задал одинаковый id - обнуляем
	{
		my %attr=(Slice=>{});
		my $ar=$dbh->selectall_arrayref('SELECT id,count(*) as cnt FROM Bank_City_Ref WHERE id!=0 GROUP BY id HAVING cnt!=1', \%attr);
		die $dbh->errstr if $dbh->err;

		last unless @$ar;

		$dbh->do(sprintf('UPDATE Bank_City_Ref SET id=0 WHERE id IN (%s)',join(',',map {$_->{'id'}} @$ar))) or die $dbh->errstr;
	}

	$dbh->do('UPDATE Bank_City_Ref SET id=seq WHERE id=0') or die $dbh->errstr;

	return 1;
}

sub get_city_descs
{
    my $this = shift;
    my $arg  = ref($_[0]) eq 'HASH' ? $_[0] : {@_};
    return [] unless ($arg->{'city_uid'} || $arg->{'bank_uid'});
    
    my $result = $this->{'dbcache'}->selectall_hashref('SELECT *
                                                         FROM Bank_City_Desc
                                                         WHERE bank_uid = ? AND city_uid = ?',
                                                         'city_uid',
                                                         undef
                                                         $this->{'uid'});
    return $result;
}

sub bank_city_list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	return [] unless $arg->{'city_uid'};
	
	my %attr=(Slice=>[], CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['City','Bank']);
	my $ar=$this->{'dbcache'}->selectall_arrayref(sprintf('SELECT DISTINCT bank_uid FROM Bank_City_Ref WHERE city_uid=%u',$arg->{'city_uid'}), \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;
	
	return [] unless @$ar;
	
	my @out = map { int($_->[0]) } @$ar;
	return \@out;
}

# Наоборот возвращает список идентификаторов городов по идентификатору банка
sub city_bank_list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	return [] unless $arg->{'bank_uid'};
	
	my %attr=(Slice=>[], CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['City','Bank']);
	my $ar=$this->{'dbcache'}->selectall_arrayref(sprintf('SELECT DISTINCT city_uid FROM Bank_City_Ref WHERE bank_uid=%u',$arg->{'bank_uid'}), \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;
	
	return [] unless @$ar;
	
	my @out = map { int($_->[0]) } @$ar;
	return \@out;
}


sub city_bank_list_ext
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	return [] unless $arg->{'bank_uid'};

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['City','Bank']);
	my $ar=$this->{'dbcache'}->selectall_arrayref(sprintf(<<'	__Q__',$arg->{'bank_uid'}), \%attr);
		SELECT 
			city_uid,
			COUNT(`id`) AS cnt 
		FROM 
			Bank_City_Ref 
		WHERE 
			bank_uid=%u 
		GROUP BY `city_uid`;
	__Q__
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;
	
	return $ar;
}


#sub list_office
#{
#	my $this=shift;
#	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
#	
#	my @where = ();
#	#push @where, sprintf('A.`bank_uid`=%u',$arg->{'bank_uid'}) if $arg->{'bank_uid'};
#	push @where, sprintf('A.`city_uid`=%u',$arg->{'city_uid'}) if $arg->{'city_uid'};
#    push @where, sprintf('A.district_id IN ');
#	#push @where, sprintf('A.`district_id`=%u',$arg->{'district_id'}) if $arg->{'district_id'};
#	push @where, sprintf('A.`district_id` IN(%s)', join(',' => map { int($_) } @{$arg->{'district_id_list'}})) if($arg->{'district_id_list'} && ref($arg->{'district_id_list'}) eq 'ARRAY');
#	push @where, 'A.`s_natural`=1'	if $arg->{'natural'};
#	push @where, 'A.`s_legal`=1'	if $arg->{'legal'};
#	
#	my $w = @where ? sprintf('WHERE %s', join(' && ' => @where)) : '';
#	
#	if (exists $arg->{'limit'} && not $arg->{'no_calc_pagenav_rows'})
#	{
#		my $sth = $dbh->prepare(sprintf(<<'		__Q__', $w)) or die $dbh->errstr;
#			SELECT
#				COUNT(A.`id`)
#			FROM
#				`Bank_City_Ref` A
#			%s
#		__Q__
#		$sth->execute or die $this->{'dbh'}->errstr;
#		($this->{'rows'})=$sth->fetchrow_array;
#		$sth->finish;
#
#		return [] if $this->{'rows'}==0;
#	}
#	
#	my $limit = ($arg->{'limit'})
#		? (
#			$arg->{'offset'}
#			? sprintf('LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'})
#			: sprintf('LIMIT %u', $arg->{'limit'})
#		)
#		: '';
#		
#	my $ar = $dbh->selectall_arrayref(sprintf(<<'	__Q__', $w,$limit), {'Slice'=>{}});
#		SELECT
#			A.*
#		FROM
#			`Bank_City_Ref` A
#		%s
#		ORDER BY A.`bank_uid` ASC, A.`address` ASC
#		%s
#	__Q__
#	
#	die $dbh->errstr if $dbh->err;
#	
#	return $ar;
#}

sub list_office_new
{
	my $self  = shift;
	my $arg   = ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my @where = (
		'B.`is_hidden`=0'
	);
	my @sql_bind;

	if ($arg->{'bank_uid'})
	{
		push @where, 'A.bank_uid = ? ';
		push @sql_bind , $arg->{'bank_uid'};
	}

	if ($arg->{'city_uid'})
	{
		push @where, 'A.city_uid = ? ';
		push @sql_bind, $arg->{'city_uid'};
	}

	if ($arg->{'bank_uid_list'} && ref($arg->{'bank_uid_list'}) eq 'ARRAY')
	{
		push @where, sprintf('A.bank_uid IN (%s)', join ',', ('?') x scalar @{$arg->{'bank_uid_list'}});
		push @sql_bind, @{$arg->{'bank_uid_list'}};
	}

	if ($arg->{'district_id_list'} && ref($arg->{'district_id_list'}) eq 'ARRAY')
	{
		push @where, sprintf('A.district_id IN (%s)', join ',', ('?') x scalar @{$arg->{'district_id_list'}});
		push @sql_bind, @{$arg->{'district_id_list'}};
	}

    if ($arg->{'except'})
    {
        push @where, 'A.id != ? ';
        push @sql_bind, $arg->{'except'};
    }		
	
	if (defined($arg->{'crd'}))
	{
		my @crd_where;

		for my $crd (@{$arg->{'crd'}})
		{
			my ($lat1, $lat2, $lng1, $lng2);

			$lat1 = int (($crd->[0] - 1 / 111.120) * 1000000);
			$lat2 = int (($crd->[0] + 1 / 111.120) * 1000000);
			$lng1 = int (($crd->[1] - 1 / abs(cos($crd->[0]) * 111.120)) * 1000000);
			$lng2 = int (($crd->[1] + 1 / abs(cos($crd->[0]) * 111.120)) * 1000000);

			push @crd_where, '( A.i_lat BETWEEN ? AND ? AND A.i_lng BETWEEN ? AND ? )';
			push @sql_bind, ($lat1, $lat2, $lng1, $lng2);
		}

		if (scalar @crd_where > 0)
		{
			my $crd_where = join ' OR ' , @crd_where;
			push @where, '(' . $crd_where . ')';
		}
	}

	push @where, 'A.s_natural=1' if $arg->{'natural'};
	push @where, 'A.s_legal=1'	 if $arg->{'legal'};

	my $w = @where ? sprintf('WHERE %s', join(' AND ' => @where)) : '';

	if (exists $arg->{'limit'} && not $arg->{'no_calc_pagenav_rows'})
	{
		my $sth = $dbh->prepare('SELECT COUNT(A.id) FROM Bank_City_Ref A LEFT JOIN `Bank` B ON(A.`bank_uid`=B.`uid`) ' . $w);
		$sth->execute(@sql_bind) or die $dbh->errstr;
		($self->{'rows'})=$sth->fetchrow_array;
		$sth->finish;
		return [] if $self->{'rows'} == 0;
	}

	my $limit = ($arg->{'limit'})
		? (
			$arg->{'offset'}
			? sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'})
			: sprintf(' LIMIT %u', $arg->{'limit'})
		)
		: '';

	my $query = (
		'SELECT A.*, B.title, B.url AS bank_url, C.`url` AS city_url, C.`title` AS city_title '
			. ' FROM Bank_City_Ref A '
			. ' LEFT JOIN Bank B ON(B.uid = A.bank_uid) '
			. ' LEFT JOIN `City` C ON(A.`city_uid`=C.`uid`) '
			. $w
			. ' ORDER BY B.title ASC, A.address ASC '
			. $limit
	);

	my $ar    = $dbh->selectall_arrayref($query, { Slice => {} }, @sql_bind);
	die $dbh->errstr if $dbh->err;

	return $ar;

}

sub list_atm
{
	my $self  = shift;
	my $arg   = ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my @where = (
		'B.is_hidden = 0'
	);
	my @sql_bind;

	if ($arg->{'bank_uid'})
	{
		push @where, 'A.bank_uid = ? ';
		push @sql_bind , $arg->{'bank_uid'};
	}

	if ($arg->{'bank_uid_list'} && ref($arg->{'bank_uid_list'}) eq 'ARRAY')
	{
		push @where, sprintf('A.bank_uid IN (%s)', join ',', ('?') x scalar @{$arg->{'bank_uid_list'}});
		push @sql_bind, @{$arg->{'bank_uid_list'}};
	}
	
	if ($arg->{'city_uid'})
	{
		push @where, 'A.city_uid = ? ';
		push @sql_bind, $arg->{'city_uid'};
	}
	
	if ($arg->{'district_id_list'} && ref($arg->{'district_id_list'}) eq 'ARRAY')
	{
		push @where, sprintf('A.district_id IN (%s)', join ',', ('?') x scalar @{$arg->{'district_id_list'}});
		push @sql_bind, @{$arg->{'district_id_list'}};
	}
	
    if ($arg->{'except'})
    {
        push @where, 'A.id != ? ';
        push @sql_bind, $arg->{'except'};
    }		
	
	if (defined($arg->{'crd'}))
	{
		my @crd_where;
		for my $crd (@{$arg->{'crd'}})
		{
			my ($lat1, $lat2, $lng1, $lng2);
			
			$lat1 = int (($crd->[0] - 1 / 111.120) * 1000000);
			$lat2 = int (($crd->[0] + 1 / 111.120) * 1000000);
			$lng1 = int (($crd->[1] - 1 / abs(cos($crd->[0]) * 111.120)) * 1000000);
			$lng2 = int (($crd->[1] + 1 / abs(cos($crd->[0]) * 111.120)) * 1000000);
			push @crd_where, '( A.i_lat BETWEEN ? AND ? AND A.i_lng BETWEEN ? AND ? )';
			push @sql_bind, ($lat1, $lat2, $lng1, $lng2);
		}
		if (scalar @crd_where > 0)
		{
			my $crd_where = join ' OR ' , @crd_where;
			push @where, '(' . $crd_where . ')';
		}
	}
	my $w = @where ? sprintf('WHERE %s', join(' AND ' => @where)) : '';
	
	if (exists $arg->{'limit'} && not $arg->{'no_calc_pagenav_rows'})
	{
		my $sth = $dbh->prepare('SELECT COUNT(A.id) FROM Bank_ATM_Ref A LEFT JOIN `Bank` B ON(A.`bank_uid`=B.`uid`) ' . $w);
		$sth->execute(@sql_bind) or die $dbh->errstr;
		($self->{'rows'})=$sth->fetchrow_array;
		$sth->finish;

		return [] if $self->{'rows'} == 0;
	}
	my $limit = ($arg->{'limit'})
		? (
			$arg->{'offset'}
			? sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'})
			: sprintf(' LIMIT %u', $arg->{'limit'})
		)
		: '';
	
	my $query = (
		'SELECT A.*, B.title, B.url AS bank_url, C.`url` AS city_url, C.`title` AS city_title '
			. ' FROM Bank_ATM_Ref A '
			. ' LEFT JOIN Bank B ON(B.uid = A.bank_uid) '
			. ' LEFT JOIN `City` C ON(A.`city_uid`=C.`uid`) '
			. $w
			. ' ORDER BY B.title ASC, A.address ASC '
			. $limit
	);
	my $ar    = $dbh->selectall_arrayref($query, { Slice => {} }, @sql_bind);
	die $dbh->errstr if $dbh->err;

	return $ar;
}

sub item_office
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	my $where;
	if($arg->{'id'})
	{
		$where = sprintf('A.`id`=%u',$arg->{'id'});
	}
	elsif($arg->{'url'} && $arg->{'bank_uid'} && $arg->{'city_uid'})
	{
		$where = sprintf('A.`bank_uid`=%u && A.`city_uid`=%u && A.`url`=%s', $arg->{'bank_uid'}, $arg->{'city_uid'}, $dbh->quote($arg->{'url'}));
	}
	
	return {} unless $where;
	
	my $sth = $dbh->prepare(sprintf(<<'	__Q__',$where)) or die $dbh->errstr;
		SELECT
			A.*, B.`title` AS bank_title, B.`url` AS bank_url, C.`url` AS city_url, C.`title` AS city_title
		FROM
			`Bank_City_Ref` A
			INNER JOIN `Bank` B ON(A.`bank_uid` = B.`uid`)
			LEFT  JOIN `City` C ON(A.city_uid=C.`uid`)
		WHERE
			%s
	__Q__
	$sth->execute($arg->{'id'}) or die $dbh->errstr;
	my $hr = $sth->fetchrow_hashref;
	$sth->finish;

	return {} unless defined $hr;
	
	return $hr;
}


sub item_atm
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	my $where;
	if($arg->{'id'})
	{
		$where = sprintf('A.`id`=%u',$arg->{'id'});
	}
	elsif($arg->{'url'} && $arg->{'bank_uid'} && $arg->{'city_uid'})
	{
		$where = sprintf('A.`bank_uid`=%u && A.`city_uid`=%u && A.`url`=%s', $arg->{'bank_uid'}, $arg->{'city_uid'}, $dbh->quote($arg->{'url'}));
	}
	
	return {} unless $where;
	
	my $sth = $dbh->prepare(sprintf(<<'	__Q__',$where)) or die $dbh->errstr;
		SELECT
			A.*, B.title, B.`url` AS bank_url, C.`url` AS city_url, C.`title` AS city_title
		FROM
			`Bank_ATM_Ref` A
			INNER JOIN `Bank` B ON(A.bank_uid = B.uid)
			LEFT  JOIN `City` C ON(A.city_uid=C.`uid`)
		WHERE
			%s
	__Q__
	$sth->execute() or die $dbh->errstr;
	my $hr = $sth->fetchrow_hashref;
	$sth->finish;

	return {} unless defined $hr;
	
	return $hr;
}


sub statistics
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %type = map { $_ => 1} split(',' => $arg->{'type'});

	my %sarg = ();
	$sarg{'city_uid'} = $arg->{'city_uid'} if $arg->{'city_uid'};
	$sarg{'bank_uid'} = $arg->{'bank_uid'} if $arg->{'bank_uid'};

	my @sql = ();
	my @tags= ('Bank');

	if($type{'atm'})
	{
		$sarg{'label'} = 'bank_atm';
		push @sql, $this->get_bank_stat_atm(\%sarg);
	}
	if($type{'office'})
	{
		$sarg{'label'} = 'bank_office';
		push @sql, $this->get_bank_stat_office(\%sarg);
	}
	if($type{'news'})
	{
		$sarg{'label'} = 'bank_news';
		push @tags, 'News';
		push @sql, $this->base_model()->obj('News')->get_bank_stat(\%sarg);
	}
	if($type{'news_ext'})
	{
		$sarg{'label'} = 'bank_news';
		push @tags, 'News';
		push @sql, $this->base_model()->obj('News')->get_bank_stat(\%sarg);
	}
	if($type{'credit'})
	{
		$sarg{'label'} = 'bank_credit';
		push @tags, 'Credit2';
		push @sql, $this->base_model()->obj('Credit2')->get_bank_stat(\%sarg);
	}
	if($type{'deposit'})
	{
		$sarg{'label'} = 'bank_deposit';
		push @tags, 'Deposit';
		push @sql, $this->base_model()->obj('Deposit')->get_bank_stat(\%sarg);
	}
	if($type{'comment'})
	{
		$sarg{'label'} = 'bank_comment';
		push @tags, 'CommentBank';
		push @sql, $this->base_model()->obj('CommentBank')->get_bank_stat(\%sarg);
	}
	if($type{'faq'})
	{
		$sarg{'label'} = 'bank_faq';
		push @tags, 'Faq';
		push @sql, $this->base_model()->obj('Faq')->get_bank_stat(\%sarg);
	}
	if($type{'trust'})
	{
		$sarg{'label'} = 'bank_trust';
		push @tags, 'Trust';
		push @sql, $this->base_model()->obj('Trust')->get_bank_stat(\%sarg);
	}

	@sql = grep { $_ ne '' } @sql;

	return [] unless @sql;

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>20*60, CacheTags=>\@tags);
	my $ar = $this->{'dbcache'}->selectall_arrayref(join(' UNION ' => @sql), \%attr);
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;

	return $ar;
}


sub get_bank_stat_atm
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return '' unless $arg->{'label'} && $arg->{'bank_uid'};

	my @where = (
		sprintf('A.`bank_uid`=%u',$arg->{'bank_uid'})
	);
	my $sql;

	if($arg->{'city_uid'})
	{
		push @where, sprintf('A.`city_uid`=%u', $arg->{'city_uid'});
		$sql = sprintf(<<'		__Q__', $dbh->quote($arg->{'label'}), join('&&' => @where));
			SELECT 
				%s AS label,
				A.`city_uid`,
				COUNT(DISTINCT A.`id`) AS value
			FROM
				`Bank_ATM_Ref` A
			WHERE
				%s
			GROUP BY A.`city_uid`
		__Q__
	}
	else
	{
		$sql = sprintf(<<'		__Q__', $dbh->quote($arg->{'label'}), $dbh->quote('0'), join('&&' => @where));
			SELECT
				%s AS label,
				%s AS city_uid,
				COUNT(DISTINCT A.`id`) AS value
			FROM
				`Bank_ATM_Ref` A
			WHERE
				%s
			GROUP BY A.`bank_uid`
		__Q__
	}

	return $sql;
}


sub get_bank_stat_office
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return '' unless $arg->{'label'} && $arg->{'bank_uid'};

	my @where = (
		sprintf('A.`bank_uid`=%u',$arg->{'bank_uid'})
	);
	my $sql;
	if($arg->{'city_uid'})
	{
		push @where, sprintf('A.`city_uid`=%u', $arg->{'city_uid'});
		$sql = sprintf(<<'		__Q__', $dbh->quote($arg->{'label'}), join('&&' => @where));
			SELECT 
				%s AS label,
				A.`city_uid`,
				COUNT(DISTINCT A.`id`) AS value
			FROM
				`Bank_City_Ref` A
			WHERE
				%s
			GROUP BY A.`city_uid`
		__Q__
	}
	else
	{
		$sql = sprintf(<<'		__Q__', $dbh->quote($arg->{'label'}), $dbh->quote('0'), join('&&' => @where));
			SELECT 
				%s AS label,
				%s AS city_uid,
				COUNT(DISTINCT A.`id`) AS value
			FROM
				`Bank_City_Ref` A
			WHERE
				%s
			GROUP BY A.`bank_uid`
		__Q__
	}

	return $sql;
}


sub update_atm_latlng
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return 0 unless $arg->{'id'};
	return 0 unless $arg->{'lat'} && $arg->{'lng'};

	my @ids = ();

	if(ref($arg->{'id'}) eq 'ARRAY')
	{
		@ids = map { int($_) } @{$arg->{'id'}};
	}
	else
	{
		push(@ids,int($arg->{'id'}));
	}

	@ids = grep { $_ > 0 } @ids;

	return 0 unless @ids;

	my $sql = <<'	__Q__';
		UPDATE
			`Bank_ATM_Ref`
		SET
			`lat`=%s,
			`lng`=%s,
			`i_lat`=%d,
			`i_lng`=%d
		WHERE
			`id` IN(%s)
			%s
		LIMIT %u
	__Q__

	#$this->{'dbh'}->do('LOCK TABLES `%s`.`Bank_ATM_Ref` WRITE') or die $this->{'dbh'}->errstr;

	$this->{'dbh'}->do(
		sprintf(
			$sql,
			$this->{'dbh'}->quote($arg->{'lat'}),
			$this->{'dbh'}->quote($arg->{'lng'}),
			$this->{'dbh'}->quote($arg->{'lat'} * 1000000),
			$this->{'dbh'}->quote($arg->{'lng'} * 1000000),
			join(','=>@ids),
			(
				exists $arg->{'straight'}
				? ''
				: q( && `lat`='' && `lng`='' )
			),
			scalar(@ids)
		)
	) or die $this->{'dbh'}->errstr;

	$this->{'dbh'}->do('UNLOCK TABLES');

	$this->geocode_log('id' => \@ids, 'type' => 'atm');

	return 1;
}


sub update_office_latlng
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return 0 unless $arg->{'id'};
	return 0 unless $arg->{'lat'} && $arg->{'lng'};

	my @ids = ();

	if(ref($arg->{'id'}) eq 'ARRAY')
	{
		@ids = map { int($_) } @{$arg->{'id'}};
	}
	else
	{
		push(@ids,int($arg->{'id'}));
	}

	@ids = grep { $_ > 0 } @ids;

	return 0 unless @ids;

	my $sql = <<'	__Q__';
		UPDATE
			`Bank_City_Ref`
		SET
			`lat`=%s,
			`lng`=%s,
			`i_lat`=%d,
			`i_lng`=%d
		WHERE
			`id` IN(%s)
			%s
		LIMIT %u
	__Q__

	$this->{'dbh'}->do('LOCK TABLES `Bank_City_Ref` WRITE') or die $this->{'dbh'}->errstr;

	$this->{'dbh'}->do(
		sprintf(
			$sql,
			$this->{'dbh'}->quote($arg->{'lat'}),
			$this->{'dbh'}->quote($arg->{'lng'}),
			$this->{'dbh'}->quote($arg->{'lat'} * 1000000),
			$this->{'dbh'}->quote($arg->{'lng'} * 1000000),
			join(','=>@ids),
			(
				exists $arg->{'straight'}
				? ''
				: q( && `lat`='' && `lng`='' )
			),
			scalar(@ids)
		)
	) or die $this->{'dbh'}->errstr;

	$this->{'dbh'}->do('UNLOCK TABLES');

	$this->geocode_log('id' => \@ids, 'type' => 'office');

	return 1;
}


sub geocode_log
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return 0 unless($arg->{'id'} && $arg->{'type'});

	my @ids = ref $arg->{'id'} eq 'ARRAY' ? @{$arg->{'id'}} : ($arg->{'id'});

	return 0 unless @ids;

	$this->{'dbh'}->do('LOCK TABLES `Geocoder_Log` WRITE',) or die $this->{'dbh'}->errstr;

	for(@ids)
	{
		$this->{'dbh'}->do(
			'REPLACE INTO `Geocoder_Log`(`id`,`type`,`timestamp`) VALUES(?,?,UNIX_TIMESTAMP())',
			undef,
			$_, 
			(
				$arg->{'type'} eq 'atm'
				? 'atm'
				: 'office'
			)
		) or die $this->{'dbh'}->errstr;
	}

	$this->{'dbh'}->do('UNLOCK TABLES') or die $this->{'dbh'}->errstr;

	return 1;
}


sub get_city_stat
{
	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return '' unless $arg->{'label'};

	my @where = ('A.`is_hidden`=0');
	push @where, sprintf('B.`city_uid`=%u',$arg->{'city_uid'}) if $arg->{'city_uid'};

	return sprintf(<<'	__Q__', $dbh->quote($arg->{'label'}), $this->{'profile'}, (@where ? sprintf('WHERE %s',join(' && ',@where)) : ''));
		SELECT
			%s AS label,
			B.`city_uid`,
			COUNT(DISTINCT A.`uid`) AS value
		FROM
			`%s` A
			INNER JOIN `Bank_City_Ref` B ON(A.`uid`=B.`bank_uid`)
		%s
		GROUP BY
			B.`city_uid`
	__Q__
}


sub geocode_atm
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'limit'};

	my $sth = $this->{'dbh'}->prepare(<<"	__Q__") or die $this->{'dbh'}->errstr;
		SELECT 
			A.address, COUNT(A.id) AS cnt, GROUP_CONCAT(A.id) AS ids, C.title, C.uid as City_uid
		FROM Bank_ATM_Ref A
		LEFT JOIN Geocoder_Log B ON (B.type = 'atm' && A.id = B.id)
		LEFT JOIN City C ON (C.uid = A.city_uid)
		WHERE 
			B.id IS NULL
			&& A.address <> '' 
			&& A.lat = '' 
			&& A.lng = '' 
		GROUP BY A.address 
		ORDER BY ? DESC
		LIMIT ?
	__Q__
	$sth->execute(($arg->{'rand'} ? 'RAND()':'cnt'), $arg->{'limit'}) or die $this->{'dbh'}->errstr;
	my $ar = $sth->fetchall_arrayref({});
	$sth->finish;

	return $ar;
}

sub geocode_office
{
	my $this = shift;
	my $arg = ref ($_[0]) eq 'HASH' ? $_[0] : {@_};

	return [] unless $arg->{'limit'};

	my $sth = $this->{'dbh'}->prepare(<<"	__Q__") or die $this->{'dbh'}->errstr;
		SELECT 
			A.address, COUNT(A.id) AS cnt, GROUP_CONCAT(A.id) AS ids, C.title, C.uid as City_uid
		FROM Bank_City_Ref A
		LEFT JOIN Geocoder_Log B ON (B.type = 'office' && A.id = B.id)
		LEFT JOIN City C ON (C.uid = A.city_uid)
		WHERE 
			B.id IS NULL
			&& A.address <> '' 
			&& A.lat = '' 
			&& A.lng = '' 
		GROUP BY A.address 
		ORDER BY ? DESC
		LIMIT ?
	__Q__
	$sth->execute(($arg->{'rand'} ? 'RAND()':'cnt'), $arg->{'limit'}) or die $this->{'dbh'}->errstr;
	my $ar = $sth->fetchall_arrayref({});
	$sth->finish;

	return $ar;
}


sub neighbours
{
	my $this = shift;
	my $arg = ref ($_[0]) eq 'HASH' ? $_[0] : {@_};

	return {} unless $arg->{'bank_uid'};

	my @from = ();
	my @where = ('A.`is_hidden`=0');

	push @where, sprintf('A.`uid` IN(%s)', join(',' => @{ $this->bank_city_list('city_uid' => $arg->{'city_uid'} ) })) if $arg->{'city_uid'};

	if($arg->{'credit_type'} && $arg->{'city_uid'})
	{
		push @from,		sprintf('INNER JOIN `Credit2_Count` CC ON(A.`uid`=CC.`bank_uid` && CC.`city_uid`=%u)',$arg->{'city_uid'});
		push @where,	'CC.`cnt`>0', sprintf('CC.`type_uid`=%u',$arg->{'credit_type'});
	}

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['Bank']);
	my $ar=$this->{'dbcache'}->selectall_arrayref(sprintf(<<'	__Q__', $this->{'profile'}, join(' ' => @from), join(' && ' => @where)),\%attr);
		SELECT
			A.`uid`
		FROM
			`%s` A
			%s
		WHERE
			%s
		ORDER BY A.`title` ASC
	__Q__
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;
	
	return {} unless @$ar;

	my %out=();
	my $i=0;
	foreach my $hr (@$ar)
	{
		$i++, next unless $hr->{'uid'} == $arg->{'bank_uid'};

		$out{'prev'} = $ar->[$i-1]->{'uid'} if $ar->[$i-1];
		$out{'next'} = $ar->[$i+1]->{'uid'} if $ar->[$i+1];

		return \%out;
	}

	return {};
}


sub _get_transliteration
{
	my $this = shift;
	my $arg = ref ($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $key = lc Basic::transliteration2(trim($arg->{'value'}));
	$key = substr($key,0,117) if length $key > 117;
	$key =~ s/_{2,}/_/g;
	$key = 'default' unless $key;
	$key = 'default' if $key =~ m/^\d+$/;

	if($arg->{'exists'}->{$key})
	{
		my $i=1;
		{
			my $kk = sprintf('%s_%u',$key,$i);
			$key = $kk, last unless $arg->{'exists'}->{$kk};

			$i++;
			redo;
		}
	}

	$arg->{'exists'}->{$key} = 1;

	return $key;
}



sub validate_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return {} unless $arg->{'q'};
	$arg->{'q'} = lc trim($arg->{'q'});
	$arg->{'q'} =~ s|[^a-zа-я\d- ]||g;

	return $arg;
}

sub bank_rating
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	my $where;
	if($arg->{'bank_uid'})
	{
		$where = sprintf('A.`bank_uid`=%u', $arg->{'bank_uid'});
	}
	
	return {} unless $where;
	
	my $sth = $dbh->prepare(sprintf(<<'	__Q__',$where)) or die $dbh->errstr;
		SELECT
			A.*
		FROM
			`bank_params` A
		WHERE
			%s
		ORDER BY timestamp DESC
	__Q__
	$sth->execute() or die $dbh->errstr;
	my $hr = $sth->fetchrow_hashref;
	$sth->finish;

	return {} unless defined $hr;

	return $hr;
}


1;
