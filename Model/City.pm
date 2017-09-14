package Model::City;

use strict;
use base qw(Model);
use utf8;
use Basic;
use Math;

my $dbh=Model::dbh();

sub item
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};



	my $hr=$this->SUPER::item($arg) or die;

	if ($hr->{'uid'} && !$arg->{'only_city'})
	{
		my %attr=(Slice=>{});
		$hr->{'City_Subway'}=$dbh->selectall_arrayref(sprintf('SELECT * FROM City_Subway WHERE city_uid=%u',$hr->{'uid'}), \%attr);
		$hr->{'City_District'}=$dbh->selectall_arrayref(sprintf('SELECT * FROM City_District WHERE city_uid=%u ORDER BY `district`',$hr->{'uid'}), \%attr);
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


	unless (exists $arg->{'all'})
	{		
		if (exists $arg->{'priority'})
		{
			push @where,sprintf('A.priority=%u',$arg->{'priority'});
		}
		else
		{
			push @where,'A.priority=0 OR A.priority=1';
		}
	}

	if (exists $arg->{'query'})
	{
		push @where,sprintf('A.title LIKE "%\%s%"',$arg->{'query'});
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


	if    ($arg->{'no_select'} eq 'file_ab') { 1; }
	elsif ($arg->{'no_select'} eq 'file_b')  { push @select,'A.file_a'; }
	else                                     { push @select,'A.file_a,A.file_b'; }

	if (exists $arg->{'role'})
	{
		push @select,'R.access_level';
		push @from,sprintf('LEFT JOIN Role AS R ON (R.role=%s AND R.access_group=A.access_group)',$dbh->quote($arg->{'role'}));
	}

	my @addon=split(',',$arg->{'addon'});

	if (in_array('length',\@addon))
	{
		push @select,'L.total, L.visible, L.new_visible';
		push @from,'LEFT JOIN Length AS L ON (A.uid=L.uid)';
	}

	my %order=(
		'timestamp' => ['A.timestamp DESC', 'A.timestamp ASC'],
		'title'     => ['A.title ASC',      'A.title DESC'],
		'seq'       => ['A.seq ASC',        'A.seq DESC'],
	);

	my $direction=$arg->{'desc'} ? 1:0;

	my $select=@select ? join(',','',@select) : '';
	my $from=join(' ',@from);
	my $where=@where ? 'WHERE '.join(' AND ',@where) : '';
	my $order=exists $order{$arg->{'order'}} ? $order{$arg->{'order'}}->[$direction] : $order{'timestamp'}->[$direction];
	my $limit=exists $arg->{'limit'} ? sprintf(' LIMIT %u,%u', $arg->{'offset'}, $arg->{'limit'}) : '';

	my %attr=(Slice=>{});
	my $ar=$dbh->selectall_arrayref(sprintf(<<'	_QUERY_',$select,$this->{'profile'},$from,$where,$order,$limit), \%attr);
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
		A.title
		%s
	FROM
		%s AS A
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
		delete $hr->{'file_a'};
		delete $hr->{'file_b'};
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
		push @{$out{'errstr'}}, 'Поле "Название города, в именительном падеже" содержит недопустимое значение. Поле должно быть заполнено. Длинна значения не должна превышать 255-и символов.';
	}

	unless (length($post->{'title2'})>0 && length($post->{'title2'})<=255)
	{
		$out{'err'}++;
		push @{$out{'errstr'}}, 'Поле "Название города, в предложном падеже" содержит недопустимое значение. Поле должно быть заполнено. Длинна значения не должна превышать 255-и символов.';
	}

	unless (length($post->{'title3'})>0 && length($post->{'title3'})<=255)
	{
		$out{'err'}++;
		push @{$out{'errstr'}}, 'Поле "Название города, в родительном падеже" содержит недопустимое значение. Поле должно быть заполнено. Длинна значения не должна превышать 255-и символов.';
	}

	unless ($post->{'lat'} =~ /^\d+.\d+$/ && $post->{'lng'} =~ /^\d+.\d+$/)
	{
		$out{'err'}++;
		push @{$out{'errstr'}}, 'Координаты должны быть в формате XX.XXXXXXX';
	}
    
	return \%out;
}

sub pack_file_ab
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'heap'}->{'POST'};

	my %out=();
	my $file_a='';
	my $file_b='';

	Bin::packarray([
		'title2'	, $post->{'title2'},
		'title3'	, $post->{'title3'},
		'r_id'		, $post->{'r_id'},
		'i_id'		, $post->{'i_id'},

		'lng'		, $post->{'lng'},
		'lat'		, $post->{'lat'},

		'x_coord'	, $post->{'x_coord'},
		'y_coord'	, $post->{'y_coord'},
		'big_city'	, $post->{'big_city'},
		'counter'	, $post->{'counter'},
		'actual_extent' , $post->{'actual_extent'},
		'road_extent' , $post->{'road_extent'},
		'2gis_id' , $post->{'2gis_id'},
		'geo_id' , $post->{'geo_id'},
		'area_name' , $post->{'area_name'}
	], \$file_a);
	#Bin::packarray([], \$file_b);

	$out{'file_a_ref'}=\$file_a;
	$out{'file_b_ref'}=\$file_b;

	return \%out;
}

sub _lock_tables
{
	my $this=shift;
	#my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	$dbh->do('LOCK TABLES Role READ, Unite WRITE, Child WRITE, Length WRITE, City WRITE, Bank_City_Ref WRITE, Credit_City_Ref WRITE') or die $dbh->errstr;
}

sub _remove
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	$this->SUPER::_remove($arg) or die;

	$dbh->do(sprintf('DELETE FROM Bank_City_Ref WHERE city_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
	$dbh->do(sprintf('DELETE FROM Credit_City_Ref WHERE city_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
}

sub _insert
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'post'};
	my $packed=$arg->{'packed'};

	$this->{'dbh'}->do('INSERT INTO City (uid,parent_uid,timestamp,priority,flags,is_hidden,access_group,child_default_profile,url,title,file_a,file_b,old_uid,area_uid) VALUES (?,?,?,?, ?,?,?,?, ?,?,?,? , ? , ?)', undef
		,$arg->{'uid'}
		,sprintf('%u',$post->{'parent_uid'})
		,sprintf('%u',$arg->{'timestamp'})
		,sprintf('%u',$post->{'priority'})
		,sprintf('%u',$arg->{'flags'})
		,sprintf('%u',$post->{'is_hidden'})
		,$post->{'access_group'}
		,($post->{'child_default_profile'} ne '' ? $post->{'child_default_profile'} : $this->{'table'})
		,($post->{'url'} eq '' ? Basic::trim(lc Basic::transliteration3(Basic::trim($post->{'title'}))) : $post->{'url'}) # $arg->{'uid'}
		,$post->{'title'}
		,${$packed->{'file_a_ref'}}
		,${$packed->{'file_b_ref'}}
		,sprintf('%u',$post->{'old_uid'})
		,sprintf('%u',$post->{'area_uid'})
	) or die $this->{'dbh'}->errstr;


	return 1;
}

sub _update
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $post=$arg->{'post'};
	my $packed=$arg->{'packed'};

	$this->{'dbh'}->do('UPDATE City SET parent_uid=?,timestamp=?,priority=?,flags=?,access_group=?,child_default_profile=?,url=?,title=?,file_a=?,file_b=?,old_uid=?,area_uid=? WHERE uid=?', undef
		,sprintf('%u',$post->{'parent_uid'})
		,sprintf('%u',$arg->{'timestamp'})
		,sprintf('%u',$post->{'priority'})
		,sprintf('%u',$arg->{'flags'})
		,$post->{'access_group'}
		,($post->{'child_default_profile'} ne '' ? $post->{'child_default_profile'} : $this->{'table'})
#		,($post->{'url'} eq '' ? $arg->{'uid'} : $post->{'url'})
		,($post->{'url'} eq '' ? Basic::trim(lc Basic::transliteration3(Basic::trim($post->{'title'}))) : $post->{'url'}) # $arg->{'uid'}
		,$post->{'title'}
		,${$packed->{'file_a_ref'}}
		,${$packed->{'file_b_ref'}}
		,sprintf('%u',$post->{'old_uid'})
		,sprintf('%u',$post->{'area_uid'})
		,$arg->{'uid'}
	) or die $this->{'dbh'}->errstr;

	return 1;
}


# обновление метро
sub replace_subway 
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $matrix=$arg->{'subway'};

	my @data=();

	for (my $i=0; $i <= $#{$matrix}; $i+=3)
	{
		push @data, sprintf('(%u,%s,%s,%s)'
			,$arg->{'uid'}
			,$dbh->quote($matrix->[$i])
			,$dbh->quote($matrix->[$i+1])
			,$dbh->quote($matrix->[$i+2])
		);
	}

	return 1 unless @data;

	$dbh->do(sprintf('DELETE FROM City_Subway WHERE city_uid=%u',$arg->{'uid'})) or die $dbh->errstr;
	$dbh->do('INSERT INTO City_Subway (city_uid,subway,geo_lat,geo_lng) VALUES ' . join(',',@data)) or die $dbh->errstr;
	
	return 1;
}

# обновление районов
sub replace_district 
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $matrix=$arg->{'district'};

	my @data=();

	for (my $i=0; $i <= $#{$matrix}; $i+=2)
	{
		push @data,([$arg->{'uid'},$matrix->[$i],$matrix->[$i+1] ? $matrix->[$i+1] : '']);
	}

	return 1 unless @data;

	foreach my $sc (@data)
	{ 
		if ($sc->[2])
		{
			if (index($sc->[1],'DELETE') == -1)             
			{
				$dbh->do('UPDATE City_District SET city_uid=?,district=? WHERE id=?',undef,$sc->[0],$sc->[1],$sc->[2]) or die $dbh->errstr;
			}
			else
			{
				$dbh->do('DELETE FROM City_District WHERE id=?',undef,$sc->[2]) or die $dbh->errstr;
			}
		}
		else
		{
			$dbh->do('INSERT INTO City_District (city_uid,district) VALUES (?,?)',undef,$sc->[0],$sc->[1]) or die $dbh->errstr;
		}
	}
	return 1;
}


sub statistics
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %type = map { $_ => 1} split(',' => $arg->{'type'});

	my %sarg = ();
	$sarg{'city_uid'} = $arg->{'city_uid'} if $arg->{'city_uid'};

	my @cache_tags = ('City');

	my @sql = ();
	if($type{'atm'})
	{
		$sarg{'label'} = 'atm';
		push @sql, $this->base_model()->obj('Bank')->get_city_atm_stat(\%sarg);
		push @cache_tags, 'Atm';	
	}

	if($type{'office'})
	{
		$sarg{'label'} = 'office';
		push @sql, $this->base_model()->obj('Bank')->get_city_office_stat(\%sarg);
		push @cache_tags, 'Office';		
	}

	if($type{'bank'})
	{
		$sarg{'label'} = 'bank';
		push @sql, $this->base_model()->obj('Bank')->get_city_stat(\%sarg);
		push @cache_tags, 'Bank';
	}

	if($type{'credit'})
	{
		$sarg{'label'} = 'credit';
		push @sql, $this->base_model()->obj('Credit2')->get_city_stat(\%sarg);
		push @cache_tags, 'Credit2';
	}

	if($type{'deposit'})
	{
		$sarg{'label'} = 'deposit';
		push @sql, $this->base_model()->obj('Deposit')->get_city_stat(\%sarg);
		push @cache_tags, 'Deposit';
	}
	if($type{'offer'})
	{
		$sarg{'label'} = 'offer';
		push @sql, $this->base_model()->obj('Offer')->get_city_stat(\%sarg);
		push @cache_tags, 'Offer';	
	}

	if($type{'news'})
	{
	}

	@sql = grep { $_ ne '' } @sql;

	return [] unless @sql;

	my $ar = $this->{'dbcache'}->selectall_arrayref(join(' UNION ' => @sql), {Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>\@cache_tags});

	return $ar;
}

sub city_href
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
  return {} unless $arg->{'city_uid'};
  
	my @where=();
	
  if(exists $arg->{'city_uid'})
	{
		push @where,sprintf('A.`uid`=%u',$arg->{'city_uid'});		
	}
	
	if (exists $arg->{'is_hidden'})
	{
		push @where,sprintf('A.is_hidden=%u',$arg->{'is_hidden'});
	}
  
	my $where=@where ? 'WHERE '.join(' AND ',@where) : '';
	
	my $sth=$dbh->prepare(sprintf(<<'	_QUERY_',$this->{'profile'},$where)) or die $dbh->errstr;
	SELECT
    A.url
	FROM
		%s AS A
		%s
	_QUERY_

	$sth->execute or die $dbh->errstr;
	my $hr=$sth->fetchrow_hashref;
	$sth->finish;

	return $hr;
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


sub list_city_near
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	return [] unless $arg->{'city_uid'};
	return [] unless $arg->{'lat'};
	return [] unless $arg->{'lng'};

	my %attr=(Slice=>{}, CacheMemExpire=>10, CacheFileExpire=>60*60, CacheTags=>['City']);
	my $ar=$this->{'dbcache'}->selectall_arrayref(sprintf(<<'	__Q__',$arg->{'city_uid'}), \%attr);
		SELECT 
			uid, url, title, file_a
		FROM 
			City
		WHERE 
			is_hidden=0
			AND uid!=%u 
		;
	__Q__
	die $this->{'dbcache'}->errstr if $this->{'dbcache'}->errstr;
	
	return [] unless @$ar;

	my %point =( 'lat'=>$arg->{'lat'}, 'lng'=>$arg->{'lng'} );

	foreach my $hr (@$ar)
	{	
		Bin::unpackhash(\$hr->{'file_a'}, $hr) if exists $hr->{'file_a'};
		
		my %point2 = ( 'lat'=>$hr->{'lat'}, 'lng'=>$hr->{'lng'} );
		$hr->{'distance'}=int(Ru::distance('point1'=>\%point, 'point2'=>\%point2));			
		delete $hr->{'file_a'};
	}

	@$ar = (sort {$a->{'distance'}<=>$b->{'distance'}} @$ar);
	if ($arg->{'limit'})
	{
		@$ar = @$ar[0..$arg->{'limit'}];
	}
	
    return $ar;
}

1;
