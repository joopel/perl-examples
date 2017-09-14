package View::Credit2;

use strict;
use base qw(View);
use utf8;
use HTML;
use Ru;
use Basic;
use JSON::XS;

my $json;

sub pagenav
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $env=$this->heap()->{'ENV'};

	my @tmp=grep { not(m/\Atype=/) } grep { not(m/\Aproduct=/) } grep { not(m/\Apage=/) } split(/&/,$env->{'QUERY_STRING'}); # выкидываем из QUERY_STRING все аргументы постраничной навигации
	my $query_string=join('&',@tmp);
	$query_string=~s/%/%%/g;

	my $tmpl=($query_string ne ''
		? '?'.$query_string.'&page=%u'
		: '?page=%u');

	return Ru::pagenav3({
			'href'=>$tmpl,
			'rows'=>$this->{'pagenav'}->{'rows'},
			'offset'=>$this->{'pagenav'}->{'offset'},
			'limit'=>$this->{'pagenav'}->{'limit'}
	});
}

sub pagenav2
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $tmpl=exists $arg->{'tmpl'} ? $arg->{'tmpl'} : '?p=%u';

	my $parg = {
			'href'=>$tmpl,
			'rows'=>$this->{'pagenav'}->{'rows'},
			'offset'=>$this->{'pagenav'}->{'offset'},
			'limit'=>$this->{'pagenav'}->{'limit'}
	};
	$parg->{'zero'} = $arg->{'zero'} if $arg->{'zero'};

	return Ru::pagenav3($parg);
}

sub item
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $hr=$this->{'model'}->item($arg) or die;

	return {} unless exists $hr->{'uid'};

	return $this->escape(
		'item'         => $hr,
		'href_prefix'  => $arg->{'href_prefix'},
		'json_summary' => ($arg->{'json_summary'} ? 1 : 0)
	);
}

sub escape
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $hr=$arg->{'item'};
	
	my $heap=$this->base_view()->{'heap'};
	
	my $out=$this->SUPER::escape($arg) or die; 
    
	#$out->{'href'}=sprintf('/banks/credit/%u',$hr->{'uid'});
	
	$out->{'bank_uid'}=sprintf('%u',$hr->{'bank_uid'});
	$out->{'bank_rating'}=sprintf('%u',$hr->{'rating'});
	$out->{'bank_city_rating'}=sprintf('%u',$hr->{'bank_city_rating'});
	$out->{'is_citizenship_required'}=sprintf('%u',$hr->{'is_citizenship_required'});
	$out->{'pricing_model'}=sprintf('%u',$hr->{'pricing_model'});
	
	$out->{'bank_img2_uri'} = sprintf '/f%s', $hr->{'img2_uri'} if $hr->{'img2_uri'};
	$out->{'bank_img2_title'} = $hr->{'img2_title'} || sprintf 'Логотип %s', $hr->{'title3'} if $out->{'bank_img2_uri'};
	
	# для страницы кредита

	$out->{'минимальный_возраст_заёмщика_для_мужчин_лет'}=sprintf('%u',$hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'}) if exists $hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'};
	$out->{'минимальный_возраст_заёмщика_для_женщин_лет'}=sprintf('%u',$hr->{'минимальный_возраст_заёмщика_для_женщин_лет'}) if exists $hr->{'минимальный_возраст_заёмщика_для_женщин_лет'};
	$out->{'максимальный_возраст_заёмщика_для_мужчин_лет'}=sprintf('%u',$hr->{'максимальный_возраст_заёмщика_для_мужчин_лет'}) if exists $hr->{'максимальный_возраст_заёмщика_для_мужчин_лет'};
	$out->{'максимальный_возраст_заёмщика_для_женщин_лет'}=sprintf('%u',$hr->{'максимальный_возраст_заёмщика_для_женщин_лет'}) if exists $hr->{'максимальный_возраст_заёмщика_для_женщин_лет'};

  $out->{'Гражданство РФ'}=sprintf('%s',$hr->{'is_citizenship_required'} ? 'требуется' : 'не требуется');	

  $out->{'минимальный_возраст_заёмщика_для_мужчин_лет_html'}=sprintf('%u %s',$hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'},Ru::wordcase($hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'}, qw(лет года лет лет лет лет))) if $hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'};
  $out->{'минимальный_возраст_заёмщика_для_женщин_лет_html'}=sprintf('%u %s',$hr->{'минимальный_возраст_заёмщика_для_женщин_лет'},Ru::wordcase($hr->{'минимальный_возраст_заёмщика_для_женщин_лет'}, qw(лет года лет лет лет лет))) if $hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'};
  $out->{'максимальный_возраст_заёмщика_для_мужчин_лет_html'}=sprintf('%u %s',$hr->{'максимальный_возраст_заёмщика_для_мужчин_лет'},Ru::wordcase($hr->{'максимальный_возраст_заёмщика_для_мужчин_лет'}, qw(лет года лет лет лет лет))) if $hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'};
  $out->{'максимальный_возраст_заёмщика_для_женщин_лет_html'}=sprintf('%u %s',$hr->{'максимальный_возраст_заёмщика_для_женщин_лет'},Ru::wordcase($hr->{'максимальный_возраст_заёмщика_для_женщин_лет'}, qw(лет года лет лет лет лет))) if $hr->{'минимальный_возраст_заёмщика_для_мужчин_лет'};

	$out->{'общий_стаж_работы_лет'}=Ru::hrnum($hr->{'общий_стаж_работы_лет'}) if exists $hr->{'общий_стаж_работы_лет'};
	$out->{'стаж_на_последнем_месте_работы_месяцы'}=Ru::hrnum($hr->{'стаж_на_последнем_месте_работы_месяцы'}) if exists $hr->{'стаж_на_последнем_месте_работы_месяцы'};

	$out->{'какие_виды_доходов_учитываются'}=nl2br(htmlspecialchars($hr->{'какие_виды_доходов_учитываются'})) if exists $hr->{'какие_виды_доходов_учитываются'};
	$out->{'обеспечение_кредита'}=nl2br(htmlspecialchars($hr->{'обеспечение_кредита'})) if exists $hr->{'обеспечение_кредита'};
	$out->{'досрочное_погашение'}=nl2br(htmlspecialchars($hr->{'досрочное_погашение'})) if exists $hr->{'досрочное_погашение'};
	$out->{'страхование'}=nl2br(htmlspecialchars($hr->{'страхование'})) if exists $hr->{'страхование'};
	$out->{'дополнительная_информация'}=nl2br(htmlspecialchars($hr->{'дополнительная_информация'})) || '&mdash;' if exists $hr->{'дополнительная_информация'};
	
	$out->{'cap_дополнительная_информация'}=nl2br(htmlspecialchars($hr->{'cap_дополнительная_информация'})) || '&mdash;' if exists $hr->{'cap_дополнительная_информация'};	
	
	$out->{'дополнительная_информация2'}=nl2br(htmlspecialchars($hr->{'дополнительная_информация'})) if exists $hr->{'дополнительная_информация'};
	$out->{'льготный_период_кредитования'}=nl2br(htmlspecialchars($hr->{'льготный_период_кредитования'})) || '&mdash;' if exists $hr->{'льготный_период_кредитования'};

	#MY
=pod	
	$out->{'годовое_обслуживание_карты'}=~ s/\.$//g;
	$out->{'лимит_снятия_наличных_средств'}=~ s/\.$//g;
	$out->{'погашение_кредита'}=~ s/\.$//g;
	$out->{'специальные_предложения'}=~ s/\.$//g;	
	$out->{'тип_карты'}=~ s/\.$//g;	
=cut
	#дополнительная обработка данных
	foreach (keys %{$out})
	{
		$out->{$_} =~ s/\.$//g;
		$out->{$_} =~ s/рублей/руб\./g;
		$out->{$_} = ucfirst($out->{$_}) if (length($out->{$_})>20);
	}	
	#
	
	
	$out->{'old_uid'} = sprintf('%u',$hr->{'old_uid'});
	
	# для результатов поиска

	$out->{'bank_title'}=htmlspecialchars($hr->{'bank_title'}) if exists $hr->{'bank_title'};
	$out->{'bank_href'}=sprintf '/banks/%s/',htmlspecialchars($hr->{'bank_url'}) if exists $hr->{'bank_url'};
	$out->{'amount_of_interest_paid_str'}=Ru::hrprice(sprintf('%u',$hr->{'amount_of_interest_paid'})) if exists $hr->{'amount_of_interest_paid'}; # Сумма процентов
	$out->{'commission'}=sprintf('%u',$hr->{'commission'}) if exists $hr->{'commission'}; # Комиссия банка
	$out->{'commission_str'}=Ru::hrprice(sprintf('%u',$hr->{'commission'})) if exists $hr->{'commission'}; # Комиссия банка
	$out->{'overpayment_str'}=Ru::hrprice(sprintf('%u',$hr->{'overpayment'})) if exists $hr->{'overpayment'}; # Итого переплата
	$out->{'min_rate_str'}=Ru::hrnum(sprintf('%u',$hr->{'min_rate'})/100) if exists $hr->{'min_rate'}; # Минимальная процентная ставка (для плавающей ставки)
	$out->{'max_rate_str'}=Ru::hrnum(sprintf('%u',$hr->{'max_rate'})/100) if exists $hr->{'max_rate'}; # Максимальная процентная ставка (для плавающей ставки)
	$out->{'var'}=$hr->{'var'} if exists $hr->{'var'}; # оставляем без экранирования т.к. данные из базы + только числа + лень делать
	
	if ($heap->{'city'}->{'priority'}==0)
	{
		$out->{'new_uid'} =  sprintf('%x',($hr->{'uid'}+$out->{'bank_uid'}+$heap->{'city'}->{'uid'}));
		$out->{'href'}=sprintf('%s%s/%s_%s',$out->{'bank_href'},$hr->{'parent_url'},$hr->{'url'},$out->{'new_uid'});
	}
	else
	{
		$out->{'new_uid'} =  sprintf('%x',($hr->{'uid'}+$out->{'bank_uid'}+306));
		$out->{'href'}=sprintf('http://%s%s%s/%s_%s',$ENV{'msk_host'},$out->{'bank_href'},$hr->{'parent_url'},$hr->{'url'},$out->{'new_uid'});
	}
	
	if ($out->{'parent_uid'}==2465)
	{
		$out->{'срок'}=htmlnobr(Ru::range(int($hr->{'period_from'}/12), int($hr->{'period_to'}/12)) . ' лет'); #Срок кредита, выводим вместо данных из матрицы
	}
	else
	{
		$out->{'срок'}=htmlnobr(Ru::range(int($hr->{'period_from'}), int($hr->{'period_to'})) . ' мес.'); #Срок кредита, выводим вместо данных из матрицы	
	}

	#$out->{'purpose1str'}
	#$out->{'purpose2str'}

	# для сравнения кредитов

	my $currency_html=(('руб.','$','&euro;')[$hr->{'currency'}]);
	$out->{'sum_html'}=htmlnobr(Ru::hrprice(sprintf('%u',$hr->{'sum'})).' '.$currency_html) if exists $hr->{'sum'}; # сумма кредита
	#$out->{'rate_html'}=($hr->{'min_rate_str'} eq $hr->{'max_rate_str'} ? $out->{'min_rate_str'} : join('-',$out->{'min_rate_str'},$out->{'max_rate_str'})).'%';
	$out->{'rate_html'}=($hr->{'min_rate_str'} eq $hr->{'max_rate_str'} ? $out->{'min_rate_str'} : join('-',$out->{'min_rate_str'},$out->{'max_rate_str'})).'%';
	$out->{'period_html'}=sprintf('%u',$hr->{'period'}).' '.Ru::wordcase(sprintf('%u',$hr->{'period'}), qw(месяцев месяц месяца));
	$out->{'initial_html'}=$hr->{'initial'} ? Ru::hrprice(sprintf('%u',$hr->{'initial'})/100).'%' : '-';
	$out->{'amount_of_interest_paid_html'}=htmlnobr(Ru::hrprice(sprintf('%u',$hr->{'amount_of_interest_paid'})).' '.$currency_html) if exists $hr->{'amount_of_interest_paid'}; # Сумма процентов
	$out->{'commission_html'}=($hr->{'commission'} ? htmlnobr(Ru::hrprice(sprintf('%u',$hr->{'commission'})).' '.$currency_html) : '-') if exists $hr->{'commission'}; # Комиссия банка
	$out->{'overpayment_html'}=htmlnobr(Ru::hrprice(sprintf('%u',$hr->{'overpayment'})).' '.$currency_html) if exists $hr->{'overpayment'}; # Итого переплата
	$out->{'overpayment_html2'}= sprintf 'от %s',$out->{'overpayment_html'} if exists $out->{'overpayment_html'};
	$out->{'ndfl_html'}=($hr->{'ndfl'} ? 'да' : '-') if exists $hr->{'ndfl'};

	my $tmp=$hr->{'общий_стаж_работы_лет'}; $tmp=~tr/0-9//cd;
	
	$out->{'общий_стаж_работы_лет_html'}=($hr->{'общий_стаж_работы_лет'} ? htmlnobr(Ru::hrnum($hr->{'общий_стаж_работы_лет'}).' '.Ru::wordcase($tmp, qw(лет года лет лет лет лет))) : '-') if $hr->{'общий_стаж_работы_лет'};
	$out->{'общий_стаж_работы_лет_html2'}=($hr->{'общий_стаж_работы_лет'} ? htmlnobr(Ru::hrnum($hr->{'общий_стаж_работы_лет'}).' '.Ru::wordcase($tmp, qw(лет года лет лет лет лет))) : '-') if exists $hr->{'общий_стаж_работы_лет'};
	my $tmp=$hr->{'стаж_на_последнем_месте_работы_месяцы'}; $tmp=~tr/0-9//cd;
	$out->{'стаж_на_последнем_месте_работы_месяцы_html'}=($hr->{'стаж_на_последнем_месте_работы_месяцы'} ? htmlnobr(Ru::hrnum($hr->{'стаж_на_последнем_месте_работы_месяцы'}).' '.Ru::wordcase($tmp, qw(месяцев месяца месяцев))) : '-') if $hr->{'стаж_на_последнем_месте_работы_месяцы'};

	$out->{'datetime'} = Ru::time2hrstr($hr->{'timestamp'});
	
	$out->{'title2'} = unhtmlspecialchars($hr->{'title'});
	
	$out->{'cap_title'} = htmlspecialchars($hr->{'cap_title'});


	# суммарная информация о кредите (диапазоны всякие разные)



	if (exists $hr->{'summary'})
	{
		$out->{'summary'}  = [];
		my %summ           = ();
		my %currency_allow = ();

		foreach my $currency (0,1,2)
		{
			
			$currency_allow{$currency} = sprintf('%u', exists $hr->{'summary'}->{$currency});
			next unless exists $hr->{'summary'}->{$currency};

			my $ref=$hr->{'summary'}->{$currency}; # чтобы не писать много букв

			push @{$out->{'summary'}},{
				'currency' => $currency,
				'sum_default' => Ru::hrprice(int($ref->{'сумма_до'} ? $ref->{'сумма_до'} : $ref->{'сумма_от'})),
				
				'sum_default2' => (int($ref->{'сумма_до'} ? $ref->{'сумма_до'} : $ref->{'сумма_от'})),
				
				'period_default' => sprintf('%u',$ref->{'срок_кредита_до'} ? $ref->{'срок_кредита_до'} : $ref->{'срок_кредита_от'}),
				'initial_default' => sprintf('%u',$ref->{'взнос_до'} ? $ref->{'взнос_до'} : $ref->{'взнос_от'}),
				'взнос' => htmlnobr(Ru::range2(Ru::hrnum($ref->{'взнос_от'}/100).'%', Ru::hrnum($ref->{'взнос_до'}/100).'%')),
				'взнос2' => htmlnobr(Ru::range2(Ru::hrnum($ref->{'взнос_от'}/100).'%', Ru::hrnum(($ref->{'взнос_до'} ? $ref->{'взнос_до'} : $ref->{'взнос_от'})/100).'%')),
				'обязательна_ндфл2' => sprintf('%u',$ref->{'обязательна_ндфл2'}),
				'валюта' => htmlnobr(('В рублях','В долларах','В евро')[$currency]),
				'валюта2' => htmlnobr(('рублях','долларах','евро')[$currency]),
				'срок'   => htmlnobr(Ru::range(int($ref->{'срок_кредита_от'}), int($ref->{'срок_кредита_до'})) . ' мес.'),
				'ставка' => htmlnobr(Ru::range(Ru::hrnum($ref->{'ставка_от'}/100).'%', Ru::hrnum($ref->{'ставка_до'}/100).'%')),
				'ставка2' => htmlnobr(Ru::range2(Ru::hrnum($ref->{'ставка_от'}/100).'%', Ru::hrnum($ref->{'ставка_до'}/100).'%')),

				'stavka_default' => sprintf('%u',$ref->{'ставка_до'} ? $ref->{'ставка_до'} : $ref->{'ставка_от'}),				
				
				'сумма'  => htmlnobr(Ru::range(Ru::hrprice(int($ref->{'сумма_от'})), Ru::hrprice(int($ref->{'сумма_до'}))) . ((' руб.',' $',' &euro;')[$currency])),
				'сумма2'  => htmlnobr(Ru::range(Ru::hrprice(int($ref->{'сумма_от'})), Ru::hrprice(int($ref->{'сумма_до'})))) . ((unhtmlspecialchars(' <span class="roubleBox">P<span>&ndash;</span></span>'),' $',' &euro;')[$currency]),
				
				'взнос_от' => htmlnobr(Ru::hrnum($ref->{'взнос_от'}/100)),
				'срок_до' => int($ref->{'срок_кредита_до'}/12),
				'ставка_от' => htmlnobr(Ru::hrnum($ref->{'ставка_от'}/100)),
				
				'сумма3'  => (Ru::range(Ru::hrprice(int($ref->{'сумма_от'})), Ru::hrprice(int($ref->{'сумма_до'}))) . ((' руб.',' $',' &euro;')[$currency])),
				
#				'сумма_до'  => htmlnobr(Ru::hrprice(int($ref->{'сумма_до'})) . ((' руб.',' $',' &euro;')[$currency])),				
#				'сумма_до2'  => int($ref->{'сумма_до'}),				
			};
			
			if($arg->{'json_summary'})
			{
				$summ{sprintf('currency_%u',$currency)} = {
					'range' => {
						'sum'     => {'from' => int($ref->{'сумма_от'}),       'to' => int($ref->{'сумма_до'})        },
						'period'  => {'from' => int($ref->{'срок_кредита_от'}),'to' => int($ref->{'срок_кредита_до'}) },
						'initial' => {'from' => ($ref->{'взнос_от'}/100),      'to' => ($ref->{'взнос_до'}/100)       }
					},
					'defaults' => {
						'sum'     => int($ref->{'сумма_до'} ? $ref->{'сумма_до'} : $ref->{'сумма_от'}),
						'period'  => sprintf('%u',$ref->{'срок_кредита_до'} ? $ref->{'срок_кредита_до'} : $ref->{'срок_кредита_от'}),
						'initial' => (sprintf('%u',$ref->{'взнос_до'} ? $ref->{'взнос_до'} : $ref->{'взнос_от'}) / 100)
					}
				};
			}			
		}
		

		if($arg->{'json_summary'})
		{
			$json = JSON::XS->new()->latin1(1) unless defined $json;

			$out->{'json_summary'} = htmlspecialchars($json->encode(\%summ));

			$out->{'currency_allow'} = \%currency_allow;
		}		
		
	}
	
	if (exists $hr->{'commission_credits_card'})
	{
		$out->{'commission_credits_card'}=[];
		
		foreach (@{$hr->{'commission_credits_card'}})
		{
			push @{$out->{'commission_credits_card'}},{
					'currency' => $_->{'currency'},
					'валюта' => ('В рублях','В долларах','В евро')[$_->{'currency'}],
					'title' => sprintf('%s, %s',$_->{'name'},('в рублях','в долларах','в евро')[$_->{'currency'}]),
					'percent' => sprintf '%s%',$_->{'percent'}/100
				};
		}
	
	}

=pod	
	# цель кредита

	if (exists $hr->{'purpose'})
	{
		$out->{'purpose'}=[];

		foreach my $purpose (@{$hr->{'purpose'}})
		{
			push @{$out->{'purpose'}},{
				'title'=>htmlspecialchars($purpose->{'title'})
			};
		}
	}

	# цель кредита 2

	if (exists $hr->{'purpose2'})
	{
		$out->{'purpose2'}=[];

		foreach my $purpose (@{$hr->{'purpose2'}})
		{
			push @{$out->{'purpose2'}},{
				'title'=>htmlspecialchars($purpose->{'title'})
			};
		}
	}
=cut	
	
	#Предоставляется на
	{
		my @p = ();
		my @p2 = ();
#		print $hr->{'purpose1str'};
#		print $hr->{'purpose_text'};

		push @p, split(/\,/ => $hr->{'purpose_text'}) if $hr->{'purpose_text'};
		push @p2, split(/\,/ => $hr->{'purpose2_text'}) if $hr->{'purpose2_text'};
		@p = grep { $_ ne '' } @p;
		@p2 = grep { $_ ne '' }  @p2;
	
		my $predostavlaetsya;
		if (scalar @p2)
		{	
			my $counter=0;
			foreach (reverse @p2)
			{
				$predostavlaetsya .= sprintf('%s',lc $_);
				if ($counter==scalar @p2-1) #последний элемент
				{
					$predostavlaetsya .= ' ';
				}				
				elsif ($counter!=scalar @p2-2)
				{
					$predostavlaetsya .= ', ';
				}
				else
				{
					$predostavlaetsya .= ' или ';
				}
				$counter++;
			}
		}
	
		if (scalar @p)
		{
			my $counter=0;
			foreach (reverse @p)
			{
				$predostavlaetsya .= sprintf('%s',lc $_);
				if ($counter!=scalar @p-2)
				{
					$predostavlaetsya .= ' ';
				}
				else
				{
					$predostavlaetsya .= ' или ';
				}
				$counter++;
			}
		}	
		$out->{'predostavlaetsya'} = Basic::trim('Предоставляется на '.$predostavlaetsya) if ($predostavlaetsya);
		$out->{'celi'} = Basic::trim($predostavlaetsya) if ($predostavlaetsya);
	}

	return $out;
}

# Список всех возможных целей кредита #1
sub purpose
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $ar=$this->{'model'}->purpose($arg) or die;

	my @out=();

	foreach my $h (@$ar)
	{
		push @out,{
			'id'=>sprintf('%u',$h->{'id'}),
			'title'=>htmlspecialchars($h->{'title'})
		};
	}

	return \@out;
}

# Список всех возможных целей кредита #2
sub purpose2
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $ar=$this->{'model'}->purpose2($arg) or die;

	my @out=();

	foreach my $h (@$ar)
	{
		push @out,{
			'id'=>sprintf('%u',$h->{'id'}),
			'title'=>htmlspecialchars($h->{'title'})
		};
	}

	return \@out;
}
# Параметры переданные из формы подбора кредитов приводятся к допустимым значениям.

sub validate_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $hr = $this->{'model'}->validate_request($arg);

	$hr->{'escape'} = $this->escape_form_value($hr);
	return $hr;
	
	#return $this->{'model'}->validate_request($arg);
}

# Запаковывает параметры переданные из формы подбора кредитов в строку.
# Перед запаковкой используется метод validate_request.
# my $pack=$HEAP{'view'}->obj('Credit_Moneyzzz')->pack_request(\%GET) or die;

sub pack_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	return $this->{'model'}->pack_request($arg);
}

# Распаковывает строку в значения для подбора кредитов (методом search).
# После распаковки используется метод validate_request.
# my $hr=$HEAP{'view'}->obj('Credit_Moneyzzz')->unpack_request('pack'=>$pack) or die;

sub unpack_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	return $this->{'model'}->unpack_request($arg);
}

sub escape_form_value
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	my $out = \$arg;

	my $currency_html=(('руб.','$','&euro;')[$arg->{'currency'}]);
	my $period_num = $arg->{'period'};
	my @period_text = qw(месяцев месяц месяца);

	if(in_array(2465,$arg->{'type'}))
	{
		$period_num = $arg->{'period'}/12;
		@period_text = qw(лет год года года года лет);
	}

	$$out->{'query_text'} = join(' ',(
		htmlnobr(Ru::hrprice(sprintf('%u',$arg->{'sum'})).' '.$currency_html),
		($arg->{'initial'} ? sprintf('c первым взносом %s%',Ru::hrprice(sprintf('%u',$arg->{'initial'})/100)) : ' без первого взноса'),
		sprintf('на %u ',$period_num) . Ru::wordcase($period_num, @period_text)
		)
	);

	return $$out;
}

sub search
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $ar=$this->{'model'}->search($arg) or die;

	$this->{'pagenav'}={
		'offset'=>$arg->{'offset'},
		'limit'=>$arg->{'limit'},
		'rows'=>$this->{'model'}->rows()
	};

	my @out=map {$this->escape('item'=>$_)} @$ar;

	return \@out;
}

# Список идентификаторов кредитов, находящихся в сравнении
sub compare_list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return $this->{'model'}->compare_list($arg);
}

# Добавить к сравнению
sub add_to_compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	return $this->{'model'}->add_to_compare($arg); # может вернуть 0
}

# Удалить кредит из сравнения
sub delete_from_compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	$this->{'model'}->delete_from_compare($arg) or die;

	return 1;
}

# Сравнение кредитов
sub compare
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
  my $hr=$this->{'model'}->compare_by_type($arg) or die;
  my @out;
    
  if($hr->{$this->{'profile'}})
  {
    my $ar=$this->{'model'}->compare('param_by_type' =>$hr->{$this->{'profile'}},%$arg) or die;	
    my @out_credit=map {$this->escape('item'=>$_)} @$ar;
 
    push @out,@out_credit;
	}
	
	if($hr->{'Offer'}) 
  {
    my $ar_offer=$this->base_view()->obj('Offer')->compare('param_by_type' =>$hr->{'Offer'},%$arg) or die;
	
    push @out,@$ar_offer;
  }

	if($hr->{'Deposit'}) 
  {
    my $ar_offer=$this->base_view()->obj('Deposit')->compare('param_by_type' =>$hr->{'Deposit'},%$arg) or die;
	
    push @out,@$ar_offer;
  }

	return \@out;
}   

sub get_main_breadcrumb
{
	return {
		'title'	=>	'Кредиты',
		'href'	=>	'/credity/'
	};
}


sub bank_credit_parent_list
{
	my $this = shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $ar = $this->{'model'}->bank_credit_parent_list($arg) or die;
	return [] unless @$ar;

	my @out = ();
	foreach(@$ar)
	{
		my $hr = $this->base_view()->obj('Node')->escape('item' => $_);
		$hr->{'cnt'} = int($_->{'cnt'});
		push @out, $hr;
	}

	return \@out;
}


sub pack_calc_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	return $this->{'model'}->pack_calc_request($arg);
}


sub unpack_calc_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	return $this->{'model'}->unpack_calc_request($arg);
}


sub item_payments
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $hr = $this->{'model'}->item_payments($arg);

	my %out = (
		'сумма_выплаченных_процентов' => Ru::hrprice($hr->{'сумма_выплаченных_процентов'}),
		'сумма_всех_выплат'           => Ru::hrprice($hr->{'сумма_всех_выплат'}),
		'сумма_кредита'               => Ru::hrprice($hr->{'сумма_кредита'}),
		'сумма_комиссий'              => (exists $hr->{'сумма_комиссий'}
			? Ru::hrprice($hr->{'сумма_комиссий'})
			: '0'
		)
	);

	$out{'аннуитетный_платеж'} = Ru::hrprice($hr->{'аннуитетный_платеж'}) if exists $hr->{'аннуитетный_платеж'};

	$out{'график_платежей'} = [
		map {
			{
				'выплата_процентов'     => Ru::hrprice($_->{'выплата_процентов'}),
				'долг_на_конец_месяца'  => Ru::hrprice($_->{'долг_на_конец_месяца'}),
				'долг_на_начало_месяца' => Ru::hrprice($_->{'долг_на_начало_месяца'}),
				'выплата_долга'         => Ru::hrprice($_->{'выплата_долга'}),
				'месяц'                 => sprintf('%u', $_->{'месяц'}),
				'доп_расходы'           => (
					(exists $_->{'доп_расходы'})
						? Ru::hrprice($_->{'доп_расходы'})
						: '0'
				),
				'сумма_платежа'         => Ru::hrprice(
					$_->{'выплата_процентов'}
						+ $_->{'выплата_долга'}
						+ ($_->{'доп_расходы'} || 0)
				)
			}
		} @{$hr->{'график_платежей'}}
	] if $hr->{'график_платежей'};

	return \%out;
}

sub credit_city_uid_list
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $ar = $this->{'model'}->credit_city_uid_list($arg);

	my @out = map { int($_) } @$ar;

	return \@out;
}


1;
