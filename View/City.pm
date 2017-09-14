package View::City;

use strict;
use base qw(View);
use utf8;
use HTML;
use Ru;

sub escape
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $out=$this->SUPER::escape($arg) or die;

	my $hr=$arg->{'item'};

	$out->{'vowel'} = 1 if $hr->{'title'} =~ m/^[АаИиОоУуЫыЭэ]/i;
	
	$out->{'City_Subway'}=[];

	if (exists $hr->{'City_Subway'} && @{$hr->{'City_Subway'}})
	{
		foreach my $row (@{$hr->{'City_Subway'}})
		{
			push @{$out->{'City_Subway'}}, {
				'id'=>sprintf('%u',$row->{'id'}),
				'city_uid'=>sprintf('%u',$row->{'city_uid'}),

				'subway'=>jsspecialchars($row->{'subway'}),
				'lat_jsspecialchars'=>jsspecialchars($row->{'geo_lat'}),
				'lng_jsspecialchars'=>jsspecialchars($row->{'geo_lng'}),

				'geo_lat' => sprintf('%.8f', $row->{'geo_lat'}),
				'geo_lng' => sprintf('%.8f', $row->{'geo_lng'}),

				'subway'=>htmlspecialchars($row->{'subway'}),
			};
		}
	}
	
	$out->{'City_District'}=[];
	
	if (exists $hr->{'City_District'} && @{$hr->{'City_District'}})
	{
		foreach my $row (@{$hr->{'City_District'}})
		{
			push @{$out->{'City_District'}}, {
				'id'=>sprintf('%u',$row->{'id'}),
				'city_uid'=>sprintf('%u',$row->{'city_uid'}),
				'js_district'=>jsspecialchars($row->{'district'}),
				'district'=>htmlspecialchars($row->{'district'}),
			};
		}
	}

	$out->{'counter'} = unhtmlspecialchars($hr->{'counter'});
	
#	use Data::Dumper;
#	print '<pre>',Dumper(\$out),'</pre>';
	
	return $out;
}

sub statistics
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my %out=();
	my $ar = $this->{'model'}->statistics($arg);

	return \%out unless @$ar;

	foreach my $hr (@$ar)
	{
		$out{$hr->{'city_uid'}} = {} unless exists $out{$hr->{'city_uid'}};
		$out{$hr->{'city_uid'}}->{$hr->{'label'}} = $hr->{'value'};
	}

	return \%out;
}

sub city_href
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	my $hr = $this->{'model'}->city_href('city_uid' => $arg->{'city_uid'});
	
	return {} unless $hr->{'url'};
	
	$hr->{'profile'} = $this->{'profile'};
	
	return {'href' => $this->SUPER::_href('item'=>$hr)};		
}

sub validate_request
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};

	my $hr=$this->{'model'}->validate_request($arg) or die;
	
	return $hr;
}

sub list_city_near
{
	my $this=shift;
	my $arg=ref($_[0]) eq 'HASH' ? $_[0] : {@_};
	
	my $ar = $this->{'model'}->list_city_near($arg) or die;
	
	my @out =  @$ar; # map { $this->escape_city($_) }
	
	return \@out;
}

1;
