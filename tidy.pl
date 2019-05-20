#!/usr/bin/env perl

# ============================================================================
# Copyright 2019 Fairtide Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

use v5.16;
use Getopt::Long;

my $clang_format = "/usr/local/bin/clang-format";
GetOptions("clang_format=s" => \$clang_format);

my $root = $0;
$root =~ s/\/tidy.pl//;
my $nroot = length($root);

&update($root);

sub update
{
    my $dir = shift;

    return if $dir =~ /Catch2/;

    my @ls = `ls $dir`;
    for (@ls) {
        chomp;
        my $path = $dir . "/" . $_;
        if (-f $path) {
            next if $path =~ /README\.md$/;
            next if $path =~ /\.data$/;
            next if $path =~ /\.txt$/;
            next if $path =~ /tidy\.pl$/;
            &filename($path);
            &copyright($path);
            if ($path =~ /\.(c|h|cpp|hpp|cl|cpp\.in)$/) {
                system "$clang_format -i $path";
            }
        } elsif (-d $path) {
            next if $path =~ /\.git$/;
            next if $path =~ /build$/;
            next if $path =~ /build-/;
            next if $path =~ /docs\/_build$/;
            next if $path =~ /docs\/doxygen$/;
            next if $path =~ /docs\/tabs\/random_distribution$/;
            next if $path =~ /docs\/tabs\/random_rng$/;
            &update($path);
        }
    }
}

sub filename
{
    my $file = shift;

    return if $file =~ /googletest/;

    open my $in, "<", $file;
    my @lines = <$in>;

    substr($file, 0, $nroot) = "DATAFRAME";

    if ($file =~ /\.hpp$/ or $file =~ /\.h$/) {
        my $guard = $file;
        $guard =~ s/\//_/g;
        $guard =~ s/\./_/g;
        $guard =~ s/^DATAFRAME_include_//;
        $guard =~ s/^DATAFRAME_tests_.*_include/DATAFRAME_TESTS/;
        $guard = "\U$guard";
        my @found = grep { /$guard/ } @lines;
        say "Incorrect header guard: $file: $guard" unless @found == 3;
    }

    open my $out, ">", $file;

    my $line = shift @lines;
    print $out $line;

    if ($line =~ /^#!/) {
        my $line = shift @lines;
        print $out $line;
        my $line = shift @lines;
        print $out $line;
    }

    $line = shift @lines;
    if ($line =~ /^(;; |\/\/ |#  |\.\.  )DATAFRAME\//) {
        print $out $1, $file, "\n";
    } else {
        print $out $line;
    }

    print $out @lines;
}

sub copyright
{
    my $file = shift;

    return if $file =~ /googletest/;

    open my $in, '<', $file;
    my @lines = <$in>;

    my $found = 0;
    for (@lines) {
        if (/Copyright/) {
            $found = 1;
            $_ =~ s/201./2019/;
            last;
        }
    }

    open my $out, '>', $file;
    print $out @lines;
    say "No copyright information: $file" unless $found;
}
