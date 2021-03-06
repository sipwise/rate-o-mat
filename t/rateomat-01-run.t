
use strict;
use warnings;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

### testcase outline:
### rate-o-mat.pl invocation test
###
### this test verifies that ratomat can be properly invoked
### by the .t test scripts here. any further testcases can
### be skipped unless this one succeeds.

{

    ok(Utils::Rateomat::run_rateomat_threads(1,5),"rate-o-mat executed");

}

done_testing();
exit;
