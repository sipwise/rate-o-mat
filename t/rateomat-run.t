
use strict;

use Utils::Api qw();
use Utils::Rateomat qw();
use Test::More;

{

    ok(Utils::Rateomat::run_rateomat_threads(1,5),"rate-o-mat executed");

}

done_testing();
exit;

