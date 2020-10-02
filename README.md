# LocalMR

LocalMR is a MapReduce implementation that is designed to be run on a single machine.
It is best suited for calculation-heavy tasks: While it can run code concurrently,
the Hard Disk obviously remains the bottleneck. So sometimes, a local `grep` may be
faster :)

Please use `cargo doc` to inform yourself about the inner workings and how to
use it. Best is to start with `controller::MRController` and then work through
the various types from there -- `Sharder`, `Mapper`, `Reducer`, etc.

