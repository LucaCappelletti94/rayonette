// An explicit turbofish on a generic function round-trips: the macro passes the
// task expression through verbatim, so `identity::<u32>` keeps its instantiation
// in both the rewritten call and the registration (the old scanner dropped it).
use rayonette::prelude::*;

fn identity<T>(x: T) -> T {
    x
}

#[rayonette::tasks]
fn main() {
    let _job = (0..3u32).net_map(identity::<u32>);
}
