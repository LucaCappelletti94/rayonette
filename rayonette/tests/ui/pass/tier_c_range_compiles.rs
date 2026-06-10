// Tier C: an unannotated closure over a suffixed range compiles, because the
// macro recovers `u32` from the range bound.
use rayonette::prelude::*;

#[rayonette::tasks]
fn main() {
    let _job = (0..3u32).net_map(|x| x * 2);
}
