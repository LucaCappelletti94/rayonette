// Tier C: an unannotated closure over a suffixed `vec!` literal compiles, because
// the macro recovers `u32` from the first element's suffix.
use rayonette::prelude::*;

#[rayonette::tasks]
fn main() {
    let _job = vec![1u32, 2, 3].net_map(|x| x * 2);
}
