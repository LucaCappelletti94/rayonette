// Tier B: an unannotated closure over a typed `let` binding compiles, because the
// macro recovers the input type (`u32`) from the binding's `Vec<u32>`.
use rayonette::prelude::*;

#[rayonette::tasks]
fn main() {
    let values: Vec<u32> = vec![1u32, 2, 3];
    let _job = values.net_map(|x| x * 2);
}
