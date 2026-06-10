// A named function passed to net_map keys by its path and registers directly.
use rayonette::prelude::*;

fn triple(x: u32) -> u32 {
    x * 3
}

#[rayonette::tasks]
fn main() {
    let _job = (0..5u32).net_map(triple);
}
