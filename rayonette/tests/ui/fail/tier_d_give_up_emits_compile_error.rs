// Tier D: the receiver is an opaque function call, so the macro cannot recover
// the unannotated closure's input type. The result is a compile error at the call
// site asking for an annotation, never a silent runtime miss.
use rayonette::prelude::*;

fn produce() -> Vec<u32> {
    vec![1, 2, 3]
}

#[rayonette::tasks]
fn main() {
    let _job = produce().net_map(|x| x * 2);
}
