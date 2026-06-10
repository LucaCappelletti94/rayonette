// An annotated non-capturing closure becomes a task with no named function and
// no hand-written registry: the macro keys it and emits its registration.
use rayonette::prelude::*;

#[rayonette::tasks]
fn main() {
    let _job = (0..5u32).net_map(|x: u32| x * 2);
}
