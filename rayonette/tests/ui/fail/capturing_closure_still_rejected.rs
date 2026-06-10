// A capturing closure cannot be a distributed task: its captured state is not
// serializable. The no-capture const-assert rejects it at compile time with a
// message naming the rule. (Migrated from the fleet.rs net_map compile_fail
// doctest so the message can be asserted.)
use rayonette::prelude::*;

fn main() {
    let captured = 10u32;
    let _job = std::iter::once(1u32).net_map(move |x: u32| x + captured);
}
