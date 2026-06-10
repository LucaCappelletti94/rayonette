// The central soundness proof: the macro only proposes the input type, so a
// closure annotated with the wrong type (`String` over a `Vec<u32>` receiver) is
// rejected by net_map's `Fn(Self::Item) -> O` bound at the call site, never a
// runtime mis-decode.
use rayonette::prelude::*;

#[rayonette::tasks]
fn main() {
    let values: Vec<u32> = vec![1, 2, 3];
    let _job = values.net_map(|x: String| x.len() as u32);
}
