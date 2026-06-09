//! Build script: extracts the agent registry and source bundle this fixture
//! consumer embeds with `rayonet::embed_microcrates!()`.

fn main() {
    rayonet_build::extract().expect("rayonet extraction failed");
}
