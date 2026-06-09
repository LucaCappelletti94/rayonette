//! Build script: extracts the agent registry and source bundle this fixture
//! consumer embeds with `rayonette::embed_microcrates!()`.

fn main() {
    rayonette_build::extract().expect("rayonette extraction failed");
}
