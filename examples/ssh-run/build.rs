//! Build script: extracts the agent registry and source bundle this ssh-run
//! example embeds with `rayonette::embed_microcrates!()`.

fn main() {
    rayonette_build::extract().expect("rayonette_build::extract failed");
}
