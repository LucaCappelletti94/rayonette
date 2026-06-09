//! Build script: extracts the agent registry and source bundle this ssh-run
//! example embeds with `rayonet::embed_microcrates!()`.

fn main() {
    rayonet_build::extract().expect("rayonet_build::extract failed");
}
