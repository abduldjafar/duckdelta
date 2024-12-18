use sinks::Sinks;
use sources::Sources;

pub mod sinks;
pub mod sources;

pub struct Pipeline<Src: Sources> {
    source: Src,
}